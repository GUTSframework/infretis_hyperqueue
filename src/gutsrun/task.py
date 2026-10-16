"""Run one task: `python -m gutsrun.task <epoch-dir> <tid>`.

Loads `in/<tid>.pkl`, runs the initializer and then the call, and writes
`out/<tid>.<instance>.pkl`: {"result": ("ok", value) or ("err", exception,
traceback text), "t": timings}. The timings are wall-clock times
(`time.time()`) at the runner's start, after unpickling the call (which
imports its modules), after the initializer, and after the call. An exception in the call is a result, not a failure: the process
still exits 0. A non-zero exit means the task itself broke.

The instance is HQ_INSTANCE_ID under HyperQueue (it goes up when HQ reruns
the task after a worker crash), GUTSRUN_INSTANCE under the local backend.

If the epoch is no longer current (a newer executor took over the spool),
the task does nothing, before starting and again before publishing.
"""

import time

T_START = time.time()  # before any other import of this runner

import os  # noqa: E402
import pickle  # noqa: E402
import sys  # noqa: E402
from pathlib import Path  # noqa: E402

from gutsrun import spool

EXIT_STALE = 0  # a stale task is not an error; nobody waits for it


def _publish(epoch_dir: Path, tid: str, result: tuple, t: dict) -> None:
    instance = os.environ.get("HQ_INSTANCE_ID") or os.environ.get("GUTSRUN_INSTANCE", "0")
    t.update(instance=instance, host=os.uname().nodename)
    try:
        data = spool.dump({"result": result, "t": t})
    except Exception as e:  # the value or exception does not pickle
        text = result[2] if result[0] == "err" else ""
        data = spool.dump({"result": ("err", RuntimeError(f"result not picklable: {e!r}"), text),
                           "t": t})
    if not spool.is_current(epoch_dir):
        print(f"gutsrun: epoch {epoch_dir.name} is stale, result dropped", file=sys.stderr)
        return
    spool.write_atomic(epoch_dir / "out" / spool.result_name(tid, instance), data)


def main(argv: list[str]) -> int:
    epoch_dir, tid = Path(argv[0]), argv[1]
    if not spool.is_current(epoch_dir):
        print(f"gutsrun: epoch {epoch_dir.name} is stale, task {tid} not run", file=sys.stderr)
        return EXIT_STALE

    with open(epoch_dir / "in" / f"{tid}.pkl", "rb") as f:
        # Like multiprocessing's spawn: the caller's sys.path first, so the
        # call's modules unpickle the same way they would in the caller.
        for p in reversed(pickle.load(f)):
            if p not in sys.path:
                sys.path.insert(0, p)
        call = pickle.load(f)
    t = {"start": T_START, "loaded": time.time()}
    fn, args, kwargs, initializer, initargs = call

    try:
        if initializer is not None:
            initializer(*initargs)
        t["inited"] = time.time()
        result = ("ok", fn(*args, **kwargs))
    except BaseException as e:  # noqa: BLE001 - the caller decides
        import traceback  # only on errors: ~15 ms of every task's start-up

        result = ("err", e, traceback.format_exc())
    t["done"] = time.time()
    _publish(epoch_dir, tid, result, t)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
