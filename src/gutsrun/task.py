"""Run one task: `python -m gutsrun.task <epoch-dir> <tid>`.

Loads `in/<tid>.pkl`, runs the initializer and then the call, and writes
`out/<tid>.<instance>.pkl`: ("ok", value) or ("err", exception, traceback
text). An exception in the call is a result, not a failure: the process
still exits 0. A non-zero exit means the task itself broke.

The instance is HQ_INSTANCE_ID under HyperQueue (it goes up when HQ reruns
the task after a worker crash), GUTSRUN_INSTANCE under the local backend.

If the epoch is no longer current (a newer executor took over the spool),
the task does nothing, before starting and again before publishing.
"""

import os
import pickle
import sys
from pathlib import Path

from gutsrun import spool

EXIT_STALE = 0  # a stale task is not an error; nobody waits for it


def _publish(epoch_dir: Path, tid: str, result: tuple) -> None:
    instance = os.environ.get("HQ_INSTANCE_ID") or os.environ.get("GUTSRUN_INSTANCE", "0")
    try:
        data = spool.dump(result)
    except Exception as e:  # the value or exception does not pickle
        text = result[2] if result[0] == "err" else ""
        data = spool.dump(("err", RuntimeError(f"result not picklable: {e!r}"), text))
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
    fn, args, kwargs, initializer, initargs = call

    try:
        if initializer is not None:
            initializer(*initargs)
        result = ("ok", fn(*args, **kwargs))
    except BaseException as e:  # noqa: BLE001 - the caller decides
        import traceback  # only on errors: ~15 ms of every task's start-up

        result = ("err", e, traceback.format_exc())
    _publish(epoch_dir, tid, result)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
