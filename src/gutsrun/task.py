"""Run one task: `python -m gutsrun.task <executor-dir> <tid>`.

Loads `in/<tid>.pkl`, runs the initializer and then the call, and writes
its value to `out/<tid>.pkl`, exit 0. If the call raises, or its value does
not pickle, the task writes the traceback to stderr and its end to the file
named in HQ_ERROR_FILENAME (HQ reports that as the task's error), and
exits 1.

HQ cancels a task with SIGINT to its process group; Python raises
KeyboardInterrupt for it, so the call unwinds as for any other exception.

If a newer executor took over the spool, this executor's directory is
gone: a task that had not started fails before running (no input); one
that had runs its call and fails when publishing.
"""

import os
import pickle
import signal
import sys
from pathlib import Path

from gutsrun import spool

ERROR_LIMIT = 1900  # characters; HQ keeps about 2 KiB of a task's error


def _fail(text: str) -> int:
    sys.stderr.write(text)
    if path := os.environ.get("HQ_ERROR_FILENAME"):
        # The end of a traceback says what went wrong; keep that part.
        if len(text) > ERROR_LIMIT:
            text = "...\n" + text[-ERROR_LIMIT:]
        Path(path).write_text(text)
    return 1


def main(argv: list[str]) -> int:
    # Also where SIGINT was ignored by whoever started the worker.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    task_dir, tid = Path(argv[0]), argv[1]
    with open(task_dir / "in" / f"{tid}.pkl", "rb") as f:
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
        data = spool.dump(fn(*args, **kwargs))
    except BaseException:  # noqa: BLE001 - the caller decides
        import traceback  # only on errors: ~15 ms of every task's start-up

        return _fail(traceback.format_exc())
    spool.write_atomic(task_dir / "out" / f"{tid}.pkl", data)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
