"""A `concurrent.futures.Executor` that runs each call as a backend task.

Drop-in for `ProcessPoolExecutor` where the caller only uses `submit`,
`map` and `shutdown` (infretis does, through `loop.run_in_executor`).
Differences:

- Every call runs in a fresh process, so `initializer(*initargs)` runs
  before every call, not once per worker.
- Calls, arguments and results must pickle by reference, as with the
  spawn start method; functions defined in `__main__` do not.
- `submit(..., resource_dict={...})` sets one call's resources
  (cores, gpus, cwd, env), as in executorlib.
- A call that raises fails its future with TaskFailed, whose message ends
  with the task's traceback (about 2 KiB of it; all of it is in the task's
  stderr log, which is kept), not with the call's own exception.

Completion: a watcher thread asks the backend every `poll_interval` which
tasks ended (under HQ, from its event stream), and reads the result file
of each one that finished. Inputs and results of ended tasks are deleted
(inode budget); the backend deletes the logs of tasks that finished.
"""

import logging
import os
import pickle
import shutil
import sys
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import Executor, Future
from pathlib import Path
from typing import Any, TypeVar

from gutsrun import spool
from gutsrun.backends import Backend, Resources

log = logging.getLogger(__name__)

_SRC = str(Path(__file__).resolve().parent.parent)  # makes `gutsrun` importable in tasks

T = TypeVar("T")


class TaskFailed(Exception):
    """The call raised, or the task process broke, or it was cancelled."""


class TaskExecutor(Executor):
    """Runs each submitted call as one task of `backend`, a fresh
    `python -m gutsrun.task` process; see the module docstring.

    - `spool_dir`: where calls, results and task logs go; on a file system
      the tasks can reach. A new executor takes it over from an old one.
    - `max_workers`: informational, for code that reads it; the backend
      limits how many tasks run at once.
    - `initializer(*initargs)`: runs in the task process before every call.
    - `resource_dict`: default resources per call (cores, gpus, cwd, env);
      `submit(..., resource_dict=...)` overrides them for one call.
    - `python`: the interpreter tasks run with.
    - `poll_interval`: seconds between asking the backend which tasks ended.
    - `result_timeout`: seconds to wait for the result file of a task that
      finished, in case the shared file system shows it late.
    """

    def __init__(self, backend: Backend, spool_dir: str | Path, *,
                 max_workers: int | None = None,
                 initializer: Callable[..., object] | None = None,
                 initargs: Iterable[Any] = (), resource_dict: dict[str, Any] | None = None,
                 python: str = sys.executable, poll_interval: float = 0.02,
                 result_timeout: float = 60.0) -> None:
        self.backend = backend
        self.max_workers = max_workers  # informational; the backend limits concurrency
        self.initializer, self.initargs = initializer, tuple(initargs)
        self.default_resources = Resources.from_dict(resource_dict, Resources())
        self.python = python
        self.poll_interval = poll_interval
        self.result_timeout = result_timeout

        self.spool = Path(spool_dir).resolve()
        self.dir = self.spool / spool.new_name()
        for sub in ("in", "out", "log"):
            (self.dir / sub).mkdir(parents=True)
        self._take_over_spool()

        self._lock = threading.Lock()  # guards _pending, _seq and backend calls
        self._pending: dict[str, Future[Any]] = {}
        self._finished: dict[str, float] = {}  # tid -> when; its result is not readable yet
        self._seq = 0
        self._shutdown = False
        self._stop = threading.Event()
        self._watcher = threading.Thread(target=self._watch, name="gutsrun-watcher", daemon=True)
        self._watcher.start()

    # --- Executor API -------------------------------------------------------

    def submit(self, fn: Callable[..., T], /, *args: Any,
               resource_dict: dict[str, Any] | None = None, **kwargs: Any) -> Future[T]:
        """Run `fn(*args, **kwargs)` as a task; `resource_dict` overrides
        the executor's default resources for this call."""
        res = Resources.from_dict(resource_dict, self.default_resources)
        res.env = {**res.env, "PYTHONPATH": _pythonpath(res.env.get("PYTHONPATH"))}
        with self._lock:
            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")
            tid = f"{self._seq:08d}"
            self._seq += 1
            spool.write_atomic(
                self.dir / "in" / f"{tid}.pkl",
                spool.dump(sys.path, (fn, args, kwargs, self.initializer, self.initargs)))
            future: Future[T] = Future()
            future.set_running_or_notify_cancel()  # it runs as soon as the backend starts it
            self._pending[tid] = future
            argv = [self.python, "-m", "gutsrun.task", str(self.dir), tid]
            log_base = str(self.dir / "log" / f"{tid}.{{instance}}")
            try:
                self.backend.submit(tid, argv, res, log_base + ".out", log_base + ".err")
            except BaseException:
                del self._pending[tid]
                raise
        return future

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Accept no more calls; with `wait`, wait for the pending ones.
        `cancel_futures` cancels the pending tasks instead. Calls still
        pending after that fail with TaskFailed."""
        with self._lock:
            self._shutdown = True
            if cancel_futures:
                for fut in self._pending.values():
                    fut.set_exception(TaskFailed("cancelled at shutdown"))
                self._pending.clear()
        if wait:
            while True:
                with self._lock:
                    if not self._pending:
                        break
                time.sleep(self.poll_interval)
        self._stop.set()
        self._watcher.join()
        with self._lock:
            self.backend.close()
            for fut in self._pending.values():
                fut.set_exception(TaskFailed("executor shut down"))
            self._pending.clear()

    # --- internals ------------------------------------------------------------

    def _take_over_spool(self) -> None:
        """Remove the directories of older executors (left by a killed
        launcher on a long-lived HQ server). Their queued tasks then fail
        without running: their input is gone. A task that already runs
        keeps running its call and fails only when it publishes. A
        directory whose files are still open may stay behind (NFS); a
        later executor removes it."""
        for d in self.spool.iterdir():
            if d.is_dir() and d != self.dir:
                shutil.rmtree(d, ignore_errors=True)

    def _watch(self) -> None:
        while not self._stop.is_set():
            try:
                self._collect()
            except Exception:
                log.exception("gutsrun watcher")
            self._stop.wait(self.poll_interval)

    def _collect(self) -> None:
        with self._lock:
            ended = self.backend.poll()
        now = time.monotonic()
        for tid, error in ended:
            if error is None:
                self._finished[tid] = now
            else:
                stderr = self.dir / "log" / f"{tid}.*.err"
                self._deliver(tid, TaskFailed(f"task {tid}: {error}\n(stderr: {stderr})"), None)
        for tid, since in list(self._finished.items()):
            failed, value = None, None
            try:
                with open(self.dir / "out" / f"{tid}.pkl", "rb") as f:
                    value = pickle.load(f)
            except FileNotFoundError:
                if now - since < self.result_timeout:
                    continue  # not visible here yet; try again
                failed = TaskFailed(f"task {tid}: finished without writing a result")
            except Exception as e:
                failed = TaskFailed(f"task {tid}: unreadable result: {e!r}")
            del self._finished[tid]
            self._deliver(tid, failed, value)

    def _deliver(self, tid: str, failed: TaskFailed | None, value: Any) -> None:
        """Delete the task's files and settle its future, with `failed` if
        set, else with `value`."""
        (self.dir / "in" / f"{tid}.pkl").unlink(missing_ok=True)
        (self.dir / "out" / f"{tid}.pkl").unlink(missing_ok=True)
        with self._lock:
            fut = self._pending.pop(tid, None)
        if fut is not None:
            if failed is not None:
                fut.set_exception(failed)
            else:
                fut.set_result(value)


def _pythonpath(existing: str | None) -> str:
    parts = [_SRC] + ([existing] if existing else []) + [os.environ.get("PYTHONPATH", "")]
    return os.pathsep.join(p for p in parts if p)
