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

Completion: a watcher thread lists `out/` every `poll_interval` and asks
the backend about failures every `failure_poll_interval`. The first
result for a task wins; later ones (an HQ rerun, a duplicate) are deleted.
Inputs, results and logs of finished tasks are deleted (inode budget).
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
    """The task process broke without delivering a result."""


class RemoteTraceback(Exception):
    """Carries the task-side traceback, as `__cause__` (like ProcessPoolExecutor)."""

    def __str__(self) -> str:
        return self.args[0]


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
    - `poll_interval`, `failure_poll_interval`: seconds between looks for
      results, and between asking the backend about failed tasks.
    - `keep_failed_logs`: keep a failed task's stdout and stderr.
    """

    def __init__(self, backend: Backend, spool_dir: str | Path, *,
                 max_workers: int | None = None,
                 initializer: Callable[..., object] | None = None,
                 initargs: Iterable[Any] = (), resource_dict: dict[str, Any] | None = None,
                 python: str = sys.executable, poll_interval: float = 0.02,
                 failure_poll_interval: float = 2.0, keep_failed_logs: bool = True) -> None:
        self.backend = backend
        self.max_workers = max_workers  # informational; the backend limits concurrency
        self.initializer, self.initargs = initializer, tuple(initargs)
        self.default_resources = Resources.from_dict(resource_dict, Resources())
        self.python = python
        self.poll_interval = poll_interval
        self.failure_poll_interval = failure_poll_interval
        self.keep_failed_logs = keep_failed_logs

        self.spool = Path(spool_dir).resolve()
        self.dir = self.spool / spool.new_name()
        for sub in ("in", "out", "log"):
            (self.dir / sub).mkdir(parents=True)
        self._take_over_spool()

        self._lock = threading.Lock()  # guards _pending, _seq and backend calls
        self._pending: dict[str, Future[Any]] = {}
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
                for tid, fut in list(self._pending.items()):
                    self.backend.cancel(tid)
                    del self._pending[tid]
                    fut.set_exception(TaskFailed("cancelled at shutdown"))
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
        last_failure_poll = time.monotonic()
        while not self._stop.is_set():
            try:
                self._collect_results()
                if time.monotonic() - last_failure_poll >= self.failure_poll_interval:
                    last_failure_poll = time.monotonic()
                    self._collect_failures()
            except Exception:
                log.exception("gutsrun watcher")
            self._stop.wait(self.poll_interval)

    def _collect_results(self) -> None:
        out = self.dir / "out"
        for name in os.listdir(out):
            parsed = spool.parse_result_name(name)
            if parsed is None:
                continue
            tid, _instance = parsed
            path = out / name
            with self._lock:
                fut = self._pending.pop(tid, None)
            if fut is None:  # duplicate, late or unknown: the first result won
                path.unlink(missing_ok=True)
                continue
            try:
                with open(path, "rb") as f:
                    result = pickle.load(f)
            except Exception as e:
                result = ("err", TaskFailed(f"task {tid}: unreadable result: {e!r}"), "")
            if result[0] == "ok":
                fut.set_result(result[1])
            else:
                exc = result[1]
                if result[2]:
                    exc.__cause__ = RemoteTraceback(f'\n"""\n{result[2]}"""')
                fut.set_exception(exc)
            # Free the backend slot only after delivering: the local backend
            # waits here for the task process to exit (~0.1 s with infretis loaded).
            with self._lock:
                self.backend.release(tid)
            self._cleanup(tid, keep_logs=False)

    def _collect_failures(self) -> None:
        with self._lock:
            ended = self.backend.poll()
        if not ended:
            return
        self._collect_results()  # a result may have landed just before the exit
        for tid, error in ended:
            with self._lock:
                fut = self._pending.pop(tid, None)
                if fut is not None:
                    self.backend.release(tid)
            if fut is None:
                continue
            msg = error or "exited without writing a result"
            fut.set_exception(TaskFailed(f"task {tid}: {msg}{self._stderr_tail(tid)}"))
            self._cleanup(tid, keep_logs=self.keep_failed_logs)

    def _stderr_tail(self, tid: str, n: int = 20) -> str:
        tails = []
        for p in sorted((self.dir / "log").glob(f"{tid}.*.err")):
            lines = p.read_text(errors="replace").splitlines()[-n:]
            if lines:
                tails.append(f"\n--- {p.name} ---\n" + "\n".join(lines))
        return "".join(tails)

    def _cleanup(self, tid: str, keep_logs: bool) -> None:
        (self.dir / "in" / f"{tid}.pkl").unlink(missing_ok=True)
        for p in (self.dir / "out").glob(f"{tid}.*.pkl"):
            p.unlink(missing_ok=True)
        if not keep_logs:
            for p in (self.dir / "log").glob(f"{tid}.*"):
                p.unlink(missing_ok=True)


def _pythonpath(existing: str | None) -> str:
    parts = [_SRC] + ([existing] if existing else []) + [os.environ.get("PYTHONPATH", "")]
    return os.pathsep.join(p for p in parts if p)
