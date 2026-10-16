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

`timings_path`: if set, one JSON line per finished task with wall-clock
times: submitted, start (runner process started), loaded (call unpickled,
its modules imported), inited (initializer done), done (call done),
collected (result read by the executor), plus instance and host.
"""

import json
import logging
import os
import pickle
import shutil
import sys
import threading
import time
from concurrent.futures import Executor, Future
from pathlib import Path

from gutsrun import spool
from gutsrun.backends import Backend, Resources

log = logging.getLogger(__name__)

_SRC = str(Path(__file__).resolve().parent.parent)  # makes `gutsrun` importable in tasks


class TaskFailed(Exception):
    """The task process broke without delivering a result."""


class RemoteTraceback(Exception):
    """Carries the task-side traceback, as `__cause__` (like ProcessPoolExecutor)."""

    def __str__(self):
        return self.args[0]


class TaskExecutor(Executor):
    def __init__(self, backend: Backend, spool_dir, *, max_workers: int | None = None,
                 initializer=None, initargs=(), resource_dict: dict | None = None,
                 python: str = sys.executable, poll_interval: float = 0.02,
                 failure_poll_interval: float = 2.0, keep_failed_logs: bool = True,
                 timings_path=None):
        self.backend = backend
        self.max_workers = max_workers  # informational; the backend limits concurrency
        self.initializer, self.initargs = initializer, tuple(initargs)
        self.default_resources = Resources.from_dict(resource_dict, Resources())
        self.python = python
        self.poll_interval = poll_interval
        self.failure_poll_interval = failure_poll_interval
        self.keep_failed_logs = keep_failed_logs
        self._timings = open(timings_path, "a") if timings_path else None
        self._submitted: dict[str, float] = {}

        self.spool = Path(spool_dir).resolve()
        self.epoch = spool.new_epoch()
        self.dir = self.spool / self.epoch
        for sub in ("in", "out", "log"):
            (self.dir / sub).mkdir(parents=True)
        self._take_over_spool()

        self._lock = threading.Lock()  # guards _pending, _seq and backend calls
        self._pending: dict[str, Future] = {}
        self._seq = 0
        self._shutdown = False
        self._stop = threading.Event()
        self._watcher = threading.Thread(target=self._watch, name="gutsrun-watcher", daemon=True)
        self._watcher.start()

    # --- Executor API -------------------------------------------------------

    def submit(self, fn, /, *args, resource_dict: dict | None = None, **kwargs):
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
            future = Future()
            future.set_running_or_notify_cancel()  # it runs as soon as the backend starts it
            self._pending[tid] = future
            self._submitted[tid] = time.time()
            argv = [self.python, "-m", "gutsrun.task", str(self.dir), tid]
            log_base = str(self.dir / "log" / f"{tid}.{{instance}}")
            try:
                self.backend.submit(tid, argv, res, log_base + ".out", log_base + ".err")
            except BaseException:
                del self._pending[tid]
                raise
        return future

    def shutdown(self, wait=True, *, cancel_futures=False):
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
            for tid, fut in self._pending.items():
                fut.set_exception(TaskFailed("executor shut down"))
            self._pending.clear()
            if self._timings:
                self._timings.close()

    # --- internals ------------------------------------------------------------

    def _take_over_spool(self):
        """Make this epoch the live one; tasks of older epochs stop publishing.

        Older epoch directories are removed. A stale task still writing into
        one fails harmlessly.
        """
        spool.write_atomic(self.spool / spool.CURRENT, self.epoch.encode())
        for d in self.spool.iterdir():
            if d.is_dir() and d.name != self.epoch:
                shutil.rmtree(d, ignore_errors=True)

    def _watch(self):
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

    def _collect_results(self):
        out = self.dir / "out"
        for name in os.listdir(out):
            parsed = spool.parse_result_name(name)
            if parsed is None:
                continue
            tid, _instance = parsed
            path = out / name
            with self._lock:
                fut = self._pending.pop(tid, None)
                if fut is not None:
                    self.backend.release(tid)
            if fut is None:  # duplicate, late or unknown: the first result won
                path.unlink(missing_ok=True)
                continue
            try:
                with open(path, "rb") as f:
                    published = pickle.load(f)
                result = published["result"]
            except Exception as e:
                fut.set_exception(TaskFailed(f"task {tid}: unreadable result: {e!r}"))
                continue
            self._record_timings(tid, published.get("t", {}))
            if result[0] == "ok":
                fut.set_result(result[1])
            else:
                exc = result[1]
                exc.__cause__ = RemoteTraceback(f'\n"""\n{result[2]}"""')
                fut.set_exception(exc)
            self._cleanup(tid, keep_logs=False)

    def _record_timings(self, tid, t):
        submitted = self._submitted.pop(tid, None)
        if self._timings is None:
            return
        row = {"tid": tid, "submitted": submitted, **t, "collected": time.time()}
        self._timings.write(json.dumps(row) + "\n")
        self._timings.flush()

    def _collect_failures(self):
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
            self._submitted.pop(tid, None)
            msg = error or "exited without writing a result"
            fut.set_exception(TaskFailed(f"task {tid}: {msg}{self._stderr_tail(tid)}"))
            self._cleanup(tid, keep_logs=self.keep_failed_logs)

    def _stderr_tail(self, tid, n=20):
        tails = []
        for p in sorted((self.dir / "log").glob(f"{tid}.*.err")):
            lines = p.read_text(errors="replace").splitlines()[-n:]
            if lines:
                tails.append(f"\n--- {p.name} ---\n" + "\n".join(lines))
        return "".join(tails)

    def _cleanup(self, tid, keep_logs):
        (self.dir / "in" / f"{tid}.pkl").unlink(missing_ok=True)
        for p in (self.dir / "out").glob(f"{tid}.*.pkl"):
            p.unlink(missing_ok=True)
        if not keep_logs:
            for p in (self.dir / "log").glob(f"{tid}.*"):
                p.unlink(missing_ok=True)


def _pythonpath(existing: str | None) -> str:
    parts = [_SRC] + ([existing] if existing else []) + [os.environ.get("PYTHONPATH", "")]
    return os.pathsep.join(p for p in parts if p)
