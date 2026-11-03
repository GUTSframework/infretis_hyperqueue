"""Local backend: one subprocess per task, for laptops and tests.

Like HyperQueue it sets OMP_NUM_THREADS to the task's cores (HQ does this
for every task), gives each task a file for its error in
HQ_ERROR_FILENAME, and deletes the logs of tasks that finish, so tasks
behave the same on both backends.
"""

import os
import signal
import subprocess
from collections import deque
from pathlib import Path

from gutsrun.backends import Resources

Queued = tuple[str, list[str], Resources, str, str]  # Backend.submit's arguments


class LocalBackend:
    """Tasks as local processes, at most `max_workers` at once (default:
    the number of CPUs)."""

    def __init__(self, max_workers: int | None = None) -> None:
        self.max_workers = max_workers or os.cpu_count() or 1
        self._queue: deque[Queued] = deque()
        self._running: dict[str, tuple[subprocess.Popen[bytes], list[Path]]] = {}

    def submit(self, tid: str, argv: list[str], res: Resources, stdout: str,
               stderr: str) -> None:
        self._queue.append((tid, argv, res, stdout, stderr))

    def _start_queued(self) -> None:
        while self._queue and len(self._running) < self.max_workers:
            tid, argv, res, stdout, stderr = self._queue.popleft()
            logs = [Path(stdout.format(instance=0)), Path(stderr.format(instance=0))]
            error = logs[1].with_suffix(".error")
            error.unlink(missing_ok=True)
            env = {**os.environ, "OMP_NUM_THREADS": str(res.cores), **res.env,
                   "GUTSRUN_INSTANCE": "0", "HQ_ERROR_FILENAME": str(error)}
            with open(logs[0], "wb") as out, open(logs[1], "wb") as err:
                proc = subprocess.Popen(
                    argv, cwd=res.cwd, env=env, stdout=out, stderr=err,
                    stdin=subprocess.DEVNULL, start_new_session=True)
            self._running[tid] = (proc, [*logs, error])

    def poll(self) -> list[tuple[str, str | None]]:
        ended: list[tuple[str, str | None]] = []
        for tid, (proc, files) in list(self._running.items()):
            code = proc.poll()
            if code is None:
                continue
            del self._running[tid]
            if code == 0:
                for path in files:
                    path.unlink(missing_ok=True)
                ended.append((tid, None))
            elif files[2].exists() and (text := files[2].read_text(errors="replace")):
                ended.append((tid, text))
            elif code < 0:
                ended.append((tid, f"killed by {signal.Signals(-code).name}"))
            else:
                ended.append((tid, f"exited with code {code}"))
        self._start_queued()
        return ended

    def close(self) -> None:
        self._queue.clear()
        # Each task runs in its own session; signal all of it, with SIGINT as
        # HQ does. infretis's engines start their MD program in a session of
        # their own, which this does not reach: the move unwinds and its
        # engine stops the program.
        for proc, _ in self._running.values():
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGINT)
                except ProcessLookupError:
                    pass
        for proc, _ in self._running.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
        self._running.clear()
