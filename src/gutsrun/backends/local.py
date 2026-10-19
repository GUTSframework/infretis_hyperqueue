"""Local backend: one subprocess per task, for laptops and tests.

Like HyperQueue it sets OMP_NUM_THREADS to the task's cores (HQ does this
for every task), so thread settings behave the same on both backends.
"""

import os
import signal
import subprocess
from collections import deque

from gutsrun.backends import Resources

Queued = tuple[str, list[str], Resources, str, str]  # Backend.submit's arguments


class LocalBackend:
    """Tasks as local processes, at most `max_workers` at once (default:
    the number of CPUs)."""

    def __init__(self, max_workers: int | None = None) -> None:
        self.max_workers = max_workers or os.cpu_count() or 1
        self._queue: deque[Queued] = deque()
        self._running: dict[str, subprocess.Popen[bytes]] = {}
        self._ended: list[tuple[str, str | None]] = []

    def submit(self, tid: str, argv: list[str], res: Resources, stdout: str,
               stderr: str) -> None:
        self._queue.append((tid, argv, res, stdout, stderr))
        self._start_queued()

    def _start_queued(self) -> None:
        self._reap()  # free slots of tasks that exited since the last poll
        while self._queue and len(self._running) < self.max_workers:
            tid, argv, res, stdout, stderr = self._queue.popleft()
            env = {**os.environ, "OMP_NUM_THREADS": str(res.cores), **res.env,
                   "GUTSRUN_INSTANCE": "0"}
            with open(stdout.format(instance=0), "wb") as out, \
                 open(stderr.format(instance=0), "wb") as err:
                self._running[tid] = subprocess.Popen(
                    argv, cwd=res.cwd, env=env, stdout=out, stderr=err,
                    stdin=subprocess.DEVNULL, start_new_session=True)

    def poll(self) -> list[tuple[str, str | None]]:
        self._start_queued()
        ended, self._ended = self._ended, []
        return ended

    def _reap(self) -> None:
        for tid, proc in list(self._running.items()):
            code = proc.poll()
            if code is None:
                continue
            del self._running[tid]
            if code == 0:
                self._ended.append((tid, None))
            elif code < 0:
                self._ended.append((tid, f"killed by {signal.Signals(-code).name}"))
            else:
                self._ended.append((tid, f"exited with code {code}"))

    def release(self, tid: str) -> None:
        # The result is in, so the process is exiting; free its slot now
        # rather than at the next failure poll.
        proc = self._running.get(tid)
        if proc is not None:
            try:
                proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                return
        self._start_queued()

    def cancel(self, tid: str) -> None:
        self._queue = deque(t for t in self._queue if t[0] != tid)
        proc = self._running.get(tid)
        if proc is not None and proc.poll() is None:
            # The task runs in its own session; stop its children too (MD engines).
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    def close(self) -> None:
        self._queue.clear()
        for tid in list(self._running):
            self.cancel(tid)
        for proc in self._running.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
        self._running.clear()
