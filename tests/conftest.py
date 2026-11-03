import json
import shutil
import subprocess
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from gutsrun import TaskExecutor
from gutsrun.hq import HQBackend

HQ_CPUS = 4


def wait_for(cond: Callable[[], object], timeout: float = 30.0, interval: float = 0.05,
             what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for {what}")


class HQCluster:
    """An `hq server` plus workers as real processes, in a temp server dir."""

    def __init__(self, root: Path, *worker_args: str) -> None:
        hq = shutil.which("hq")
        assert hq is not None, "no hq binary"
        self.hq = hq
        self.dir = root / "hq-server"
        self.root = root
        self.workers: list[subprocess.Popen[bytes]] = []
        self.server = subprocess.Popen(
            [self.hq, "server", "start", "--server-dir", str(self.dir)],
            stdout=open(root / "hq-server.log", "wb"), stderr=subprocess.STDOUT)
        wait_for(lambda: (self.dir / "hq-current").exists(), what="hq server")
        self.add_worker(*worker_args)

    def cli(self, *args: str) -> Any:
        out = subprocess.run([self.hq, *args, "--server-dir", str(self.dir),
                              "--output-mode", "json"],
                             capture_output=True, text=True, check=True)
        return json.loads(out.stdout)

    def add_worker(self, *args: str) -> subprocess.Popen[bytes]:
        n = len(self.workers)
        proc = subprocess.Popen(
            [self.hq, "worker", "start", "--server-dir", str(self.dir),
             "--cpus", str(HQ_CPUS), *args],
            stdout=open(self.root / f"hq-worker{n}.log", "wb"), stderr=subprocess.STDOUT)
        self.workers.append(proc)
        alive = sum(1 for w in self.workers if w.poll() is None)
        # `worker list` shows connected (running) workers only.
        wait_for(lambda: len(self.cli("worker", "list")) >= alive, what="hq worker")
        return proc

    def stop(self) -> None:
        subprocess.run([self.hq, "server", "stop", "--server-dir", str(self.dir)],
                       capture_output=True)
        for p in [*self.workers, self.server]:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()


@pytest.fixture
def hq_cluster(tmp_path: Path) -> Iterator[HQCluster]:
    if shutil.which("hq") is None:
        pytest.skip("no hq binary (run inside `nix develop`)")
    cluster = HQCluster(tmp_path)
    yield cluster
    cluster.stop()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark `hq` every test that needs an HQ server, for `-m "not hq"`."""
    for item in items:
        if {"hq_cluster", "make_executor"} & set(getattr(item, "fixturenames", ())):
            item.add_marker(pytest.mark.hq)


@pytest.fixture
def make_executor(hq_cluster: HQCluster,
                  tmp_path: Path) -> Iterator[Callable[..., TaskExecutor]]:
    """Factory for a TaskExecutor on a fresh HQ server; shuts them down after."""
    made: list[TaskExecutor] = []

    def make(**kwargs: Any) -> TaskExecutor:
        ex = TaskExecutor(HQBackend(hq_cluster.dir), tmp_path / "spool", **kwargs)
        made.append(ex)
        return ex

    yield make
    for ex in made:
        ex.shutdown(wait=False, cancel_futures=True)
