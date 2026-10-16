import json
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from gutsrun import TaskExecutor
from gutsrun.backends.local import LocalBackend

HQ_CPUS = 4


def wait_for(cond, timeout=30.0, interval=0.05, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for {what}")


class HQCluster:
    """An `hq server` plus workers as real processes, in a temp server dir."""

    def __init__(self, root: Path):
        self.hq = shutil.which("hq")
        self.dir = root / "hq-server"
        self.root = root
        self.workers: list[subprocess.Popen] = []
        self.server = subprocess.Popen(
            [self.hq, "server", "start", "--server-dir", str(self.dir)],
            stdout=open(root / "hq-server.log", "wb"), stderr=subprocess.STDOUT)
        wait_for(lambda: (self.dir / "hq-current").exists(), what="hq server")
        self.add_worker()

    def cli(self, *args):
        out = subprocess.run([self.hq, *args, "--server-dir", str(self.dir),
                              "--output-mode", "json"],
                             capture_output=True, text=True, check=True)
        return json.loads(out.stdout)

    def add_worker(self):
        n = len(self.workers)
        proc = subprocess.Popen(
            [self.hq, "worker", "start", "--server-dir", str(self.dir),
             "--cpus", str(HQ_CPUS)],
            stdout=open(self.root / f"hq-worker{n}.log", "wb"), stderr=subprocess.STDOUT)
        self.workers.append(proc)
        alive = sum(1 for w in self.workers if w.poll() is None)
        # `worker list` shows connected (running) workers only.
        wait_for(lambda: len(self.cli("worker", "list")) >= alive, what="hq worker")
        return proc

    def stop(self):
        subprocess.run([self.hq, "server", "stop", "--server-dir", str(self.dir)],
                       capture_output=True)
        for p in [*self.workers, self.server]:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()


@pytest.fixture
def hq_cluster(tmp_path):
    if shutil.which("hq") is None:
        pytest.skip("no hq binary (run inside `nix develop`)")
    try:
        import hyperqueue  # noqa: F401
    except ImportError as e:
        pytest.skip(f"HyperQueue client not importable: {e}")
    cluster = HQCluster(tmp_path)
    yield cluster
    cluster.stop()


@pytest.fixture(params=["local", pytest.param("hq", marks=pytest.mark.hq)])
def make_executor(request, tmp_path):
    """Factory for a TaskExecutor on either backend; shuts them down after."""
    made = []
    if request.param == "hq":
        cluster = request.getfixturevalue("hq_cluster")

    def make(**kwargs):
        if request.param == "local":
            backend = LocalBackend(max_workers=kwargs.pop("max_workers", 4))
        else:
            from gutsrun.backends.hq import HQBackend
            kwargs.pop("max_workers", None)
            backend = HQBackend(str(cluster.dir))
        kwargs.setdefault("failure_poll_interval", 0.2)
        ex = TaskExecutor(backend, tmp_path / "spool", **kwargs)
        made.append(ex)
        return ex

    make.backend = request.param
    yield make
    for ex in made:
        ex.shutdown(wait=False, cancel_futures=True)
