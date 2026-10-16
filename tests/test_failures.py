"""Failure injection: a coordinator killed with tasks in flight, an HQ
worker killed under a running task."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

import helpers
from conftest import wait_for
from gutsrun import TaskExecutor
from gutsrun.backends.local import LocalBackend

HERE = Path(__file__).parent


def test_coordinator_restart_with_tasks_in_flight(tmp_path):
    """Kill the coordinator while its tasks run; a new one takes over the
    spool. The orphaned tasks finish but publish nothing, and the new
    executor works normally."""
    spool_dir = tmp_path / "spool"
    started = tmp_path / "started"
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(HERE.parent / "src"), str(HERE)])}
    old = subprocess.Popen(
        [sys.executable, str(HERE / "coordinator.py"), str(spool_dir), str(started), "3", "2"],
        stdout=subprocess.PIPE, text=True, env=env)
    old_epoch = old.stdout.readline().strip()
    wait_for(lambda: len(list(tmp_path.glob("started*"))) == 2, what="old tasks started")
    old.send_signal(signal.SIGKILL)
    old.wait()

    ex = TaskExecutor(LocalBackend(max_workers=2), spool_dir, failure_poll_interval=0.2)
    try:
        assert ex.epoch != old_epoch
        assert not (spool_dir / old_epoch).exists()
        assert ex.submit(helpers.square, 5).result(timeout=60) == 25
        time.sleep(4)  # the orphans finish (3 s) and must not publish
        assert os.listdir(ex.dir / "out") == []
        assert sorted(p.name for p in spool_dir.iterdir()) == sorted(["CURRENT", ex.epoch])
    finally:
        ex.shutdown()


@pytest.mark.hq
def test_hq_reruns_task_after_worker_crash(hq_cluster, tmp_path):
    from gutsrun.backends.hq import HQBackend

    ex = TaskExecutor(HQBackend(str(hq_cluster.dir)), tmp_path / "spool",
                      failure_poll_interval=0.2)
    try:
        started = tmp_path / "started"
        fut = ex.submit(helpers.slow, str(started), 2.0)
        wait_for(lambda: Path(f"{started}.0").exists(), what="first instance started")
        hq_cluster.workers[0].send_signal(signal.SIGKILL)
        hq_cluster.workers[0].wait()
        hq_cluster.add_worker()
        # HQ reruns it as instance 1; the instance-0 process died with its worker.
        assert fut.result(timeout=60) == "1"
    finally:
        ex.shutdown()


@pytest.mark.hq
def test_hq_failed_task_reports_stderr(hq_cluster, tmp_path):
    from gutsrun.backends.hq import HQBackend

    ex = TaskExecutor(HQBackend(str(hq_cluster.dir)), tmp_path / "spool",
                      failure_poll_interval=0.2)
    try:
        # The interpreter cannot start: the task fails without a result.
        ex.python = "/nonexistent/python"
        with pytest.raises(Exception, match="task 00000000"):
            ex.submit(helpers.square, 2).result(timeout=60)
    finally:
        ex.shutdown()
