"""Failure injection: a coordinator killed with tasks in flight, an HQ
worker killed under a running task."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import helpers
import pytest
from conftest import wait_for

from gutsrun import TaskExecutor, TaskFailed
from gutsrun.backends.local import LocalBackend

HERE = Path(__file__).parent


def test_coordinator_restart_with_tasks_in_flight(tmp_path):
    """Kill the coordinator while its tasks run; a new one takes over the
    spool. The orphaned tasks finish but publish nothing (their directory
    is gone), and the new executor works normally."""
    spool_dir = tmp_path / "spool"
    started = tmp_path / "started"
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(HERE.parent / "src"), str(HERE)])}
    old = subprocess.Popen(
        [sys.executable, str(HERE / "coordinator.py"), str(spool_dir), str(started), "3", "2"],
        stdout=subprocess.PIPE, text=True, env=env)
    assert old.stdout is not None
    old_dir = old.stdout.readline().strip()
    wait_for(lambda: len(list(tmp_path.glob("started*"))) == 2, what="old tasks started")
    old.send_signal(signal.SIGKILL)
    old.wait()

    ex = TaskExecutor(LocalBackend(max_workers=2), spool_dir)
    try:
        assert ex.dir.name != old_dir
        assert not (spool_dir / old_dir).exists()
        assert ex.submit(helpers.square, 5).result(timeout=60) == 25
        time.sleep(4)  # the orphans finish (3 s) and must not publish
        assert os.listdir(ex.dir / "out") == []
        assert [p.name for p in spool_dir.iterdir()] == [ex.dir.name]
    finally:
        ex.shutdown()


@pytest.mark.hq
def test_hq_reruns_task_after_worker_crash(hq_cluster, tmp_path):
    from gutsrun.hq import HQBackend

    ex = TaskExecutor(HQBackend(hq_cluster.dir), tmp_path / "spool")
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
def test_hq_task_that_cannot_start_reports_why(hq_cluster, tmp_path):
    from gutsrun.hq import HQBackend

    ex = TaskExecutor(HQBackend(hq_cluster.dir), tmp_path / "spool")
    try:
        # The interpreter cannot start: the task fails without a result.
        ex.python = "/nonexistent/python"
        with pytest.raises(TaskFailed, match="task 00000000: Cannot execute"):
            ex.submit(helpers.square, 2).result(timeout=60)
    finally:
        ex.shutdown()


@pytest.mark.hq
def test_hq_shutdown_cancels_running_tasks_with_sigint(hq_cluster, tmp_path):
    """HQ cancels with SIGINT to the task's process group; the call unwinds
    (an MD engine would stop its program there) and the task's log stays."""
    from gutsrun.hq import HQBackend

    ex = TaskExecutor(HQBackend(hq_cluster.dir), tmp_path / "spool")
    started = tmp_path / "started"
    fut = ex.submit(helpers.slow, str(started), 60.0)
    wait_for(lambda: Path(f"{started}.0").exists(), what="task started")
    ex.shutdown(wait=False, cancel_futures=True)
    with pytest.raises(TaskFailed, match="cancelled"):
        fut.result(timeout=1)
    stderr = ex.dir / "log" / "00000000.0.err"
    wait_for(lambda: "KeyboardInterrupt" in stderr.read_text(), what="KeyboardInterrupt")


@pytest.mark.hq
def test_hq_time_request_holds_a_move_that_would_not_fit(tmp_path):
    """A worker with less time left than a task requests does not start it."""
    import shutil

    from conftest import HQCluster

    from gutsrun.hq import HQBackend

    if shutil.which("hq") is None:
        pytest.skip("no hq binary (run inside `nix develop`)")
    cluster = HQCluster(tmp_path, "--time-limit", "10m")
    try:
        fits = TaskExecutor(HQBackend(cluster.dir, time_request="1m"), tmp_path / "a")
        too_long = TaskExecutor(HQBackend(cluster.dir, time_request="1h"), tmp_path / "b")
        held = too_long.submit(helpers.square, 3)
        assert fits.submit(helpers.square, 2).result(timeout=60) == 4
        time.sleep(1)
        assert not held.done()
        fits.shutdown()
        too_long.shutdown(wait=False, cancel_futures=True)
    finally:
        cluster.stop()
