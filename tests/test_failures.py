"""Failure injection on a real HQ server: a worker killed under a running
task, a task that cannot start, a cancel, a lost event stream."""

import signal
import time
from pathlib import Path

import helpers
import pytest
from conftest import wait_for

from gutsrun import TaskExecutor, TaskFailed
from gutsrun.hq import HQBackend


def test_hq_reruns_task_after_worker_crash(hq_cluster, tmp_path):
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


def test_hq_task_that_cannot_start_reports_why(hq_cluster, tmp_path):
    ex = TaskExecutor(HQBackend(hq_cluster.dir), tmp_path / "spool")
    try:
        # The interpreter cannot start: the task fails without a result.
        ex.python = "/nonexistent/python"
        with pytest.raises(TaskFailed, match="task 00000000: Cannot execute"):
            ex.submit(helpers.square, 2).result(timeout=60)
    finally:
        ex.shutdown()


def test_hq_shutdown_cancels_running_tasks_with_sigint(hq_cluster, tmp_path):
    """HQ cancels with SIGINT to the task's process group; the call unwinds
    (an MD engine would stop its program there) and the task's log stays."""
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


def test_hq_lost_event_stream_fails_the_tasks_in_flight(hq_cluster, tmp_path):
    """The run then stops, and its next start redoes the moves."""
    backend = HQBackend(hq_cluster.dir)
    ex = TaskExecutor(backend, tmp_path / "spool")
    started = tmp_path / "started"
    try:
        fut = ex.submit(helpers.slow, str(started), 60.0)
        wait_for(lambda: Path(f"{started}.0").exists(), what="task started")
        backend._stream.kill()
        with pytest.raises(TaskFailed, match="hq journal stream ended"):
            fut.result(timeout=10)
    finally:
        ex.shutdown(wait=False, cancel_futures=True)
