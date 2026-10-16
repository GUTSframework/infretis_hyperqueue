"""Executor contract, run against both backends (local and HQ)."""

import asyncio
import functools
import os
import pickle
import time

import pytest

import helpers
from conftest import wait_for
from gutsrun import RemoteTraceback, TaskFailed
from gutsrun import spool


def test_happy_path(make_executor):
    ex = make_executor()
    futures = [ex.submit(helpers.square, i) for i in range(8)]
    assert [f.result(timeout=60) for f in futures] == [i * i for i in range(8)]
    assert list(ex.map(helpers.square, [2, 3])) == [4, 9]


def test_run_in_executor(make_executor):
    """How infretis calls it: loop.run_in_executor(executor, partial(...))."""
    ex = make_executor()

    async def main():
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(ex, functools.partial(helpers.square, 7))

    assert asyncio.run(main()) == 49


def test_exception_reaches_future(make_executor):
    ex = make_executor()
    fut = ex.submit(helpers.boom, "bad move")
    with pytest.raises(ValueError, match="bad move") as info:
        fut.result(timeout=60)
    assert isinstance(info.value.__cause__, RemoteTraceback)
    assert "in boom" in str(info.value.__cause__)


def test_initializer_runs_before_each_call(make_executor):
    ex = make_executor(initializer=helpers.init_state, initargs=("ready",))
    assert ex.submit(helpers.read_state).result(timeout=60) == "ready"


def test_resources_cwd_env_threads(make_executor, tmp_path):
    ex = make_executor()
    rd = {"cores": 2, "cwd": str(tmp_path), "env": {"GUTSRUN_TEST": "yes"}}
    assert ex.submit(helpers.env, "OMP_NUM_THREADS", resource_dict=rd).result(timeout=60) == "2"
    assert ex.submit(helpers.cwd, resource_dict=rd).result(timeout=60) == str(tmp_path)
    assert ex.submit(helpers.env, "GUTSRUN_TEST", resource_dict=rd).result(timeout=60) == "yes"


def test_broken_task_fails_future(make_executor):
    ex = make_executor()
    fut = ex.submit(helpers.hard_exit, 3)
    with pytest.raises(TaskFailed):
        fut.result(timeout=60)
    # The executor keeps working.
    assert ex.submit(helpers.square, 3).result(timeout=60) == 9


def test_first_result_wins_duplicates_ignored(make_executor, tmp_path):
    ex = make_executor()
    started = tmp_path / "started"
    fut = ex.submit(helpers.slow, str(started), 1.0, "real")
    tid = "00000000"
    wait_for(lambda: list(tmp_path.glob("started.*")), what="task started")
    # A competing result (as from an HQ rerun) arrives first.
    spool.write_atomic(ex.dir / "out" / spool.result_name(tid, "9"), spool.dump(("ok", "first")))
    assert fut.result(timeout=60) == "first"
    # So does a result for a task nobody submitted.
    spool.write_atomic(ex.dir / "out" / spool.result_name("99999999", "0"), spool.dump(("ok", 1)))
    # The real task finishes after ~1 s; its late result must be dropped too.
    time.sleep(2.5)
    assert os.listdir(ex.dir / "out") == []
    assert ex.submit(helpers.square, 4).result(timeout=60) == 16


def test_finished_tasks_leave_no_files(make_executor):
    ex = make_executor()
    for f in [ex.submit(helpers.square, i) for i in range(5)]:
        f.result(timeout=60)
    ex.shutdown(wait=True)
    for sub in ("in", "out", "log"):
        assert os.listdir(ex.dir / sub) == [], sub


def test_submit_after_shutdown_raises(make_executor):
    ex = make_executor()
    ex.shutdown()
    with pytest.raises(RuntimeError):
        ex.submit(helpers.square, 1)


def test_unpicklable_result(make_executor):
    ex = make_executor()
    fut = ex.submit(functools.partial(eval, "lambda: 1"))
    with pytest.raises(RuntimeError, match="not picklable"):
        fut.result(timeout=60)


def test_stale_epoch_task_does_nothing(tmp_path):
    """A task from an epoch that is no longer current neither runs nor publishes."""
    from gutsrun import task

    spool_dir = tmp_path / "spool"
    old = spool_dir / "old-epoch"
    for sub in ("in", "out", "log"):
        (old / sub).mkdir(parents=True)
    marker = tmp_path / "ran"
    (old / "in" / "00000000.pkl").write_bytes(
        spool.dump([], (helpers.touch, (str(marker),), {}, None, ())))
    (spool_dir / spool.CURRENT).write_text("new-epoch")

    assert task.main([str(old), "00000000"]) == task.EXIT_STALE
    assert not marker.exists()
    assert os.listdir(old / "out") == []

    # Same input, current epoch: it runs and publishes.
    (spool_dir / spool.CURRENT).write_text("old-epoch")
    assert task.main([str(old), "00000000"]) == 0
    assert marker.exists()
    (result,) = os.listdir(old / "out")
    assert pickle.loads((old / "out" / result).read_bytes()) == ("ok", str(marker))
