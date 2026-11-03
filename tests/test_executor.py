"""Executor contract, run against both backends (local and HQ)."""

import asyncio
import functools
import os
import time

import helpers
import pytest

from gutsrun import TaskExecutor, TaskFailed, spool
from gutsrun.backends import Resources


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
    """As the task's traceback in a TaskFailed; the task's logs are kept."""
    ex = make_executor()
    fut = ex.submit(helpers.boom, "bad move")
    with pytest.raises(TaskFailed, match="ValueError: bad move") as info:
        fut.result(timeout=60)
    assert "in boom" in str(info.value)
    assert "ValueError: bad move" in (ex.dir / "log" / "00000000.0.err").read_text()


def test_long_traceback_keeps_its_end(make_executor):
    ex = make_executor()
    with pytest.raises(TaskFailed, match="ValueError: the end") as info:
        ex.submit(helpers.long_traceback, "the end").result(timeout=60)
    assert "RuntimeError" not in str(info.value)  # the start was cut
    assert len(str(info.value)) < 2000 + len(str(ex.dir))


def test_initializer_runs_before_each_call(make_executor):
    ex = make_executor(initializer=helpers.init_state, initargs=("ready",))
    assert ex.submit(helpers.read_state).result(timeout=60) == "ready"


def test_resources_cwd_env_threads(make_executor, tmp_path):
    ex = make_executor()
    rd = {"cores": 2, "cwd": str(tmp_path), "env": {"GUTSRUN_TEST": "yes"}}
    assert ex.submit(helpers.env, "OMP_NUM_THREADS", resource_dict=rd).result(timeout=60) == "2"
    assert ex.submit(helpers.cwd, resource_dict=rd).result(timeout=60) == str(tmp_path)
    assert ex.submit(helpers.env, "GUTSRUN_TEST", resource_dict=rd).result(timeout=60) == "yes"


def test_task_environment_does_not_grow(make_executor):
    """Each task gets gutsrun's src on PYTHONPATH, without adding it to the
    executor's defaults: grown once per task, it would pass Linux's 128 KiB
    limit for one environment string (E2BIG) after a few thousand tasks."""
    ex = make_executor()
    default_env = dict(ex.default_resources.env)
    paths = [ex.submit(helpers.env, "PYTHONPATH").result(timeout=60) for _ in range(3)]
    assert paths[0] == paths[1] == paths[2]
    assert ex.default_resources.env == default_env


def test_resources_from_dict_never_returns_the_default():
    default = Resources(env={"A": "1"})
    for d in (None, {}, {"cores": 2}):
        res = Resources.from_dict(d, default)
        res.env["B"] = "2"
        assert res is not default
        assert default.env == {"A": "1"}


def test_broken_task_fails_future(make_executor):
    ex = make_executor()
    fut = ex.submit(helpers.hard_exit, 3)
    with pytest.raises(TaskFailed):
        fut.result(timeout=60)
    # The executor keeps working.
    assert ex.submit(helpers.square, 3).result(timeout=60) == 9


class Ends:
    """A backend whose tasks never run: the test says when they end."""

    def __init__(self) -> None:
        self.ended: list[tuple[str, str | None]] = []

    def submit(self, tid, argv, res, stdout, stderr) -> None:
        pass

    def poll(self) -> list[tuple[str, str | None]]:
        ended, self.ended = self.ended, []
        return ended

    def close(self) -> None:
        pass


def test_a_result_that_shows_late_is_still_read(tmp_path):
    """On a shared file system the result file may show up after the
    backend reports the task finished."""
    backend = Ends()
    ex = TaskExecutor(backend, tmp_path / "spool", poll_interval=0.01)
    try:
        fut = ex.submit(helpers.square, 3)
        backend.ended.append(("00000000", None))
        time.sleep(0.2)
        assert not fut.done()
        spool.write_atomic(ex.dir / "out" / "00000000.pkl", spool.dump(9))
        assert fut.result(timeout=5) == 9
        assert os.listdir(ex.dir / "out") == os.listdir(ex.dir / "in") == []
        backend.ended.append(("00000000", None))  # a repeated event changes nothing
    finally:
        ex.shutdown()


def test_finished_without_a_result_fails(tmp_path):
    backend = Ends()
    ex = TaskExecutor(backend, tmp_path / "spool", poll_interval=0.01, result_timeout=0.1)
    try:
        fut = ex.submit(helpers.square, 3)
        backend.ended.append(("00000000", None))
        with pytest.raises(TaskFailed, match="without writing a result"):
            fut.result(timeout=5)
    finally:
        ex.shutdown()


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
    with pytest.raises(TaskFailed, match="pickle"):
        fut.result(timeout=60)


def test_queued_task_of_a_taken_over_spool_does_not_run(tmp_path):
    """A task that had not started when a newer executor removed its
    executor's directory does not run."""
    from gutsrun import task
    from gutsrun.backends.local import LocalBackend

    spool_dir = tmp_path / "spool"
    old = spool_dir / "old"
    for sub in ("in", "out", "log"):
        (old / sub).mkdir(parents=True)
    marker = tmp_path / "ran"
    (old / "in" / "00000000.pkl").write_bytes(
        spool.dump([], (helpers.touch, (str(marker),), {}, None, ())))

    TaskExecutor(LocalBackend(max_workers=1), spool_dir).shutdown()
    assert not old.exists()
    with pytest.raises(FileNotFoundError):
        task.main([str(old), "00000000"])
    assert not marker.exists()
