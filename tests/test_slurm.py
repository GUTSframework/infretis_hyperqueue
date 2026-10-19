"""gutsrun.slurm (chained Slurm jobs) without Slurm: fake sbatch, scancel and
squeue record their calls, and a counting workload (chain_workload.py)
stands in for infretis. Links run as real processes, as in a job."""

import dataclasses
import getpass
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import tomllib
from pathlib import Path

import pytest

import gutsrun_infretis.chain as cli
from gutsrun.slurm import (ChainError, Site, Spec, StateError, events, follow, load_spec,
                           note, seconds, start, status, stop, venv_stale, walltime,
                           why_over)
from gutsrun_infretis.chain import InfretisRun
from conftest import wait_for

REPO = Path(__file__).resolve().parent.parent

SITE = """\
[slurm]
partition = "testq"
cores_per_node = 2
max_walltime_day = "00:15:00"

[node]
local_disk = "{local_disk}"
local_venv = {local_venv}
"""

FAKE_SBATCH = """\
#!/usr/bin/env bash
echo "sbatch $*" >> "$FAKE_LOG"
n=$(( $(grep -c '^sbatch' "$FAKE_LOG") + 1000 ))
echo "$n"
"""
FAKE_SCANCEL = '#!/usr/bin/env bash\necho "scancel $*" >> "$FAKE_LOG"\n'
# Prints $FAKE_SQUEUE (lines of "<id> <state> <time> <reason>"); fails like an
# unreachable slurmctld when it is unset.
FAKE_SQUEUE = """\
#!/usr/bin/env bash
[ -n "${FAKE_SQUEUE+x}" ] || exit 1
printf '%s\\n' "$FAKE_SQUEUE"
"""
FAKE_HQ = '#!/usr/bin/env bash\necho "hyperqueue v0.26.2"\n'


class Project:
    """A gutsrun checkout (sites/, .venv, fake Slurm commands) and a run directory."""

    def __init__(self, root: Path, local_venv: bool = False):
        self.root = root
        self.local = root / "local"
        self.local.mkdir()
        (root / "sites").mkdir()
        (root / "sites" / "test.toml").write_text(SITE.format(
            local_venv=str(local_venv).lower(), local_disk=self.local))
        (root / ".venv" / "bin").mkdir(parents=True)
        (root / ".venv" / "lib" / "python3.12" / "site-packages").mkdir(parents=True)
        (root / ".venv" / "bin" / "python").symlink_to(sys.executable)
        self.fakebin = root / "fakebin"
        self.fakebin.mkdir()
        for name, text in [("sbatch", FAKE_SBATCH), ("scancel", FAKE_SCANCEL),
                           ("squeue", FAKE_SQUEUE)]:
            (self.fakebin / name).write_text(text)
            (self.fakebin / name).chmod(0o755)
        self.log = root / "fake.log"
        self.log.touch()
        self.rundir = root / "run"
        self.rundir.mkdir()
        self.env = {**os.environ, "FAKE_LOG": str(self.log),
                    "PATH": os.pathsep.join([str(self.fakebin), os.environ["PATH"]]),
                    "PYTHONPATH": str(REPO / "src")}
        self.env.pop("FAKE_SQUEUE", None)

    def use_fakes(self, monkeypatch: pytest.MonkeyPatch, squeue: str | None = None) -> None:
        """Make the fake Slurm commands the ones this process calls."""
        monkeypatch.setenv("PATH", self.env["PATH"])
        monkeypatch.setenv("FAKE_LOG", str(self.log))
        if squeue is None:
            monkeypatch.delenv("FAKE_SQUEUE", raising=False)
        else:
            monkeypatch.setenv("FAKE_SQUEUE", squeue)

    def make_spec(self, links: int = 2, **counter) -> Spec:
        return Spec(rundir=str(self.rundir), site="test", name="r",
                    workload="chain_workload:Counter", pythonpath=[str(REPO / "tests")],
                    args=counter, time="00:05:00", links=links, root=str(self.root))

    def spec(self, links: int = 2, **counter) -> Spec:
        spec = self.make_spec(links, **counter)
        start(spec, submit_first=False)
        return spec

    def link(self, spec: Spec, n: int, wait: bool = True, **env):
        argv = [sys.executable, "-m", "gutsrun.slurm", "link", str(spec.path), str(n)]
        proc = subprocess.Popen(argv, cwd=self.root, env={**self.env, **env},
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if not wait:
            return proc
        out, _ = proc.communicate(timeout=120)
        return proc.returncode, out

    def calls(self, cmd: str) -> list[str]:
        return [line for line in self.log.read_text().splitlines() if line.startswith(cmd)]


def step(spec: Spec) -> int:
    return json.loads((Path(spec.rundir) / "state.json").read_text())["step"]


@pytest.fixture
def project(tmp_path):
    return Project(tmp_path)


@pytest.mark.parametrize("text, expected", [
    ("15", 900), ("15:30", 930), ("00:14:00", 840), ("48:00:00", 172800),
    ("2-0", 172800), ("1-2:30", 95400), ("1-02:03:04", 93784)])
def test_seconds_reads_slurm_walltimes(text, expected):
    assert seconds(text) == expected
    assert seconds(walltime(expected)) == expected


# --- sites --------------------------------------------------------------------------


def test_template_lists_every_site_key():
    text = (REPO / "sites" / "template.toml").read_text()
    # Optional keys are commented out with their example value.
    data = tomllib.loads("\n".join(line.removeprefix("# ") if " = " in line else line
                                   for line in text.splitlines()))
    assert {table: set(keys) for table, keys in data.items()} == Site.KEYS
    Site(REPO, "template")
    Site(REPO, "das6")


def test_site_refusals(project):
    path = project.root / "sites" / "test.toml"
    path.write_text(path.read_text() + "typo = 1\n")
    with pytest.raises(ChainError, match="unknown in \\[node\\]: typo"):
        Site(project.root, "test")
    path.write_text('[slurm]\npartition = "q"\ncores_per_node = 2\nexclusive = false\n')
    with pytest.raises(ChainError, match="not supported"):
        Site(project.root, "test")
    with pytest.raises(ChainError, match="known: test"):
        Site(project.root, "nosuch")


# --- starting -----------------------------------------------------------------------


def test_start_submits_link_1_with_site_options(project, monkeypatch):
    project.use_fakes(monkeypatch)
    spec = project.make_spec()
    assert start(spec) == "1001"
    [call] = project.calls("sbatch")
    for part in ["--partition=testq", "--exclusive", "--nodes=1", "--time=00:05:00",
                 "--signal=B:TERM@90", f"--chdir={project.rundir}",
                 f"--output={project.rundir}/.gutsrun/link1-%j.out",
                 f"exec {project.root}/.venv/bin/python -m gutsrun.slurm link",
                 f"{spec.path} 1"]:
        assert part in call
    assert "--dependency" not in call
    assert load_spec(project.rundir) == spec
    assert [e["event"] for e in events(spec)] == ["chain", "submitted"]


def test_start_refusals(project):
    def spec(**changes) -> Spec:
        return dataclasses.replace(project.make_spec(), **changes)

    with pytest.raises(ChainError, match="daytime limit"):
        start(spec(time="00:15:00"), submit_first=False)
    with pytest.raises(ChainError, match="no time to run"):
        start(spec(time="00:02:00"), submit_first=False)
    with pytest.raises(ChainError, match="no run directory"):
        start(spec(rundir="/nonexistent"), submit_first=False)
    (project.rundir / "state.json").write_text('{"step": 5}')
    with pytest.raises(ChainError, match="is done: step 5 of 5"):
        start(project.make_spec(), submit_first=False)
    (project.rundir / "state.json").write_text("{")
    with pytest.raises(StateError, match="does not parse"):
        start(project.make_spec(), submit_first=False)
    (project.rundir / "state.json").unlink()
    start(spec(time="00:15:00"), night=True, submit_first=False)


def test_a_new_chain_waits_for_the_last_to_leave_the_queue(project, monkeypatch):
    project.use_fakes(monkeypatch, squeue="1001 PENDING 0:00 (None)")
    spec = project.make_spec()
    start(spec)
    with pytest.raises(ChainError, match="still queued or running .*1001"):
        start(spec)
    project.use_fakes(monkeypatch)  # squeue does not answer
    with pytest.raises(ChainError, match="squeue does not answer"):
        start(spec)
    project.use_fakes(monkeypatch, squeue="")
    assert start(spec) == "1002"
    assert [e["event"] for e in events(spec)] == ["chain", "submitted"]  # the new chain's


# --- links --------------------------------------------------------------------------


def test_done_cancels_the_pre_submitted_link(project):
    spec = project.spec(links=3)
    rc, out = project.link(spec, 1)
    assert rc == 0, out
    assert step(spec) == 5
    assert (project.rundir / "cwd.txt").read_text() == str(project.rundir)
    [sbatch] = project.calls("sbatch")
    assert "--dependency=afterany:nojob-" in sbatch and f"{spec.path} 2" in sbatch
    assert project.calls("scancel") == ["scancel 1001"]
    ended = [e for e in events(spec) if e["event"] == "ended"]
    assert [(e["link"], e["result"], e["progress"]) for e in ended] == [
        (1, "done", "step 5 of 5")]


def test_sigterm_stops_and_the_next_link_continues(project):
    spec = project.spec(steps=200, delay=0.02)
    proc = project.link(spec, 1, wait=False)
    wait_for(lambda: (project.rundir / "state.json").exists() and step(spec) >= 3,
             what="workload progress")
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=60)
    assert proc.returncode == 0, out
    assert "link 2 continues" in out
    assert project.calls("scancel") == []
    stopped = step(spec)
    assert 3 <= stopped < 200

    rc, out = project.link(spec, 2)
    assert rc == 0, out
    assert f"continuing at step {stopped} of 200" in out
    assert (project.rundir / "before2").exists()
    assert step(spec) == 200
    assert len(project.calls("sbatch")) == 1  # link 2 is the last


def test_failure_cancels_the_next_link(project):
    spec = project.spec(fail_at=3)
    rc, out = project.link(spec, 1)
    assert rc == 1
    assert "workload failed (exit 1) at step 2 of 5" in out
    assert project.calls("scancel") == ["scancel 1001"]


def test_unreadable_state_stops_the_chain_before_submitting(project):
    spec = project.spec()
    (project.rundir / "state.json").write_text("{")
    rc, out = project.link(spec, 2)
    assert rc == 1
    assert "chain stops" in out
    assert project.calls("sbatch") == []


def test_finished_run_makes_later_links_exit_at_once(project):
    spec = project.spec(links=3)
    assert project.link(spec, 1)[0] == 0
    rc, out = project.link(spec, 2)
    assert rc == 0 and "done: step 5 of 5" in out
    assert len(project.calls("sbatch")) == 1


# --- status and stop ----------------------------------------------------------------


def test_status_reports_progress_and_each_link(project, monkeypatch, capsys):
    spec = project.spec(links=3, steps=200, delay=0.02)
    proc = project.link(spec, 1, wait=False)
    wait_for(lambda: (project.rundir / "state.json").exists(), what="workload progress")
    proc.send_signal(signal.SIGTERM)
    proc.communicate(timeout=60)

    project.use_fakes(monkeypatch, squeue="1001 PENDING 0:00 (Dependency)")
    report = status(spec)
    assert report.active and report.over is None
    assert report.progress.startswith("r (test): step ")
    assert "ended" in report.links[0] and "stopped at the walltime" in report.links[0]
    assert report.links[1].endswith("PENDING 0:00 (Dependency)")
    assert "job 1001" in report.links[1]
    assert report.states == ["1 ended", "2 PENDING (Dependency)"]
    # The job's time is not part of its state.
    project.use_fakes(monkeypatch, squeue="1001 PENDING 0:30 (Dependency)")
    assert status(spec).states == report.states

    project.use_fakes(monkeypatch, squeue="")
    report = status(spec)
    assert not report.active and report.links[1].endswith("cancelled before it started")
    assert report.over.startswith(f"link 1: stopped at the walltime; see {spec.dir}/link1-")
    follow(spec, interval=0)  # returns: nothing left in the queue
    assert "chain over: link 1" in capsys.readouterr().out

    project.use_fakes(monkeypatch)
    report = status(spec)
    assert report.active and "(squeue does not answer)" in report.links


def test_why_a_chain_is_over(project):
    spec = project.make_spec(links=2)
    project.rundir.joinpath(".gutsrun").mkdir()

    def ended(link: int, result: str) -> dict:
        return {"link": link, "job": "7", "result": result}

    assert why_over(spec, ended(1, "done")) == "the run is done"
    assert why_over(spec, ended(2, "stopped at the walltime")) == (
        "all 2 links ran; `submit` again to continue")
    assert why_over(spec, ended(1, "failed (exit 1)")).startswith("link 1: failed (exit 1)")
    assert why_over(spec, None).startswith("no link ran to the end")
    spec.stop_file.touch()
    assert why_over(spec, ended(1, "stopped by request")).startswith("stopped by request")


def test_stop_cancels_queued_links_and_signals_the_running_one(project, monkeypatch):
    spec = project.make_spec()
    project.use_fakes(monkeypatch, squeue="1001 RUNNING 0:10 node1")
    start(spec)
    note(spec, "submitted", link=2, job="1002")
    monkeypatch.setenv("FAKE_SQUEUE", "1001 RUNNING 0:10 node1\n1002 PENDING 0:00 (Dependency)")
    assert stop(spec) == ["link 1 (job 1001): sent SIGTERM", "link 2 (job 1002): cancelled"]
    assert project.calls("scancel") == ["scancel --batch --signal=TERM 1001", "scancel 1002"]
    assert spec.stop_file.exists()


def test_a_stopped_chain_cancels_its_next_link_and_runs_no_more(project):
    spec = project.spec(links=3, steps=200, delay=0.02)
    proc = project.link(spec, 1, wait=False)
    wait_for(lambda: (project.rundir / "state.json").exists(), what="workload progress")
    spec.stop_file.touch()
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=60)
    assert proc.returncode == 0, out
    assert "the chain was stopped" in out
    assert project.calls("scancel") == ["scancel 1001"]
    rc, out = project.link(spec, 2)
    assert rc == 0 and "not running" in out


# --- the venv copy ------------------------------------------------------------------


def test_local_venv_copy_and_sweep(tmp_path):
    project = Project(tmp_path, local_venv=True)
    spec = project.spec()  # packs bin/venv.tar
    assert not venv_stale(project.root)
    user = getpass.getuser()
    gone, running = project.local / f"{user}-gutsrun-999", project.local / f"{user}-gutsrun-1234"
    gone.mkdir()
    running.mkdir()
    rc, out = project.link(spec, 1, FAKE_SQUEUE="1234 RUNNING 0:01 node1")
    assert rc == 0, out
    assert f"removed stale {gone}" in out
    assert sorted(project.local.iterdir()) == [running]  # its own copy removed too
    assert (project.rundir / "python.txt").read_text().startswith(str(project.local))


def test_no_squeue_answer_removes_nothing(tmp_path):
    project = Project(tmp_path, local_venv=True)
    spec = project.spec()
    other = project.local / f"{getpass.getuser()}-gutsrun-999"
    other.mkdir()
    rc, out = project.link(spec, 1)  # FAKE_SQUEUE unset: squeue fails
    assert rc == 0, out
    assert other.exists()


def test_stale_tarball_refuses_and_cancels(tmp_path):
    project = Project(tmp_path, local_venv=True)
    spec = project.spec()
    later = time.time() + 10
    os.utime(project.root / ".venv" / "lib" / "python3.12" / "site-packages", (later, later))
    rc, out = project.link(spec, 1)
    assert rc == 1
    assert "older than .venv" in out
    assert project.calls("scancel") == ["scancel 1001"]


# --- HQ inside the job --------------------------------------------------------------


@pytest.mark.hq
def test_hq_server_and_worker_in_the_link(project):
    if shutil.which("hq") is None:
        pytest.skip("no hq binary (run inside `nix develop`)")
    spec = project.spec(needs_hq=True)
    rc, out = project.link(spec, 1)
    assert rc == 0, out
    seen = json.loads((project.rundir / "hq.json").read_text())
    assert "hq-current" in seen["listed"] and seen["hq"]
    assert not list(spec.dir.glob("hq-*"))  # server dir removed
    assert list(spec.dir.glob("link1-*-hq-worker.log"))


# --- the infretis workload and gutsrun-infretis --------------------------------------


def test_infretis_progress_and_argv(tmp_path):
    run = InfretisRun(cores_per_task=2)
    assert run.needs_hq
    assert not run.progress(tmp_path).done  # not started
    assert run.argv("py", tmp_path, 1) == [
        "py", "-m", "gutsrun_infretis.launcher", "-i", "infretis.toml", "--backend", "hq",
        "--cores-per-task", "2"]
    (tmp_path / "restart.toml").write_text(
        "[current]\ncstep = 40\n[simulation]\nsteps = 100\n")
    progress = run.progress(tmp_path)
    assert not progress.done and progress.text == "step 40 of 100"
    assert run.argv("py", tmp_path, 1)[4] == "restart.toml"
    (tmp_path / "restart.toml").write_text("[current]\ncstep = ")
    with pytest.raises(StateError, match="does not parse"):
        run.progress(tmp_path)


def test_submit_reads_gutsrun_toml_and_the_command_line(project, monkeypatch, capsys):
    (project.fakebin / "hq").write_text(FAKE_HQ)
    (project.fakebin / "hq").chmod(0o755)
    project.use_fakes(monkeypatch)
    rundir = project.rundir
    with pytest.raises(ChainError, match="no infretis.toml"):
        cli.make_spec(rundir, {}, root=project.root)
    (rundir / "infretis.toml").write_text("")
    with pytest.raises(ChainError, match="no site"):
        cli.make_spec(rundir, {}, root=project.root)
    (rundir / "gutsrun.toml").write_text('site = "test"\nlinks = 7\nwalltime = "1"\n')
    with pytest.raises(ChainError, match="unknown key 'walltime'"):
        cli.make_spec(rundir, {}, root=project.root)
    (rundir / "gutsrun.toml").write_text('site = "test"\nlinks = 7\ncores_per_task = 2\n')
    spec = cli.make_spec(rundir, {"links": 5, "name": None}, root=project.root)
    assert (spec.links, spec.name, spec.time) == (5, "run", "0:14:00")  # just under 15 min
    assert spec.args == {"cores_per_task": 2}
    assert spec.workload == "gutsrun_infretis.chain:InfretisRun"

    lines = cli.describe(spec, night=False)
    assert lines[0] == "checks passed: run on test, not started (no restart.toml)"
    assert lines[-1].startswith("would submit link 1 of 5: sbatch --parsable")
    assert not spec.dir.exists() and project.calls("sbatch") == []  # nothing written


def test_status_exit_code_says_whether_the_chain_is_active(project, monkeypatch, capsys):
    spec = project.make_spec()
    project.use_fakes(monkeypatch, squeue="1001 PENDING 0:00 (None)")
    start(spec)
    with pytest.raises(SystemExit) as stopped:
        cli.main(["status", str(project.rundir)])
    assert stopped.value.code == cli.EXIT_ACTIVE
    project.use_fakes(monkeypatch, squeue="")
    with pytest.raises(SystemExit) as stopped:
        cli.main(["status", str(project.rundir)])
    assert stopped.value.code == 0
    assert "cancelled before it started" in capsys.readouterr().out


def test_bench_workload_imports_from_bench(project):
    spec = Spec(rundir=str(project.rundir), site="test", name="b", workload="chain:BenchRun",
                pythonpath=[str(REPO / "bench")],
                args={"example": "ex", "runner": "bare"}, root=str(project.root))
    run = spec.make_workload()
    assert not run.needs_hq
    assert run.argv("py", project.rundir, 2) == [
        "py", str(REPO / "bench" / "bare_run.py"), "ex", str(project.rundir), "--in-place",
        "--runner", "bare"]
