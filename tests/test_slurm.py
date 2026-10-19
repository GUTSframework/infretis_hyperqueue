"""gutsrun.slurm (chained Slurm jobs) without Slurm: fake sbatch, scancel and
squeue record their calls, and a counting workload (chain_workload.py)
stands in for infretis. Links run as real processes, as in a job."""

import getpass
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from gutsrun.slurm import ChainError, Spec, seconds, start, venv_stale
from gutsrun_infretis.chain import InfretisRun
from conftest import wait_for

REPO = Path(__file__).resolve().parent.parent

SITE = """\
[slurm]
partition = "testq"
max_walltime_day = "00:15:00"
cores_per_node = 2
exclusive = true

[python]
interpreter = "{python}"

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
# Lists the job ids in $FAKE_SQUEUE; fails like an unreachable slurmctld without it.
FAKE_SQUEUE = '#!/usr/bin/env bash\n[ -n "${FAKE_SQUEUE:-}" ] && echo "$FAKE_SQUEUE"\n'


class Project:
    def __init__(self, root: Path, local_venv=False):
        self.root = root
        self.local = root / "local"
        self.local.mkdir()
        (root / "sites").mkdir()
        (root / "sites" / "test.toml").write_text(SITE.format(
            python=sys.executable, local_venv=str(local_venv).lower(), local_disk=self.local))
        (root / ".venv" / "bin").mkdir(parents=True)
        (root / ".venv" / "lib" / "python3.12" / "site-packages").mkdir(parents=True)
        (root / ".venv" / "bin" / "python").symlink_to(sys.executable)
        fakebin = root / "fakebin"
        fakebin.mkdir()
        for name, text in [("sbatch", FAKE_SBATCH), ("scancel", FAKE_SCANCEL),
                           ("squeue", FAKE_SQUEUE)]:
            (fakebin / name).write_text(text)
            (fakebin / name).chmod(0o755)
        self.log = root / "fake.log"
        self.log.touch()
        self.env = {**os.environ, "FAKE_LOG": str(self.log),
                    "PATH": os.pathsep.join([str(fakebin), os.environ["PATH"]]),
                    "PYTHONPATH": os.pathsep.join([str(REPO / "src"), str(REPO / "tests")])}

    def spec(self, name="r", links=2, **counter):
        spec = Spec(root=str(self.root), site="test", name=name,
                    workload="chain_workload:Counter", args=counter,
                    time="00:05:00", links=links)
        start(spec, submit_first=False)
        return spec

    def link(self, spec, n, wait=True, **env):
        argv = [sys.executable, "-m", "gutsrun.slurm", "link", str(spec.path), str(n)]
        proc = subprocess.Popen(argv, cwd=self.root, env={**self.env, **env},
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if not wait:
            return proc
        out, _ = proc.communicate(timeout=120)
        return proc.returncode, out

    def calls(self, cmd):
        return [line for line in self.log.read_text().splitlines() if line.startswith(cmd)]


def step(spec):
    return json.loads((spec.rundir / "state.json").read_text())["step"]


@pytest.fixture
def project(tmp_path):
    return Project(tmp_path)


@pytest.mark.parametrize("walltime, expected", [
    ("15", 900), ("15:30", 930), ("00:14:00", 840), ("48:00:00", 172800),
    ("2-0", 172800), ("1-2:30", 95400), ("1-02:03:04", 93784)])
def test_seconds_reads_slurm_walltimes(walltime, expected):
    assert seconds(walltime) == expected


# --- starting -----------------------------------------------------------------------


def test_start_submits_link_1_with_site_options(project, monkeypatch):
    spec = Spec(root=str(project.root), site="test", name="r",
                workload="chain_workload:Counter", time="00:05:00")
    monkeypatch.setenv("PATH", project.env["PATH"])
    monkeypatch.setenv("FAKE_LOG", str(project.log))
    assert start(spec) == "1001"
    [call] = project.calls("sbatch")
    for part in ["--partition=testq", "--exclusive", "--nodes=1", "--time=00:05:00",
                 "--signal=B:TERM@90", f"--chdir={project.root}", "-m gutsrun.slurm link",
                 f"{spec.path} 1", f"{sys.executable}"]:
        assert part in call
    assert "--dependency" not in call
    assert Spec.load(spec.path) == spec


def test_start_refusals(project):
    def spec(**kw):
        return Spec(**{"root": str(project.root), "site": "test", "name": "r",
                       "workload": "chain_workload:Counter", **kw})
    with pytest.raises(ChainError, match="daytime limit"):
        start(spec(time="00:15:00"), submit_first=False)
    start(spec(time="00:15:00"), night=True, submit_first=False)
    with pytest.raises(ChainError, match="exists"):
        start(spec(), submit_first=False)
    with pytest.raises(ChainError, match="no time to run"):
        start(spec(name="s", time="00:02:00"), submit_first=False)


# --- links --------------------------------------------------------------------------


def test_done_cancels_the_pre_submitted_link(project):
    spec = project.spec(links=3)
    rc, out = project.link(spec, 1)
    assert rc == 0, out
    assert step(spec) == 5
    [sbatch] = project.calls("sbatch")
    assert "--dependency=afterany:nojob-" in sbatch and f"{spec.path} 2" in sbatch
    assert project.calls("scancel") == ["scancel 1001"]


def test_sigterm_stops_and_the_next_link_continues(project):
    spec = project.spec(steps=200, delay=0.02)
    proc = project.link(spec, 1, wait=False)
    wait_for(lambda: (spec.rundir / "state.json").exists() and step(spec) >= 3,
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
    assert (spec.rundir / "before2").exists()
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


# --- the venv copy ------------------------------------------------------------------


def test_local_venv_copy_and_sweep(tmp_path):
    project = Project(tmp_path, local_venv=True)
    spec = project.spec()  # packs bin/venv.tar
    assert not venv_stale(project.root)
    user = getpass.getuser()
    gone, running = project.local / f"{user}-gutsrun-999", project.local / f"{user}-gutsrun-1234"
    gone.mkdir()
    running.mkdir()
    rc, out = project.link(spec, 1, FAKE_SQUEUE="1234")
    assert rc == 0, out
    assert f"removed stale {gone}" in out
    assert sorted(project.local.iterdir()) == [running]  # its own copy removed too
    assert (spec.rundir / "python.txt").read_text().startswith(str(project.local))


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
    import shutil
    if shutil.which("hq") is None:
        pytest.skip("no hq binary (run inside `nix develop`)")
    spec = project.spec(needs_hq=True)
    rc, out = project.link(spec, 1)
    assert rc == 0, out
    seen = json.loads((spec.rundir / "hq.json").read_text())
    assert "hq-current" in seen["listed"] and seen["hq"]
    assert not list(spec.runs.glob("r-hq-*"))  # server dir removed
    assert list(spec.runs.glob("r-link1-*-hq-worker.log"))


# --- the infretis workload ----------------------------------------------------------


def test_infretis_progress_and_argv(tmp_path):
    run = InfretisRun("examples/2d_potential", "gutsrun", workers=4, steps=100)
    (tmp_path / "restart.toml").write_text(
        "[current]\ncstep = 40\n[simulation]\nsteps = 100\n")
    progress = run.progress(tmp_path)
    assert not progress.done and progress.text == "step 40 of 100"
    assert run.needs_hq
    assert run.argv("py", tmp_path, 2) == [
        "py", "bench/bare_run.py", "examples/2d_potential", str(tmp_path), "--workers", "4",
        "--runner", "gutsrun", "--steps", "100", "--backend", "hq", "--restart"]
    assert "--restart" not in run.argv("py", tmp_path, 1)
    (tmp_path / "restart.toml").write_text("[current]\ncstep = ")
    with pytest.raises(Exception, match="does not parse"):
        run.progress(tmp_path)
