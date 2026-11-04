"""gutsrun.chain (chained Slurm jobs) without Slurm: fake sbatch, scancel and
squeue record their calls, and a counting workload (chain_workload.py)
stands in for infretis. Links run as real processes, as in a job."""

import dataclasses
import functools
import json
import os
import shutil
import signal
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
from conftest import wait_for

import gutsrun.chain as chain
import gutsrun_infretis.chain as cli
from gutsrun.chain import (
    STOPPED_BY_SIGNAL,
    ChainError,
    Report,
    Spec,
    StateError,
    events,
    follow,
    load_spec,
    note,
    revision,
    sbatch_argv,
    seconds,
    start,
    status,
    stop,
    walltime,
    why_over,
)
from gutsrun.site import Site, SiteError, setup_env
from gutsrun_infretis.chain import InfretisRun

REPO = Path(__file__).resolve().parent.parent

SITE = """\
[slurm]
partition = "testq"
cores_per_node = 2
max_walltime = "00:15:00"
"""
# Exports a variable and writes to stdout, as `module load` may.
SETUP = """
[environment]
setup = '''
export GUTSRUN_TEST_SETUP="loaded $SLURM_JOB_ID"
echo "setup output"
'''
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
# Runs the step's command here, once, as if on one other node. Like srun, it
# does not pass SIGTERM on: it kills the step.
FAKE_SRUN = """\
#!/usr/bin/env bash
echo "srun $*" >> "$FAKE_LOG"
while [ "${1#-}" != "$1" ]; do shift; done
trap 'echo "srun: forcing job termination"; kill -KILL "$child"' TERM
"$@" &
child=$!
while kill -0 "$child" 2>/dev/null; do wait "$child"; rc=$?; done
exit "$rc"
"""
FAKE_HQ = '#!/usr/bin/env bash\necho "hyperqueue v0.26.2"\n'


class Project:
    """A gutsrun checkout (sites/, .venv, fake Slurm commands) and a run directory."""

    def __init__(self, root: Path):
        self.root = root
        (root / "sites").mkdir()
        (root / "sites" / "test.toml").write_text(SITE)
        (root / ".venv" / "bin").mkdir(parents=True)
        (root / ".venv" / "bin" / "python").symlink_to(sys.executable)
        self.fakebin = root / "fakebin"
        self.fakebin.mkdir()
        for name, text in [("sbatch", FAKE_SBATCH), ("scancel", FAKE_SCANCEL),
                           ("squeue", FAKE_SQUEUE), ("srun", FAKE_SRUN)]:
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

    def make_spec(self, links: int = 2, nodes: int = 1, **counter) -> Spec:
        return Spec(rundir=str(self.rundir), site="test", name="r",
                    workload="chain_workload:Counter", pythonpath=[str(REPO / "tests")],
                    args=counter, time="00:05:00", links=links, nodes=nodes,
                    root=str(self.root))

    def spec(self, links: int = 2, nodes: int = 1, **counter) -> Spec:
        spec = self.make_spec(links, nodes, **counter)
        start(spec, submit_first=False)
        return spec

    def start_link(self, spec: Spec, n: int, **env: str) -> subprocess.Popen[str]:
        """Start link `n` as a process, as Slurm would."""
        argv = [sys.executable, "-m", "gutsrun.chain", "link", str(spec.path), str(n)]
        return subprocess.Popen(argv, cwd=self.root, env={**self.env, **env},
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    def link(self, spec: Spec, n: int, **env: str) -> tuple[int, str]:
        """Run link `n` to its end; its exit code and output."""
        proc = self.start_link(spec, n, **env)
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
    with pytest.raises(SiteError, match="unknown in \\[slurm\\]: typo"):
        Site(project.root, "test")
    with pytest.raises(SiteError, match="known: test"):
        Site(project.root, "nosuch")


def test_setup_env_keeps_values_with_newlines_and_equals_signs():
    env = setup_env('export A="x=1\ny"; unset B', {"PATH": os.environ["PATH"], "B": "1"})
    assert env["A"] == "x=1\ny" and "B" not in env


def test_site_setup_sets_the_workloads_environment(project):
    path = project.root / "sites" / "test.toml"
    path.write_text(path.read_text() + SETUP)
    spec = project.spec()
    rc, out = project.link(spec, 1, SLURM_JOB_ID="77")
    assert rc == 0, out
    assert "setup output" in out  # in the link's log
    assert (project.rundir / "setup.txt").read_text() == "loaded 77"


def test_a_failing_site_setup_fails_the_link(project):
    path = project.root / "sites" / "test.toml"
    path.write_text(path.read_text() + '[environment]\nsetup = "false"\n')
    spec = project.spec()
    rc, out = project.link(spec, 1)
    assert rc == 1
    assert "the site's setup failed (exit 1)" in out
    assert project.calls("scancel") == ["scancel 1001"]


# --- starting -----------------------------------------------------------------------


def test_start_submits_link_1_with_site_options(project, monkeypatch):
    project.use_fakes(monkeypatch)
    spec = project.make_spec()
    assert start(spec) == "1001"
    [call] = project.calls("sbatch")
    for part in ["--partition=testq", "--exclusive", "--nodes=1", "--time=00:05:00",
                 "--signal=B:TERM@90", f"--chdir={project.rundir}",
                 f"--output={project.rundir}/.gutsrun/link1-%j.out",
                 f"exec {project.root}/.venv/bin/python -m gutsrun.chain link",
                 f"{spec.path} 1"]:
        assert part in call
    assert "--dependency" not in call
    assert load_spec(project.rundir) == spec
    assert "--nodes=2" in sbatch_argv(project.make_spec(nodes=2), Site(project.root, "test"), 1)
    assert [e["event"] for e in events(spec)] == ["chain", "submitted"]


def test_start_refusals(project):
    def spec(**changes) -> Spec:
        return dataclasses.replace(project.make_spec(), **changes)

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
    with pytest.raises(ChainError, match="nodes = 0; need at least 1"):
        start(spec(nodes=0), submit_first=False)
    with pytest.raises(ChainError, match="does not run its tasks through HQ"):
        start(spec(nodes=2), submit_first=False)
    (project.rundir / "state.json").unlink()
    start(spec(time="00:15:00"), submit_first=False)  # the daytime limit is not enforced


def test_sbatch_missing_or_refusing(project, monkeypatch):
    spec = project.make_spec()
    monkeypatch.setenv("PATH", "/nonexistent")
    with pytest.raises(ChainError, match="no sbatch on PATH"):
        start(spec)
    assert not spec.dir.exists()  # nothing written
    project.use_fakes(monkeypatch, squeue="")
    (project.fakebin / "sbatch").write_text(
        "#!/usr/bin/env bash\necho 'sbatch: error: invalid partition specified' >&2\nexit 1\n")
    with pytest.raises(ChainError, match="sbatch refused link 1, nothing submitted: .*partition"):
        start(spec)
    assert [e["event"] for e in events(spec)] == ["chain", "not submitted"]
    assert status(spec).over == "no link was submitted"


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


def test_done_cancels_the_pre_submitted_link(project, monkeypatch):
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
    project.use_fakes(monkeypatch, squeue="")
    report = status(spec)
    assert report.links[1].endswith("job 1001       cancelled: the run was done")
    assert report.over == "the run is done"


def test_sigterm_stops_and_the_next_link_continues(project):
    spec = project.spec(steps=200, delay=0.02)
    proc = project.start_link(spec, 1)
    wait_for(lambda: (project.rundir / "state.json").exists() and step(spec) >= 3,
             what="workload progress")
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=60)
    assert proc.returncode == 0, out
    assert "link 2 continues" in out
    assert f"{STOPPED_BY_SIGNAL} after 0:00:" in out  # when the stop came
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
    assert "workload failed (exit 1); run: step 2 of 5" in out
    assert project.calls("scancel") == ["scancel 1001"]


def test_a_crashing_link_ends_in_the_record_and_cancels_the_next(project):
    spec = project.spec(crash=True)
    rc, out = project.link(spec, 1)
    assert rc == 1
    assert "RuntimeError: boom" in out  # the traceback is in the link's log
    assert project.calls("scancel") == ["scancel 1001"]
    [ended] = [e for e in events(spec) if e["event"] == "ended"]
    assert ended["result"] == "crashed: RuntimeError: boom"


def test_revision_without_git(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path))
    assert revision(tmp_path) == "unknown"


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
    proc = project.start_link(spec, 1)
    wait_for(lambda: (project.rundir / "state.json").exists(), what="workload progress")
    proc.send_signal(signal.SIGTERM)
    proc.communicate(timeout=60)

    project.use_fakes(monkeypatch, squeue="1001 PENDING 0:00 (Dependency)")
    report = status(spec)
    assert report.active and report.over is None
    assert report.progress.startswith("r (test): step ")
    assert "ended" in report.links[0] and STOPPED_BY_SIGNAL in report.links[0]
    assert report.links[1].endswith("PENDING 0:00 (Dependency)")
    assert "job 1001" in report.links[1]

    project.use_fakes(monkeypatch, squeue="")
    report = status(spec)
    assert not report.active and report.links[1].endswith("not in the queue, no end recorded")
    assert report.over == (
        f"link 2 recorded no end (cancelled or killed); see {spec.dir}/link2-1001.out")
    follow(spec, interval=0)  # returns: nothing left in the queue
    assert "chain over: link 2" in capsys.readouterr().out

    project.use_fakes(monkeypatch)
    report = status(spec)
    assert report.active and "(squeue does not answer)" in report.links


def test_why_a_chain_is_over(project):
    spec = project.make_spec(links=2)
    project.rundir.joinpath(".gutsrun").mkdir()

    def submitted(link: int) -> dict:
        return {"event": "submitted", "link": link, "job": str(link)}

    def started(link: int) -> dict:
        return {"event": "started", "link": link, "job": str(link), "host": "node1"}

    def ended(link: int, result: str) -> dict:
        return {"event": "ended", "link": link, "job": str(link), "result": result}

    walltime = [submitted(1), started(1), submitted(2), ended(1, STOPPED_BY_SIGNAL)]
    assert why_over(spec, [{"event": "chain"}]) == "no link was submitted"
    assert why_over(spec, [submitted(1), started(1), ended(1, "done")]) == "the run is done"
    assert why_over(spec, [*walltime, started(2), ended(2, STOPPED_BY_SIGNAL)]) == (
        "link 2 stopped at the walltime, the last link; `submit` again to continue")
    assert why_over(spec, [*walltime, started(2), ended(2, "failed (exit 1)")]) == (
        f"link 2: failed (exit 1); see {spec.dir}/link2-2.out")
    # The last link, not the last "ended" event, says why.
    for record in ([*walltime, started(2)], walltime):
        assert why_over(spec, record) == (
            f"link 2 recorded no end (cancelled or killed); see {spec.dir}/link2-2.out")
    spec.stop_file.touch()
    assert why_over(spec, walltime).startswith("stopped by request")
    # Done wins over a stop that came after the end.
    assert why_over(spec, [*walltime, started(2), ended(2, "done")]) == "the run is done"


def test_follow_prints_each_change_until_the_chain_is_over(project, monkeypatch):
    spec = project.make_spec()
    reports = iter([Report("step 1", [], None, True), Report("step 1", [], None, True),
                    Report("step 2", [], None, True), Report("step 2", [], "done", False)])
    printed = []
    monkeypatch.setattr(chain, "status", lambda _spec: next(reports))
    monkeypatch.setattr(chain.time, "sleep", lambda _s: None)
    monkeypatch.setattr("builtins.print", lambda *a, **_: printed.append(a[1]))
    follow(spec)
    assert printed == ["step 1", "step 2", "step 2"]  # the repeat is not printed


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
    proc = project.start_link(spec, 1)
    wait_for(lambda: (project.rundir / "state.json").exists(), what="workload progress")
    spec.stop_file.touch()
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=60)
    assert proc.returncode == 0, out
    assert "stopped by request; run: step" in out
    assert project.calls("scancel") == ["scancel 1001"]
    rc, out = project.link(spec, 2)
    assert rc == 0 and "not running" in out


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


@pytest.mark.hq
def test_hq_workers_on_the_other_nodes(tmp_path):
    if shutil.which("hq") is None:
        pytest.skip("no hq binary (run inside `nix develop`)")
    project = Project(tmp_path)
    spec = project.spec(nodes=2, needs_hq=True, workers=2)
    rc, out = project.link(spec, 1, SLURM_JOB_ID="4242")
    assert rc == 0, out
    assert json.loads((project.rundir / "hq.json").read_text())["workers"] == 2
    [srun] = project.calls("srun")
    for part in ["--nodes=1", "--ntasks=1", "--ntasks-per-node=1", "--relative=1",
                 "--cpus-per-task=2", f"--output={spec.dir}/link1-4242-hq-worker-%N.log",
                 "worker start --server-dir", "--cpus 2"]:
        assert part in srun
    assert "hq workers on the other nodes: srun exit" in out  # the step ended


# --- the infretis workload and gutsrun-infretis --------------------------------------


def test_infretis_progress_and_argv(tmp_path):
    run = InfretisRun(cores_per_task=2)
    assert run.needs_hq
    assert not run.progress(tmp_path).done  # not started
    assert run.argv("py", tmp_path, 1) == [
        "py", "-m", "gutsrun_infretis.launcher", "-i", "infretis.toml",
        "--cores-per-task", "2"]
    (tmp_path / "restart.toml").write_text(
        "[current]\ncstep = 40\n[simulation]\nsteps = 100\n")
    progress = run.progress(tmp_path)
    assert not progress.done and progress.text == "step 40 of 100"
    assert run.argv("py", tmp_path, 1)[4] == "restart.toml"
    assert InfretisRun(time_request="30m").argv("py", tmp_path, 1)[-2:] == [
        "--time-request", "30m"]
    assert InfretisRun(gpus_per_task=1).argv("py", tmp_path, 1)[-2:] == [
        "--gpus-per-task", "1"]
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
    (rundir / "gutsrun.toml").write_text(
        'site = "test"\nlinks = 7\ncores_per_task = 2\nnodes = 3\ntime_request = "30m"\n'
        'gpus_per_task = 1\n')
    with pytest.raises(ChainError, match="cores_per_task is 3, but a node of test has 2 cores"):
        cli.make_spec(rundir, {"cores_per_task": 3}, root=project.root)
    spec, source = cli.make_spec(rundir, {"links": 5, "name": None}, root=project.root)
    assert (spec.links, spec.name, spec.time) == (5, "run", "0:14:00")  # just under 15 min
    assert source == "just under max_walltime 00:15:00 in sites/test.toml"
    assert spec.nodes == 3
    assert spec.args == {"cores_per_task": 2, "gpus_per_task": 1, "time_request": "30m"}
    assert spec.workload == "gutsrun_infretis.chain:InfretisRun"

    lines = cli.describe(spec)
    assert lines[0] == "checks passed: run on test, not started (no restart.toml)"
    assert lines[-1].startswith("would submit link 1 of 5: sbatch --parsable")
    assert not spec.dir.exists() and project.calls("sbatch") == []  # nothing written

    (rundir / "gutsrun.toml").write_text('site = "test"\ntime = "00:20:00"\n')
    assert cli.make_spec(rundir, {}, root=project.root)[1] == "from gutsrun.toml"
    spec, source = cli.make_spec(rundir, {"time": "00:30:00"}, root=project.root)
    assert (spec.time, source) == ("00:30:00", "from --time")
    monkeypatch.setattr(cli, "make_spec", functools.partial(cli.make_spec, root=project.root))
    cli.main(["submit", "--check", str(rundir)])
    assert capsys.readouterr().out.startswith(
        "walltime per link: 00:20:00 (from gutsrun.toml)\n")


def test_status_exit_code_says_whether_the_chain_is_active(project, monkeypatch, capsys):
    spec = project.make_spec()
    project.use_fakes(monkeypatch, squeue="1001 PENDING 0:00 (None)")
    start(spec)
    with pytest.raises(SystemExit) as stopped:
        cli.main(["status", str(project.rundir)])
    assert stopped.value.code == cli.EXIT_ACTIVE
    capsys.readouterr()
    project.use_fakes(monkeypatch, squeue="")
    with pytest.raises(SystemExit) as stopped:
        cli.main(["status", str(project.rundir)])
    assert stopped.value.code == 0
    text = capsys.readouterr().out
    assert "not in the queue, no end recorded" in text
    with pytest.raises(SystemExit) as stopped:
        cli.main(["status", "--json", str(project.rundir)])
    assert stopped.value.code == 0
    report = json.loads(capsys.readouterr().out)
    assert report["lines"] == text.splitlines()
    assert report["over"].startswith("link 1 recorded no end") and report["active"] is False


def test_bench_workload_imports_from_bench(project):
    spec = Spec(rundir=str(project.rundir), site="test", name="b", workload="chain:BenchRun",
                pythonpath=[str(REPO / "bench")],
                args={"example": "ex", "runner": "bare"}, root=str(project.root))
    run = spec.make_workload()
    assert not run.needs_hq
    run.before_link(project.rundir, 2)  # no restart.toml yet: nothing to back up
    assert not list(project.rundir.glob("restart.toml*"))
    assert run.argv("py", project.rundir, 2) == [
        "py", str(REPO / "bench" / "bare_run.py"), "ex", str(project.rundir), "--in-place",
        "--runner", "bare"]
