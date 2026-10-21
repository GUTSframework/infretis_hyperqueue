"""The infretis adapter and launcher, on real infretis runs.

Needs infretis importable (inside `nix develop`: numpy/scipy need its
libraries). The `slow` exact check runs LAMMPS (~3 min); run it with
`pytest -m slow`.
"""

import ast
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytest.importorskip("infretis.setup", exc_type=ImportError)

from conftest import wait_for  # noqa: E402

from gutsrun.backends.local import LocalBackend  # noqa: E402
from gutsrun_infretis.launcher import (  # noqa: E402
    EXIT_DONE,
    EXIT_REFUSED,
    EXIT_STOPPED,
    HOLD_ENV,
    InfretisExecutor,
    changed_sources,
    strip_pinning,
)

ROOT = Path(__file__).resolve().parent.parent
BARE_RUN = ROOT / "bench" / "bare_run.py"


def run(example, rundir, *args, check=True):
    proc = subprocess.run(
        [sys.executable, str(BARE_RUN), str(ROOT / "examples" / example), str(rundir), *args],
        capture_output=True, text=True)
    if check:
        assert proc.returncode == 0, proc.stderr[-2000:]
    return json.loads((rundir / "run.json").read_text())[-1]


def prepare(example, rundir, workers, steps):
    """Copy an example into a run dir, as bench/bare_run.py does."""
    import tomli
    import tomli_w

    shutil.copytree(ROOT / "examples" / example, rundir)
    (rundir / "load_copy").rename(rundir / "load")
    cfg = tomli.loads((rundir / "infretis.toml").read_text())
    cfg["runner"]["workers"] = workers
    cfg["runner"]["wmdrun"] = [cfg["runner"]["wmdrun"][0]] * workers
    cfg["simulation"]["steps"] = steps
    (rundir / "infretis.toml").write_text(tomli_w.dumps(cfg))


def launch(rundir, *args, env=None):
    return subprocess.Popen(
        [sys.executable, "-m", "gutsrun_infretis.launcher", *args], cwd=rundir,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src"), **(env or {})},
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def path_numbers(rundir):
    rows = [line.split() for line in (rundir / "infretis_data.txt").read_text().splitlines()
            if line.strip() and not line.lstrip().startswith("#")]
    return [int(r[0]) for r in rows]


def test_strip_pinning():
    cmds = ["gmx mdrun -ntomp 2 -ntmpi 1 -pin on -pinoffset 4 -pinstride 1", 1]
    assert strip_pinning(cmds, 2) == ["gmx mdrun -ntomp 2 -ntmpi 1", 1]
    with pytest.raises(SystemExit, match="-ntomp 2"):
        strip_pinning(cmds, 4)


def test_known_infretis_sources():
    """The installed infretis has the functions the launcher was written for."""
    assert changed_sources() == []


def test_changed_infretis_source_refused(monkeypatch):
    from gutsrun_infretis import launcher

    monkeypatch.setitem(launcher.KNOWN_SOURCES, "REPEX_state.write_toml", "0" * 16)
    with pytest.raises(SystemExit, match="REPEX_state.write_toml"):
        launcher.check_infretis(allow_other=False)
    launcher.check_infretis(allow_other=True)  # warns in the log, runs


def test_executor_takes_only_infretis_calls(tmp_path):
    ex = InfretisExecutor(LocalBackend(1), tmp_path / "spool")
    try:
        with pytest.raises(TypeError):
            ex.submit(print, "x")
    finally:
        ex.shutdown()


def test_2d_potential_run(tmp_path):
    rd = tmp_path / "run"
    rec = run("2d_potential", rd, "--workers", "2", "--steps", "12", "--runner", "gutsrun")
    assert rec["returncode"] == EXIT_DONE
    nums = path_numbers(rd)
    assert len(nums) == len(set(nums)) > 0
    (epoch,) = [d for d in (rd / "gutsrun-spool").iterdir() if d.is_dir()]
    for sub in ("in", "out", "log"):
        assert os.listdir(epoch / sub) == [], sub
    assert {"worker0.log", "worker1.log"} <= {p.name for p in rd.iterdir()}
    assert "infinity" not in (rd / "gutsrun.log").read_text()  # no infretis log echoed


def test_sigterm_then_restart(tmp_path):
    rd = tmp_path / "run"
    rec = run("2d_potential", rd, "--workers", "2", "--steps", "400", "--runner", "gutsrun",
              "--kill-after", "8")
    assert rec["returncode"] == EXIT_STOPPED
    leftover = subprocess.run(["pgrep", "-f", f"gutsrun.task {rd}"], capture_output=True)
    assert leftover.returncode == 1, "task processes survived the stop"

    rec = run("2d_potential", rd, "--restart", "--runner", "gutsrun", "--kill-after", "6")
    assert rec["returncode"] in (EXIT_STOPPED, EXIT_DONE)
    assert list(rd.glob("restart.toml.bak.*")), "restart.toml was not backed up"
    nums = path_numbers(rd)
    assert len(nums) == len(set(nums)), "duplicate path numbers after restart"


def test_sigterm_inside_treat_output(tmp_path):
    """Deterministic version of the stop drill: the signal arrives while
    treat_output runs (held there by a test hook). The launcher must finish
    it, write restart.toml, and stop; a restart then completes cleanly."""
    rd = tmp_path / "run"
    prepare("2d_potential", rd, workers=2, steps=40)
    proc = launch(rd, "-i", "infretis.toml", env={HOLD_ENV: "2"})
    wait_for(lambda: (rd / "treat_output.entered").exists(), timeout=120, what="treat_output")
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=120)
    assert proc.returncode == EXIT_STOPPED, out
    assert "stopping after treat_output" in (rd / "gutsrun.log").read_text()
    import tomli
    cstep = tomli.loads((rd / "restart.toml").read_text())["current"]["cstep"]
    assert cstep >= 1
    # The treated move is saved; the other pin's move is lost and redone.
    (stop,) = [json.loads(line) for line in (rd / "gutsrun-stops.jsonl").open()]
    assert stop["signal"] == "SIGTERM" and len(stop["moves"]) == 1, stop
    assert stop["lost_s"] == stop["longest_s"] == stop["moves"][0]["seconds"] > 0
    assert "1 move(s) not saved, to be redone" in out

    proc = launch(rd, "-i", "restart.toml")
    out, _ = proc.communicate(timeout=600)
    assert proc.returncode == EXIT_DONE, out
    nums = path_numbers(rd)
    assert len(nums) == len(set(nums)), "duplicate path numbers after restart"


@pytest.mark.parametrize("gap", [0.001, 0.03])
def test_second_sigterm_while_stopping(tmp_path, gap):
    """`scancel` signals every process of the job: the launcher gets SIGTERM
    from Slurm and again from the link. It stops once, with EXIT_STOPPED,
    whether the second arrives while it stops (`gap` 0.001) or while
    Python shuts down (0.03)."""
    rd = tmp_path / "run"
    prepare("2d_potential", rd, workers=2, steps=400)
    proc = launch(rd, "-i", "infretis.toml")
    wait_for(lambda: (rd / "restart.toml").exists(), timeout=120, what="restart.toml")
    proc.send_signal(signal.SIGTERM)
    time.sleep(gap)
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=120)
    assert proc.returncode == EXIT_STOPPED, out


def test_stop_before_the_first_move_leaves_restart_toml_alone(tmp_path):
    """A resumed link stopped while it starts up (`stop` right after a link
    starts): infretis writes restart.toml only at the end of treat_output,
    so the file stays as the last link left it, and the next link continues
    from it. Not refused: infretis refuses only when cstep equals
    restarted_from, and a written file always has cstep > restarted_from."""
    import tomli

    rd = tmp_path / "run"
    prepare("2d_potential", rd, workers=2, steps=20)
    proc = launch(rd, "-i", "infretis.toml")
    wait_for(lambda: (rd / "restart.toml").exists(), timeout=120, what="restart.toml")
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=120)
    assert proc.returncode == EXIT_STOPPED, out
    left = (rd / "restart.toml").read_bytes()

    proc = launch(rd, "-i", "restart.toml")
    time.sleep(0.2)  # importing: with or without its handler, no move has run
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=120)
    assert proc.returncode in (EXIT_STOPPED, -signal.SIGTERM), out
    assert (rd / "restart.toml").read_bytes() == left

    proc = launch(rd, "-i", "restart.toml")
    out, _ = proc.communicate(timeout=600)
    assert proc.returncode == EXIT_DONE, out
    current = tomli.loads((rd / "restart.toml").read_text())["current"]
    assert current["restarted_from"] == tomli.loads(left.decode())["current"]["cstep"]
    nums = path_numbers(rd)
    assert len(nums) == len(set(nums)), "duplicate path numbers after restart"


@pytest.mark.hq
def test_launcher_through_hq(hq_cluster, tmp_path):
    rd = tmp_path / "run"
    prepare("2d_potential", rd, workers=2, steps=10)
    proc = launch(rd, "-i", "infretis.toml", "--backend", "hq",
                  "--hq-server-dir", str(hq_cluster.dir))
    out, _ = proc.communicate(timeout=600)
    assert proc.returncode == EXIT_DONE, out
    rows = [json.loads(line) for line in (rd / "gutsrun-timings.jsonl").read_text().splitlines()]
    assert len(rows) >= 10 and all(r["instance"] == "0" for r in rows)
    nums = path_numbers(rd)
    assert len(nums) == len(set(nums)) > 0


@pytest.mark.slow
def test_exact_lammps_workers_1(tmp_path):
    """With one worker the run is sequential and every random number comes
    from the seeded state in md_items, so bare infretis and gutsrun must
    give byte-identical output. Not 2d_potential: its ASE
    engine draws velocities from NumPy's unseeded global RNG."""
    common = ["--workers", "1", "--steps", "8"]
    run("water_dissociation_lammps", tmp_path / "bare", *common)
    run("water_dissociation_lammps", tmp_path / "guts", *common, "--runner", "gutsrun")
    for name in ("infretis_data.txt", "restart.toml"):
        bare, guts = (tmp_path / "bare" / name), (tmp_path / "guts" / name)
        assert bare.read_bytes() == guts.read_bytes(), name


@pytest.mark.parametrize("module, first", [
    ("worker", "infretis.setup"), ("launcher", "gutsrun_infretis.worker")])
def test_infretis_setup_is_imported_before_the_rest_of_infretis(module, first):
    # A runtime check cannot see a wrong order today: infretis.core.tis
    # happens to import importlib.util, which the factory needs.
    source = ROOT / "src" / "gutsrun_infretis" / f"{module}.py"
    names = [node.names[0].name if isinstance(node, ast.Import) else node.module or ""
             for node in ast.parse(source.read_text()).body
             if isinstance(node, ast.Import | ast.ImportFrom)]
    ours = [n for n in names if n.split(".")[0] in ("infretis", "gutsrun_infretis")]
    assert ours[0] == first


def test_relaunched_finished_run_refused_with_a_way_out(tmp_path):
    """Launching a finished run saves restarted_from = cstep; infretis then
    refuses to continue it with more steps. The message says how to undo
    that, and doing so continues the run."""
    import tomli
    import tomli_w

    rd = tmp_path / "run"
    prepare("2d_potential", rd, workers=2, steps=6)
    for inp in ("infretis.toml", "restart.toml"):  # finish, then launch again
        out, _ = launch(rd, "-i", inp).communicate(timeout=600)
    config = tomli.loads((rd / "restart.toml").read_text())
    cstep = config["current"]["cstep"]
    assert config["current"]["restarted_from"] == cstep
    config["simulation"]["steps"] = cstep + 4
    (rd / "restart.toml").write_text(tomli_w.dumps(config))

    proc = launch(rd, "-i", "restart.toml")
    out, _ = proc.communicate(timeout=600)
    assert proc.returncode == EXIT_REFUSED, out
    assert f"delete the line `restarted_from = {cstep}` under [current]" in out

    del config["current"]["restarted_from"]
    (rd / "restart.toml").write_text(tomli_w.dumps(config))
    proc = launch(rd, "-i", "restart.toml")
    out, _ = proc.communicate(timeout=600)
    assert proc.returncode == EXIT_DONE, out
    nums = path_numbers(rd)
    assert len(nums) == len(set(nums)), "duplicate path numbers after restart"


GROMACS_ENGINE = {"gmx": "gmx-must-not-run", "timestep": 0.002, "subcycles": 1000,
                  "temperature": 573.15}


def gromacs_input(tmp_path):
    """water_boiling's inputs: they include `conf.g96`, which the engine reads."""
    inp = tmp_path / "gromacs_input"
    shutil.copytree(ROOT / "examples" / "water_boiling" / "gromacs_input", inp)
    return inp


def parent_writes_mdp(inp, monkeypatch, **settings):
    """`infretis.mdp` as infretis's setup writes it in the parent (minus
    grompp, as `gmx` is not callable here)."""
    from infretis.classes.engines.gromacs import GromacsEngine

    with monkeypatch.context() as m:
        m.setattr(GromacsEngine, "_execute_grompp",
                  lambda self, mdp, conf, deffnm: {"tpr": f"{deffnm}.tpr", "mdout": "mdout.mdp"})
        GromacsEngine(input_path=str(inp), **{**GROMACS_ENGINE, **settings})
    return inp / "infretis.mdp"


def snapshot(directory):
    return {p.name: (p.stat().st_ino, p.stat().st_mtime_ns, p.read_bytes())
            for p in directory.iterdir()}


def test_task_engine_leaves_the_gromacs_inputs_alone(tmp_path, monkeypatch):
    """A task builds its GROMACS engine without grompp and without writing
    anything to the input directory that other moves read."""
    from infretis.classes.engines.gromacs import GromacsEngine

    from gutsrun_infretis.worker import shared_gromacs_inputs_read_only

    inp = gromacs_input(tmp_path)
    assert "ref-t = 573.15" in parent_writes_mdp(inp, monkeypatch).read_text()
    before = snapshot(inp)
    grompp = GromacsEngine._execute_grompp
    with shared_gromacs_inputs_read_only():
        GromacsEngine(input_path=str(inp), **GROMACS_ENGINE)
    assert snapshot(inp) == before
    assert GromacsEngine._execute_grompp is grompp
    assert "_modify_input" not in GromacsEngine.__dict__


@pytest.mark.parametrize("change", ["edited", "other engine", "missing"])
def test_task_engine_stops_on_a_changed_gromacs_mdp(tmp_path, monkeypatch, change):
    from infretis.classes.engines.gromacs import GromacsEngine

    from gutsrun_infretis.worker import SharedInputChanged, shared_gromacs_inputs_read_only

    inp = gromacs_input(tmp_path)
    mdp = parent_writes_mdp(inp, monkeypatch)
    if change == "edited":
        mdp.write_text(mdp.read_text().replace("573.15", "300"))
    elif change == "other engine":  # built last in the parent, so its mdp is on disk
        parent_writes_mdp(inp, monkeypatch, temperature=300)
    else:
        mdp.unlink()
    before = snapshot(inp)
    with pytest.raises(SharedInputChanged, match="infretis.mdp"):
        with shared_gromacs_inputs_read_only():
            GromacsEngine(input_path=str(inp), **GROMACS_ENGINE)
    assert snapshot(inp) == before
    assert "_modify_input" not in GromacsEngine.__dict__
