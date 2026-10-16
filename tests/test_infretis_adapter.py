"""The infretis adapter and launcher, on real infretis runs.

Needs infretis importable (inside `nix develop`: numpy/scipy need its
libraries). The `slow` exact check runs LAMMPS (~3 min); run it with
`pytest -m slow`.
"""

import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("infretis.setup", exc_type=ImportError)

from gutsrun.backends.local import LocalBackend  # noqa: E402
from gutsrun_infretis.launcher import (  # noqa: E402
    EXIT_DONE, EXIT_STOPPED, InfretisExecutor, strip_pinning)

ROOT = Path(__file__).resolve().parent.parent
BARE_RUN = ROOT / "bench" / "bare_run.py"


def run(example, rundir, *args, check=True):
    proc = subprocess.run(
        [sys.executable, str(BARE_RUN), str(ROOT / "examples" / example), str(rundir), *args],
        capture_output=True, text=True)
    if check:
        assert proc.returncode == 0, proc.stderr[-2000:]
    return json.loads((rundir / "run.json").read_text())[-1]


def path_numbers(rundir):
    rows = [line.split() for line in (rundir / "infretis_data.txt").read_text().splitlines()
            if line.strip() and not line.lstrip().startswith("#")]
    return [int(r[0]) for r in rows]


def test_strip_pinning():
    cmds = ["gmx mdrun -ntomp 2 -ntmpi 1 -pin on -pinoffset 4 -pinstride 1", 1]
    assert strip_pinning(cmds, 2) == ["gmx mdrun -ntomp 2 -ntmpi 1", 1]
    with pytest.raises(SystemExit, match="-ntomp 2"):
        strip_pinning(cmds, 4)


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


@pytest.mark.slow
def test_exact_lammps_workers_1(tmp_path):
    """With one worker the run is sequential and every random number comes
    from the seeded state in md_items, so bare infretis and gutsrun must
    give byte-identical output (review S7). Not 2d_potential: its ASE
    engine draws velocities from NumPy's unseeded global RNG."""
    common = ["--workers", "1", "--steps", "8"]
    run("water_dissociation_lammps", tmp_path / "bare", *common)
    run("water_dissociation_lammps", tmp_path / "guts", *common, "--runner", "gutsrun")
    for name in ("infretis_data.txt", "restart.toml"):
        assert (tmp_path / "bare" / name).read_bytes() == (tmp_path / "guts" / name).read_bytes(), name
