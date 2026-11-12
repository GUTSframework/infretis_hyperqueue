"""The infretis workload of a chain (see `gutsrun.chain.Workload`): what
each link runs, and how far the run got. `gutsrun_infretis.cli` creates
chains with it.

Standard library only: every link imports this module to read the run's
state.
"""

import tomllib
from dataclasses import dataclass
from pathlib import Path

from gutsrun.chain import RunProgress, StateError


@dataclass
class InfretisRun:
    """The infretis workload: the gutsrun launcher with HQ. Its fields are
    the defaults of the settings of the same name (see `Config`)."""

    cores_per_task: int = 1
    gpus_per_task: int = 0
    time_request: str | None = None

    @property
    def needs_hq(self) -> bool:
        return True

    def progress(self, rundir: Path) -> RunProgress:
        path = rundir / "restart.toml"
        if not path.exists():
            return RunProgress(False, "not started (no restart.toml)")
        try:
            config = tomllib.loads(path.read_text())
            cstep, steps = config["current"]["cstep"], config["simulation"]["steps"]
        except (OSError, tomllib.TOMLDecodeError, KeyError) as e:
            raise StateError(f"restart.toml in {rundir} does not parse ({e})") from e
        return RunProgress(cstep >= steps, f"step {cstep} of {steps}")

    def before_link(self, rundir: Path, link: int) -> None:
        """Hook run at the start of each link after the first; bench/chain.py
        uses it to keep a copy of restart.toml per link. Nothing to do here:
        restart.toml is all a link needs."""

    def argv(self, python: str, rundir: Path) -> list[str]:
        """The launcher, from restart.toml once there is one, else from
        infretis.toml."""
        inp = "restart.toml" if (rundir / "restart.toml").exists() else "infretis.toml"
        argv = [python, "-m", "gutsrun_infretis.launcher", "-i", inp,
                "--cores-per-task", str(self.cores_per_task)]
        if self.gpus_per_task:
            argv += ["--gpus-per-task", str(self.gpus_per_task)]
        if self.time_request:
            argv += ["--time-request", self.time_request]
        return argv
