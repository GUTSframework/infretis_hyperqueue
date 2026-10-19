"""Chained infretis runs on a Slurm cluster: the infretis workload for
`gutsrun.slurm`, and the command that starts a chain. Run it from the
project root on the login node:

    python3.12 -m gutsrun_infretis.chain 2d gutsrun --steps 1000 --time 00:05:00
    python3.12 -m gutsrun_infretis.chain 2d bare --steps 3000 --time 00:04:00

case: 2d (examples/2d_potential) or lammps (examples/water_dissociation_lammps;
needs an `lmp` on the nodes). runner: bare (infretis's own
ProcessPoolExecutor, the baseline row) or gutsrun (the launcher with the HQ
backend). Prints link 1's job id, for scripts/wait-jobs.sh. With
--no-submit it prints the spec's path instead, and a link can be tried
without Slurm: `python -m gutsrun.slurm link <spec> 1`.

Each link runs bench/bare_run.py, which records run.json in the run
directory; later links continue from restart.toml (`--restart`). The run's
state is restart.toml's `cstep` against `steps`.

Standard library only, like gutsrun.slurm: it runs with the site's system
Python, before the venv is unpacked.
"""

import argparse
import shutil
import sys
import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from gutsrun.slurm import ChainError, Progress, Spec, StateError, setup_logging, start

CASES = {"2d": "examples/2d_potential", "lammps": "examples/water_dissociation_lammps"}


@dataclass
class InfretisRun:
    example: str
    runner: str  # bare | gutsrun
    workers: int = 4
    steps: int | None = None  # None: the example's own

    @property
    def needs_hq(self) -> bool:
        return self.runner == "gutsrun"

    def progress(self, rundir: Path) -> Progress:
        try:
            config = tomllib.loads((rundir / "restart.toml").read_text())
            cstep, steps = config["current"]["cstep"], config["simulation"]["steps"]
        except (OSError, tomllib.TOMLDecodeError, KeyError) as e:
            raise StateError(f"restart.toml in {rundir} is missing or does not parse ({e}); "
                             "backups: restart.toml.bak.*, restart.toml.link*") from e
        return Progress(cstep >= steps, f"step {cstep} of {steps}")

    def before_link(self, rundir: Path, link: int):
        # Bare infretis writes restart.toml in place, so a kill can truncate it.
        shutil.copy2(rundir / "restart.toml", rundir / f"restart.toml.link{link}")

    def argv(self, python: str, rundir: Path, link: int) -> list[str]:
        argv = [python, "bench/bare_run.py", self.example, str(rundir),
                "--workers", str(self.workers), "--runner", self.runner]
        if self.steps is not None:
            argv += ["--steps", str(self.steps)]
        if self.runner == "gutsrun":
            argv += ["--backend", "hq"]
        if link > 1:
            argv.append("--restart")
        return argv


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("case", choices=sorted(CASES))
    p.add_argument("runner", choices=["bare", "gutsrun"])
    p.add_argument("--steps", type=int, help="MC steps (default: the example's)")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--time", default="00:14:00", help="walltime per link")
    p.add_argument("--margin", type=int, default=90,
                   help="seconds before the walltime that SIGTERM is asked for")
    p.add_argument("--links", type=int, default=3, help="most links (3: DAS-6 daytime)")
    p.add_argument("--name", help="run name (default: <date>-<case>-<runner>)")
    p.add_argument("--site", default="das6")
    p.add_argument("--night", action="store_true",
                   help="allow a walltime beyond the site's daytime limit")
    p.add_argument("--no-submit", action="store_true",
                   help="write the spec only and print its path")
    args = p.parse_args(argv)
    setup_logging()

    run = InfretisRun(CASES[args.case], args.runner, args.workers, args.steps)
    spec = Spec(root=str(Path.cwd()), site=args.site,
                name=args.name or f"{date.today()}-{args.case}-{args.runner}",
                workload="gutsrun_infretis.chain:InfretisRun", args=vars(run),
                time=args.time, margin=args.margin, links=args.links)
    try:
        print(start(spec, night=args.night, submit_first=not args.no_submit))
    except ChainError as e:
        sys.exit(str(e))


if __name__ == "__main__":
    main()
