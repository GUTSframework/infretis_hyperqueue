"""Chains of an example case with either runner, for the bare-vs-gutsrun
comparison on a cluster. Run it on the login node, from the project root:

    .venv/bin/python bench/chain.py 2d gutsrun --steps 1000 --time 00:05:00
    .venv/bin/python bench/chain.py 2d bare --steps 3000 --time 00:04:00

case: 2d (examples/2d_potential) or lammps (examples/water_dissociation_lammps;
needs an `lmp` on the nodes). runner: bare (infretis's own
ProcessPoolExecutor, the baseline row) or gutsrun (the launcher with the HQ
backend). The run directory is runs/<site>/<date>-<case>-<runner>, prepared
here as bench/bare_run.py does; each link runs bare_run.py --in-place there,
which records run.json. Prints link 1's job id, for scripts/wait-jobs.sh;
`gutsrun-infretis status <run directory>` reports on the chain.
"""

import argparse
import shutil
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from gutsrun.slurm import ROOT, ChainError, Site, Spec, StateError, check_time, setup_logging, start
from gutsrun_infretis.chain import InfretisRun

CASES = {"2d": "examples/2d_potential", "lammps": "examples/water_dissociation_lammps"}
BENCH = Path(__file__).resolve().parent


@dataclass
class BenchRun(InfretisRun):
    """bench/bare_run.py --in-place with either runner."""

    example: str = ""
    runner: str = "gutsrun"  # bare | gutsrun

    @property
    def needs_hq(self) -> bool:
        return self.runner == "gutsrun"

    def before_link(self, rundir: Path, link: int) -> None:
        # Bare infretis writes restart.toml in place, so a kill can truncate it.
        # None yet if the run was stopped before its first step.
        if (rundir / "restart.toml").exists():
            shutil.copy2(rundir / "restart.toml", rundir / f"restart.toml.link{link}")

    def argv(self, python: str, rundir: Path, link: int) -> list[str]:
        argv = [python, str(BENCH / "bare_run.py"), self.example, str(rundir), "--in-place",
                "--runner", self.runner]
        if self.runner == "gutsrun":
            argv += ["--backend", "hq", "--cores-per-task", str(self.cores_per_task)]
        return argv


def main() -> None:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
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
                   help="prepare the run and its chain, submit nothing, print the spec's "
                        "path: run links by hand with `python -m gutsrun.slurm link`")
    args = p.parse_args()
    setup_logging()

    import bare_run  # not at the top: links import this module, standard library only

    name = args.name or f"{date.today()}-{args.case}-{args.runner}"
    rundir = ROOT / "runs" / args.site / name
    example = str(ROOT / CASES[args.case])
    spec = Spec(rundir=str(rundir), site=args.site, name=name,
                workload="chain:BenchRun", pythonpath=[str(BENCH)],
                args={"example": example, "runner": args.runner},
                time=args.time, margin=args.margin, links=args.links)
    try:
        check_time(spec, Site(ROOT, args.site), args.night)  # before preparing the run
        rundir.parent.mkdir(parents=True, exist_ok=True)
        bare_run.prepare(argparse.Namespace(
            example=Path(example), rundir=rundir, workers=args.workers, steps=args.steps,
            seed=None, wmdrun=None, cores_per_worker=1, set=[]))
        print(start(spec, night=args.night, submit_first=not args.no_submit))
    except (ChainError, StateError) as e:
        sys.exit(str(e))


if __name__ == "__main__":
    main()
