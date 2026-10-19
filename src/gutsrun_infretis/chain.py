"""Run infretis as a chain of Slurm jobs, in your own run directory.

From a cluster's login node:

    cd <run directory>          # infretis.toml, load/, the engine's inputs
    gutsrun-infretis submit --site das6 --time 00:14:00 --links 20
    gutsrun-infretis status [--follow]
    gutsrun-infretis stop

Each link runs the gutsrun launcher (`gutsrun_infretis.launcher`) with HQ,
from restart.toml if there is one, else infretis.toml, and stops at the
walltime with a valid restart.toml; the next link continues from it. The
run is done when restart.toml's `cstep` reaches `simulation.steps`; raise
`steps` there and `submit` again to run longer.

Settings come from gutsrun.toml in the run directory if there is one
(keys below), and the command line overrides them. The chain's own files,
including each link's log, go to .gutsrun/ there.
"""

# Standard library only: a link imports this module to read the run's state.

import argparse
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from gutsrun.slurm import (ROOT, ChainError, Progress, Site, Spec, StateError, follow,
                           load_spec, seconds, setup_logging, start, status, stop, walltime)

# gutsrun.toml's keys, their types and what they mean (for --help).
CONFIG = {
    "site": (str, "sites/<site>.toml in the gutsrun checkout"),
    "name": (str, "label for the Slurm job names (default: the directory's name)"),
    "time": (str, "walltime per link, e.g. 00:14:00 (default: just under the site's "
                  "max_walltime_day)"),
    "links": (int, "most links, a cap on the whole chain (default: 10)"),
    "margin": (int, "seconds before the walltime that SIGTERM is asked for (default: 90)"),
    "cores_per_task": (int, "cores per infretis move, i.e. per MD engine call (default: 1)"),
}


@dataclass
class InfretisRun:
    """The infretis workload: the gutsrun launcher with HQ."""

    cores_per_task: int = 1

    @property
    def needs_hq(self) -> bool:
        return True

    def progress(self, rundir: Path) -> Progress:
        path = rundir / "restart.toml"
        if not path.exists():
            return Progress(False, "not started (no restart.toml)")
        try:
            config = tomllib.loads(path.read_text())
            cstep, steps = config["current"]["cstep"], config["simulation"]["steps"]
        except (OSError, tomllib.TOMLDecodeError, KeyError) as e:
            raise StateError(f"restart.toml in {rundir} does not parse ({e}); "
                             "restore it from restart.toml.bak.*") from e
        return Progress(cstep >= steps, f"step {cstep} of {steps}")

    def before_link(self, rundir: Path, link: int) -> None:
        pass  # the launcher backs up restart.toml itself

    def argv(self, python: str, rundir: Path, link: int) -> list[str]:
        inp = "restart.toml" if (rundir / "restart.toml").exists() else "infretis.toml"
        return [python, "-m", "gutsrun_infretis.launcher", "-i", inp, "--backend", "hq",
                "--cores-per-task", str(self.cores_per_task)]


def read_config(rundir: Path) -> dict[str, Any]:
    """gutsrun.toml in `rundir`, checked against CONFIG; {} without one."""
    path = rundir / "gutsrun.toml"
    if not path.exists():
        return {}
    config = tomllib.loads(path.read_text())
    for key, value in config.items():
        if key not in CONFIG:
            raise ChainError(f"{path}: unknown key {key!r}; known: {', '.join(CONFIG)}")
        if not isinstance(value, CONFIG[key][0]):
            raise ChainError(f"{path}: {key} must be a {CONFIG[key][0].__name__}")
    return config


def submit(rundir: Path, options: dict[str, Any], night: bool = False,
           submit_first: bool = True, root: str | Path = ROOT) -> str:
    """Start a chain in `rundir` with gutsrun.toml's settings, overridden by
    `options` (those not None); returns the first job's id (or the spec's
    path, if not `submit_first`). `root`: the gutsrun checkout."""
    rundir = rundir.resolve()
    if not (rundir / "infretis.toml").exists():
        raise ChainError(f"no infretis.toml in {rundir}")
    config = read_config(rundir) | {k: v for k, v in options.items() if v is not None}
    if "site" not in config:
        raise ChainError("no site: set `site` in gutsrun.toml or pass --site")
    spec = Spec(rundir=str(rundir), site=config["site"],
                name=config.get("name", rundir.name),
                workload="gutsrun_infretis.chain:InfretisRun",
                args={"cores_per_task": config.get("cores_per_task", 1)},
                margin=config.get("margin", 90), links=config.get("links", 10),
                root=str(root))
    if "time" in config:
        spec.time = config["time"]
    else:
        limit = Site(spec.root, spec.site).max_walltime_day
        if limit is None:
            raise ChainError("no time: set `time` in gutsrun.toml or pass --time")
        spec.time = walltime(seconds(limit) - 60)
    return start(spec, night=night, submit_first=submit_first)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="gutsrun-infretis",
                                description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                epilog="gutsrun.toml keys: " + ", ".join(CONFIG))
    commands = p.add_subparsers(dest="command", required=True)

    sub = commands.add_parser("submit", help="start a chain in the run directory")
    sub.add_argument("rundir", nargs="?", type=Path, default=Path.cwd())
    for key, (kind, text) in CONFIG.items():
        sub.add_argument(f"--{key.replace('_', '-')}", dest=key, type=kind, help=text)
    sub.add_argument("--night", action="store_true",
                     help="allow a walltime beyond the site's max_walltime_day")
    sub.add_argument("--no-submit", action="store_true",
                     help="write the spec only and print its path")

    stat = commands.add_parser("status", help="report on the chain in the run directory")
    stat.add_argument("rundir", nargs="?", type=Path, default=Path.cwd())
    stat.add_argument("--follow", action="store_true",
                      help="print it again on every change, until the chain ends")
    stat.add_argument("--interval", type=float, default=30, help="seconds between checks")

    halt = commands.add_parser("stop", help="stop the chain in the run directory")
    halt.add_argument("rundir", nargs="?", type=Path, default=Path.cwd())

    args = p.parse_args(argv)
    setup_logging()
    try:
        if args.command == "submit":
            options = {key: getattr(args, key) for key in CONFIG}
            result = submit(args.rundir, options, args.night, not args.no_submit)
            if args.no_submit:
                print(result)
            else:
                print(f"link 1: job {result}; follow it with `gutsrun-infretis status --follow`")
        elif args.command == "status":
            spec = load_spec(args.rundir)
            if args.follow:
                follow(spec, args.interval)
            else:
                print(*status(spec)[0], sep="\n")
        else:
            print(*stop(load_spec(args.rundir)), sep="\n")
    except (ChainError, StateError) as e:
        sys.exit(f"gutsrun-infretis: {e}")


if __name__ == "__main__":
    main()
