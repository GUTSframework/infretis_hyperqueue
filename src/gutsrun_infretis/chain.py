"""Run infretis as a chain of Slurm jobs, in your own run directory.

From a cluster's login node:

    cd <run directory>          # infretis.toml, load/, the engine's inputs
    gutsrun-infretis submit --site das6 --time 00:14:00 --links 20
    gutsrun-infretis submit --check     # the checks and the sbatch command only
    gutsrun-infretis status [--follow | --json]
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
import dataclasses
import json
import shlex
import sys
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, get_args

from gutsrun.chain import (
    ROOT,
    ChainError,
    ChainSpec,
    RunProgress,
    StateError,
    chain_status,
    check_before_start,
    follow_chain_status,
    link_sbatch_argv,
    load_spec,
    seconds_to_slurm_walltime,
    setup_logging,
    slurm_walltime_to_seconds,
    start_chain,
    stop_chain,
)
from gutsrun.site import Site, SiteError

EXIT_ACTIVE = 10  # `status`: some job of the chain is still queued or running


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


def setting(text: str) -> Any:
    """A `Config` field: not set (None) unless gutsrun.toml or the command
    line sets it. `text` is for --help."""
    return field(default=None, metadata={"help": text})


@dataclass
class Config:
    """The user's settings for a chain, as written: gutsrun.toml's keys, which
    are also `submit`'s options. Any of them may be missing (None).
    `make_spec` turns them into a `ChainSpec`, the complete chain the links
    read; a setting that is not set takes its default from there
    (`ChainSpec` for the chain's settings, `InfretisRun` for the moves')."""

    site: str | None = setting("sites/<site>.toml in the gutsrun checkout (required)")
    name: str | None = setting("label for the Slurm job names (default: the directory's name)")
    time: str | None = setting("walltime per link, e.g. 00:14:00 (default: just under the "
                               "site's max_walltime)")
    links: int | None = setting("most links, a cap on the whole chain")
    margin: int | None = setting("seconds before the walltime that SIGTERM is asked for")
    nodes: int | None = setting("nodes per link, each with an HQ worker on all its cores")
    constraint: str | None = setting("Slurm node features the link's nodes must have, e.g. "
                                     "a GPU type such as A4000 (sbatch --constraint; not "
                                     "set: any node)")
    cores_per_task: int | None = setting("cores per infretis move, i.e. per MD engine call")
    gpus_per_task: int | None = setting("GPUs per infretis move")
    time_request: str | None = setting("start a move only on a worker with this much time "
                                       "left in its link, e.g. 30m: about the longest move "
                                       "plus `margin` (not set: any worker)")


SETTINGS = [f.name for f in dataclasses.fields(Config)]  # gutsrun.toml's keys
CHAIN_SETTINGS = ["links", "margin", "nodes", "constraint"]  # passed on to ChainSpec
WORKLOAD_SETTINGS = [f.name for f in dataclasses.fields(InfretisRun)]  # to InfretisRun


def setting_type(key: str) -> type:
    """The type of a `Config` field, without its None: int or str."""
    [f] = [f for f in dataclasses.fields(Config) if f.name == key]
    return next(t for t in get_args(f.type) if t is not type(None))


def setting_help(key: str) -> str:
    """--help's text for a setting, with the default it takes from `ChainSpec` or
    `InfretisRun` when it is not set."""
    [f] = [f for f in dataclasses.fields(Config) if f.name == key]
    text = f.metadata["help"]
    owner = (ChainSpec if key in CHAIN_SETTINGS
             else InfretisRun if key in WORKLOAD_SETTINGS else None)
    if owner is None:
        return text
    [default] = [o.default for o in dataclasses.fields(owner) if o.name == key]
    return text if default is None else f"{text} (default: {default})"


def read_gutsrun_toml(rundir: Path) -> Config:
    """gutsrun.toml in `rundir`, checked against `Config`; nothing set
    without one."""
    path = rundir / "gutsrun.toml"
    if not path.exists():
        return Config()
    values = tomllib.loads(path.read_text())
    for key, value in values.items():
        if key not in SETTINGS:
            raise ChainError(f"{path}: unknown key {key!r}; known: {', '.join(SETTINGS)}")
        kind = setting_type(key)
        # bool is a subclass of int: `links = true` must not pass as 1.
        if isinstance(value, bool) or not isinstance(value, kind):
            wanted = "an integer" if kind is int else "a string"
            raise ChainError(f"{path}: {key} must be {wanted}")
    return Config(**values)


def make_spec(rundir: Path, options: Config,
              root: str | Path = ROOT) -> tuple[ChainSpec, str]:
    """The chain for `rundir`, from gutsrun.toml's settings overridden by
    the command line's `options` (those set), and where its walltime came
    from, for the user. `root`: the gutsrun checkout."""
    rundir = rundir.resolve()
    if not (rundir / "infretis.toml").exists():
        raise ChainError(f"no infretis.toml in {rundir}")
    from_file = read_gutsrun_toml(rundir)
    config = dataclasses.replace(
        from_file, **{k: v for k, v in asdict(options).items() if v is not None})
    if config.site is None:
        raise ChainError("no site: set `site` in gutsrun.toml or pass --site")
    site = Site(root, config.site)

    def chosen(keys: list[str]) -> dict[str, Any]:
        """The settings among `keys` that are set: the rest keep their defaults."""
        return {k: getattr(config, k) for k in keys if getattr(config, k) is not None}

    run = InfretisRun(**chosen(WORKLOAD_SETTINGS))
    if run.cores_per_task > site.cores:
        # HQ would hold every move: no worker has that many cores.
        raise ChainError(f"cores_per_task is {run.cores_per_task}, but a node of "
                         f"{config.site} has {site.cores} cores (cores_per_node); no move "
                         "would ever start")

    if options.time is not None:
        time, source = options.time, "from --time"
    elif from_file.time is not None:
        time, source = from_file.time, "from gutsrun.toml"
    else:
        limit = site.max_walltime
        if limit is None:
            raise ChainError("no time: set `time` in gutsrun.toml or pass --time")
        time = seconds_to_slurm_walltime(slurm_walltime_to_seconds(limit) - 60)
        source = f"just under max_walltime {limit} in sites/{config.site}.toml"

    spec = ChainSpec(rundir=str(rundir), site=config.site, name=config.name or rundir.name,
                     workload="gutsrun_infretis.chain:InfretisRun", time=time, args=asdict(run),
                     root=str(root), **chosen(CHAIN_SETTINGS))
    return spec, source


def describe_submit(spec: ChainSpec) -> list[str]:
    """What `submit` would do, after the checks; writes nothing."""
    plan = check_before_start(spec)
    lines = [f"checks passed: {spec.name} on {spec.site}, {plan.progress.text}"]
    lines.append(f"would submit link 1 of {spec.links}: "
                 f"{shlex.join(link_sbatch_argv(spec, plan.site, 1))}")
    return lines


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="gutsrun-infretis",
                                description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                epilog="gutsrun.toml keys: " + ", ".join(SETTINGS))
    commands = p.add_subparsers(dest="command", required=True)

    sub = commands.add_parser("submit", help="start a chain in the run directory")
    sub.add_argument("rundir", nargs="?", type=Path, default=Path.cwd())
    for key in SETTINGS:
        sub.add_argument(f"--{key.replace('_', '-')}", dest=key, type=setting_type(key),
                         help=setting_help(key))
    sub.add_argument("--check", action="store_true",
                     help="run the checks and show the sbatch command; submit nothing")

    stat = commands.add_parser("status", help="report on the chain in the run directory")
    stat.add_argument("rundir", nargs="?", type=Path, default=Path.cwd())
    stat.epilog = ("exit status: 0 once no job of the chain is queued or running, "
                   f"{EXIT_ACTIVE} while some are (or squeue does not answer)")
    how = stat.add_mutually_exclusive_group()
    how.add_argument("--follow", action="store_true",
                     help="print it again whenever it changes, until the chain ends")
    how.add_argument("--json", action="store_true",
                     help="print one JSON object, for scripts: progress, links (one line "
                          "each), over (null while active), active, and lines (the report "
                          "as text)")
    stat.add_argument("--interval", type=float, default=30, help="seconds between checks")

    halt = commands.add_parser("stop", help="stop the chain in the run directory")
    halt.add_argument("rundir", nargs="?", type=Path, default=Path.cwd())

    args = p.parse_args(argv)
    setup_logging()
    try:
        if args.command == "submit":
            spec, source = make_spec(args.rundir,
                                     Config(**{key: getattr(args, key) for key in SETTINGS}))
            print(f"walltime per link: {spec.time} ({source})")
            if args.check:
                print(*describe_submit(spec), sep="\n")
            else:
                job = start_chain(spec)
                print(f"link 1: job {job}; follow it with `gutsrun-infretis status --follow`")
        elif args.command == "status":
            spec = load_spec(args.rundir)
            if args.follow:
                follow_chain_status(spec, args.interval)
            else:
                report = chain_status(spec)
                if args.json:
                    print(json.dumps({**asdict(report), "lines": report.lines()}))
                else:
                    print(*report.lines(), sep="\n")
                sys.exit(EXIT_ACTIVE if report.active else 0)
        else:
            print(*stop_chain(load_spec(args.rundir)), sep="\n")
    except (ChainError, SiteError, StateError) as e:
        sys.exit(f"gutsrun-infretis: {e}")


if __name__ == "__main__":
    main()
