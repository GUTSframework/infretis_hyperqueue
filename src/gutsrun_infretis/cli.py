"""The `gutsrun-infretis` command: run infretis as a chain of Slurm jobs, in
your own run directory.

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

import argparse
import dataclasses
import json
import shlex
import sys
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, get_args, get_type_hints

from gutsrun.chain import (
    FOLLOW_INTERVAL_SECONDS,
    ROOT,
    ChainError,
    ChainSpec,
    StateError,
    chain_status,
    check_before_start,
    follow_chain_status,
    link_sbatch_argv,
    load_spec,
    setup_logging,
    start_chain,
    stop_chain,
)
from gutsrun.site import Site, SiteError
from gutsrun_infretis.workload import InfretisRun

EXIT_ACTIVE = 10  # `status`: some job of the chain is still queued or running


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
    time: str | None = setting("walltime per link, e.g. 00:14:00 (default: the site's "
                               "max_walltime)")
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
# How make_spec uses each setting: every one is in exactly one group
# (tests/test_chain.py checks this).
CHAIN_SETTINGS = ["links", "margin", "nodes", "constraint"]  # passed on to ChainSpec
WORKLOAD_SETTINGS = [f.name for f in dataclasses.fields(InfretisRun)]  # to InfretisRun
SPECIAL_SETTINGS = ["site", "name", "time"]  # each with its own rule in make_spec

CONFIG_HELP = {f.name: f.metadata["help"] for f in dataclasses.fields(Config)}
# `int | None` -> int: what a setting's value must be when it is set.
CONFIG_TYPES = {key: next(t for t in get_args(hint) if t is not type(None))
                for key, hint in get_type_hints(Config).items()}
# What a setting that is not set becomes: the default of its field in its owner.
DEFAULTS = ({f.name: f.default for f in dataclasses.fields(ChainSpec) if f.name in CHAIN_SETTINGS}
            | {f.name: f.default for f in dataclasses.fields(InfretisRun)})


def setting_help(key: str) -> str:
    """--help's text for a setting, with the default it takes when it is not
    set (from `ChainSpec` or `InfretisRun`; a default of None says nothing)."""
    default = DEFAULTS.get(key)
    return CONFIG_HELP[key] if default is None else f"{CONFIG_HELP[key]} (default: {default})"


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
        kind = CONFIG_TYPES[key]
        # bool is a subclass of int: `links = true` must not pass as 1.
        if isinstance(value, bool) or not isinstance(value, kind):
            wanted = "an integer" if kind is int else "a string"
            raise ChainError(f"{path}: {key} must be {wanted}")
    return Config(**values)


def make_spec(rundir: Path, options: Config,
              root: str | Path = ROOT) -> tuple[ChainSpec, str]:
    """Build the chain for the run in `rundir`.

    The settings come from `rundir`'s gutsrun.toml; those set in `options`
    (the command line) override them, and those set in neither take their
    defaults. `root` is the gutsrun checkout, whose sites/ holds the site
    files.

    Returns the chain and where its walltime came from (--time,
    gutsrun.toml or the site's max_walltime), which `submit` prints.
    Raises ChainError if the run directory or the settings will not do."""
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
        time, source = limit, f"max_walltime {limit} in sites/{config.site}.toml"

    spec = ChainSpec(rundir=str(rundir), site=config.site, name=config.name or rundir.name,
                     workload="gutsrun_infretis.workload:InfretisRun", time=time, args=asdict(run),
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
        sub.add_argument(f"--{key.replace('_', '-')}", dest=key, type=CONFIG_TYPES[key],
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
                          "each), state (active, over, or unknown if squeue does not "
                          "answer), reason (why it is over; null otherwise), and lines "
                          "(the report as text)")
    stat.add_argument("--interval", type=float, default=FOLLOW_INTERVAL_SECONDS,
                      help="seconds between checks (default: %(default)s)")

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
                sys.exit(0 if report.state == "over" else EXIT_ACTIVE)
        else:
            print(*stop_chain(load_spec(args.rundir)), sep="\n")
    except (ChainError, SiteError, StateError) as e:
        sys.exit(f"gutsrun-infretis: {e}")


if __name__ == "__main__":
    main()
