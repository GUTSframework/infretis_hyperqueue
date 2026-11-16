"""Run one link of a chain, inside its Slurm job:

    .venv/bin/python -m gutsrun.link <rundir>/.gutsrun/chain.json <link>

where <link> is the link's number, 1, 2, ... `gutsrun.chain` puts this
command into each link's job and says what a link does.

A module of its own so that `gutsrun.chain` is only ever imported. Run as
`python -m gutsrun.chain`, that file would be loaded twice, as `__main__`
and as `gutsrun.chain` when the workload imports it, and the link would not
catch the `StateError` a workload raises: a different class of the same
name.

Standard library only, like `gutsrun.chain` (see there).
"""

import argparse
import sys
from pathlib import Path

from gutsrun.chain import run_link, setup_logging


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m gutsrun.link",
                                     description="run one link of a chain, inside its "
                                                 "Slurm job")
    parser.add_argument("spec", type=Path, help="the chain's spec, <rundir>/.gutsrun/chain.json")
    parser.add_argument("link", type=int, help="the link's number, 1, 2, ...")
    args = parser.parse_args(argv)
    setup_logging()
    return run_link(args.spec, args.link)


if __name__ == "__main__":
    sys.exit(main())
