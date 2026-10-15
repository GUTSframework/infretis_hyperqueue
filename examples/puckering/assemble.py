"""Turn an `inft infinit` result into a run-ready example for bench/bare_run.py.

infinit leaves its last completed simulation as `infretis_<N>.toml` +
`restart_<N>.toml` (the highest N); `infretis.toml` next to them holds the
interfaces it would use for a further round, which the active paths do not
necessarily fit. So we take the highest N, copy its active paths into
`load_copy/0..n-1` (one per ensemble, which is what a fresh infretis start
expects) and point `input_path` at a local copy of `gromacs_input/`.

Usage:
    .venv/bin/python examples/puckering/assemble.py PREPDIR
where PREPDIR is the directory prepare.sh wrote (default runs/prepared/puckering).
The result is PREPDIR/ready/.
"""

import re
import shutil
import sys
from pathlib import Path

import tomli
import tomli_w


def main():
    prep = Path(sys.argv[1] if len(sys.argv) > 1 else "runs/prepared/puckering")
    infinit = prep / "infinit"
    ready = prep / "ready"
    if ready.exists():
        sys.exit(f"{ready} exists")

    n = max(int(m.group(1)) for p in infinit.glob("restart_*.toml")
            if (m := re.fullmatch(r"restart_(\d+)\.toml", p.name)))
    restart = tomli.loads((infinit / f"restart_{n}.toml").read_text())
    config = tomli.loads((infinit / f"infretis_{n}.toml").read_text())
    if restart["simulation"]["interfaces"] != config["simulation"]["interfaces"]:
        sys.exit(f"interfaces of restart_{n}.toml and infretis_{n}.toml differ")
    active = restart["current"]["active"]

    ready.mkdir()
    shutil.copytree(prep / "gromacs_input", ready / "gromacs_input")
    for i, pn in enumerate(active):
        shutil.copytree(infinit / "load" / str(pn), ready / "load_copy" / str(i))

    config.pop("infinit", None)
    config.pop("current", None)
    config["engine"]["input_path"] = "gromacs_input"
    config["simulation"]["load_dir"] = "load"
    (ready / "infretis.toml").write_text(tomli_w.dumps(config))
    print(f"{ready}: {len(active)} ensembles from restart_{n}.toml, paths {active}")


if __name__ == "__main__":
    main()
