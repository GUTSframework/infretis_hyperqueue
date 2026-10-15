"""Time the cold start of an infretis worker for a prepared run directory.

One HQ task per move pays this on every move: start a fresh Python, import
infretis, and do what `worker_initializer` does (create the engines and order
parameters from the run's config). Each repetition is a separate process.

Usage (inside `nix develop`):

    .venv/bin/python bench/cold_start.py RUNDIR [--repeat 5]

RUNDIR must contain a `restart.toml` (preferred) or `infretis.toml` whose relative
paths resolve from RUNDIR, e.g. a directory made by bench/bare_run.py.
"""

import argparse
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

# Runs in the child. `infretis.setup` is imported first on purpose:
# `classes/engines/factory.py` uses `importlib.util` without importing it, so
# importing the factory first fails (import-order bug, 2026-10-14 log).
CHILD = r"""
import time, sys
t0 = time.perf_counter()
import importlib.util
from infretis.setup import setup_config
from infretis.classes.engines.factory import create_engines
from infretis.classes.orderparameter import create_orderparameters
t1 = time.perf_counter()
config = setup_config(sys.argv[1])
engines, _ = create_engines(config)
create_orderparameters(engines, config)
t2 = time.perf_counter()
print(f"{t1 - t0} {t2 - t1}")
"""


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("rundir", type=Path)
    p.add_argument("--repeat", type=int, default=5)
    p.add_argument("--input", default=None, help="config file (default: infretis.toml)")
    args = p.parse_args()
    # setup_config refuses infretis.toml once a restart.toml exists.
    inp = args.input or ("restart.toml" if (args.rundir / "restart.toml").exists() else "infretis.toml")

    rows = []
    for _ in range(args.repeat):
        start = time.perf_counter()
        out = subprocess.run(
            [sys.executable, "-c", CHILD, inp],
            cwd=args.rundir, capture_output=True, text=True,
        )
        total = time.perf_counter() - start
        if out.returncode != 0:
            sys.exit(out.stderr)
        imports, init = map(float, out.stdout.split()[-2:])
        rows.append({"total_s": total, "imports_s": imports, "init_s": init})

    def med(key):
        return statistics.median(r[key] for r in rows)

    print(json.dumps({
        "rundir": str(args.rundir),
        "repeat": args.repeat,
        "median_total_s": round(med("total_s"), 3),
        "median_imports_s": round(med("imports_s"), 3),
        "median_init_s": round(med("init_s"), 3),
        "runs": [{k: round(v, 3) for k, v in r.items()} for r in rows],
    }, indent=2))


if __name__ == "__main__":
    main()
