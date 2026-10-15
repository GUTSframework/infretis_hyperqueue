"""Run bare infretis (ProcessPoolExecutor, no HQ) on a prepared example.

Copies `examples/<name>` to a fresh run directory, turns `load_copy/` into
`load/`, applies config overrides, runs `infretisrun` and records what ran in
`run.json` next to the outputs.

Usage (inside `nix develop`):

    .venv/bin/python bench/bare_run.py examples/2d_potential runs/2d-w4 \
        --workers 4 --steps 1000

For GROMACS examples, `--wmdrun` is a template expanded once per worker with
`{i}` (worker index) and `{offset}` (i * --cores-per-worker):

    --wmdrun "gmx mdrun -ntomp 2 -ntmpi 1 -pin on -pinoffset {offset} -pinstride 1" \
    --cores-per-worker 2

To restart a run in place (later chain links), pass `--restart`: it runs
`infretisrun -i restart.toml` in the existing directory instead of copying.
"""

import argparse
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import tomli
import tomli_w


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("example", type=Path, help="prepared example directory")
    p.add_argument("rundir", type=Path, help="run directory to create")
    p.add_argument("--workers", type=int)
    p.add_argument("--steps", type=int)
    p.add_argument("--seed", type=int)
    p.add_argument("--wmdrun", help="per-worker command template ({i}, {offset})")
    p.add_argument("--cores-per-worker", type=int, default=1)
    p.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="extra override, dotted key, VALUE parsed as TOML (e.g. engine.subcycles=2)",
    )
    p.add_argument(
        "--kill-after",
        type=float,
        help="send SIGTERM after this many seconds (restart drills)",
    )
    p.add_argument(
        "--restart", action="store_true", help="continue from restart.toml in RUNDIR"
    )
    return p.parse_args()


def set_dotted(config, key, value):
    *path, last = key.split(".")
    node = config
    for part in path:
        node = node.setdefault(part, {})
    node[last] = value


def prepare(args):
    if args.rundir.exists():
        sys.exit(f"{args.rundir} exists; pick a new run directory")
    shutil.copytree(args.example, args.rundir, ignore=shutil.ignore_patterns("README.md"))
    if (args.rundir / "load_copy").is_dir():
        (args.rundir / "load_copy").rename(args.rundir / "load")

    cfg_path = args.rundir / "infretis.toml"
    config = tomli.loads(cfg_path.read_text())
    if args.workers is not None:
        config["runner"]["workers"] = args.workers
    workers = config["runner"]["workers"]
    if args.wmdrun:
        config["runner"]["wmdrun"] = [
            args.wmdrun.format(i=i, offset=i * args.cores_per_worker)
            for i in range(workers)
        ]
    elif "wmdrun" in config["runner"]:
        # ASE examples use a placeholder per worker; keep the list length right.
        wmdrun = config["runner"]["wmdrun"]
        if len(wmdrun) != workers and len(set(map(str, wmdrun))) == 1:
            config["runner"]["wmdrun"] = [wmdrun[0]] * workers
    if args.steps is not None:
        config["simulation"]["steps"] = args.steps
    if args.seed is not None:
        config["simulation"]["seed"] = args.seed
    for item in args.set:
        key, _, value = item.partition("=")
        set_dotted(config, key, tomli.loads(f"v = {value}")["v"])
    cfg_path.write_text(tomli_w.dumps(config))
    return "infretis.toml"


def git_head(path):
    try:
        return subprocess.run(
            ["git", "-C", str(path), "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def main():
    args = parse_args()
    if args.restart:
        inp = "restart.toml"
        if not (args.rundir / inp).exists():
            sys.exit(f"no {inp} in {args.rundir}")
    else:
        inp = prepare(args)

    infretisrun = Path(sys.executable).with_name("infretisrun")
    cmd = [str(infretisrun), "-i", inp]
    started = time.time()
    proc = subprocess.Popen(cmd, cwd=args.rundir)
    killed = False
    try:
        proc.wait(timeout=args.kill_after)
    except subprocess.TimeoutExpired:
        proc.send_signal(signal.SIGTERM)
        killed = True
        proc.wait()
    wall = time.time() - started

    record = {
        "started": datetime.fromtimestamp(started, timezone.utc).isoformat(),
        "wall_s": round(wall, 3),
        "returncode": proc.returncode,
        "killed_after_s": args.kill_after if killed else None,
        "command": cmd,
        "input": inp,
        "example": str(args.example),
        "host": platform.node(),
        "cpus": os.cpu_count(),
        "python": platform.python_version(),
        "repo_commit": git_head(Path(__file__).parent),
    }
    try:
        from importlib.metadata import version

        record["infretis_version"] = version("infretis")
    except Exception:
        pass
    runs = args.rundir / "run.json"
    history = json.loads(runs.read_text()) if runs.exists() else []
    history.append(record)
    runs.write_text(json.dumps(history, indent=2) + "\n")
    print(json.dumps(record, indent=2))
    sys.exit(0 if killed else proc.returncode)


if __name__ == "__main__":
    main()
