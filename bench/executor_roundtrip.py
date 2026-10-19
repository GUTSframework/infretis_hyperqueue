"""Sequential round trip of a trivial call through gutsrun's TaskExecutor.

Compares with bench/hq_dispatch.py's "function" row (HQ Python function
task, 80 ms on the laptop): gutsrun uses HQ program tasks running
`python -m gutsrun.task`, which carry only paths.

Usage (inside `nix develop`; starts its own hq server + worker):

    .venv/bin/python bench/executor_roundtrip.py --repeat 30
"""

import argparse
import json
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path[:0] = [str(Path(__file__).resolve().parent.parent / "src")]

from gutsrun import TaskExecutor  # noqa: E402
from gutsrun.backends.local import LocalBackend  # noqa: E402


def timed(ex: TaskExecutor, repeat: int) -> dict[str, float]:
    times = []
    for i in range(repeat):
        start = time.perf_counter()
        ex.submit(abs, -i).result()
        times.append(time.perf_counter() - start)
    return {"n": repeat, "median_s": round(statistics.median(times), 4),
            "min_s": round(min(times), 4), "max_s": round(max(times), 4)}


def main() -> None:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("--repeat", type=int, default=30)
    p.add_argument("--backends", default="local,hq")
    args = p.parse_args()
    backends = args.backends.split(",")
    results = {}
    with tempfile.TemporaryDirectory(prefix="gutsrun-rt-") as tmp:
        tmp = Path(tmp)
        if "local" in backends:
            ex = TaskExecutor(LocalBackend(1), tmp / "spool-local")
            results["local"] = timed(ex, args.repeat)
            ex.shutdown()
        if "hq" not in backends:
            print(json.dumps(results, indent=2))
            return

        hq = shutil.which("hq")
        if hq is None:
            sys.exit("no hq on PATH (run inside `nix develop`)")
        server_dir = tmp / "hq"
        server = subprocess.Popen([hq, "server", "start", "--server-dir", str(server_dir)],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        while not (server_dir / "hq-current").exists():
            time.sleep(0.05)
        worker = subprocess.Popen([hq, "worker", "start", "--server-dir", str(server_dir),
                                   "--cpus", "1"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            from gutsrun.backends.hq import HQBackend
            ex = TaskExecutor(HQBackend(str(server_dir)), tmp / "spool-hq")
            ex.submit(abs, 0).result()  # waits for the worker to connect
            results["hq"] = timed(ex, args.repeat)
            ex.shutdown()
        finally:
            subprocess.run([hq, "server", "stop", "--server-dir", str(server_dir)],
                           capture_output=True)
            worker.wait(timeout=10)
            server.wait(timeout=10)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
