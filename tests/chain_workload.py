"""A counting workload for the gutsrun.slurm tests (standard library only).

`Counter` is the workload; run as a script it is the command a link runs:
it counts `step` in <rundir>/state.json up to `steps`, one per `delay`
seconds, exits 3 on SIGTERM and 1 at `fail_at`. Under HQ it records what
it saw of the server in hq.json, after waiting for `workers` HQ workers
to join. With `crash`, `argv()` raises, as a workload with a bug would.
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from gutsrun.slurm import Progress, StateError


@dataclass
class Counter:
    steps: int = 5
    delay: float = 0.05
    fail_at: int | None = None
    needs_hq: bool = False
    crash: bool = False
    workers: int = 1

    def progress(self, rundir: Path) -> Progress:
        path = rundir / "state.json"
        if not path.exists():
            return Progress(False, f"step 0 of {self.steps}")
        try:
            step = json.loads(path.read_text())["step"]
        except (OSError, ValueError, KeyError) as e:
            raise StateError(f"state.json in {rundir} does not parse ({e})") from e
        return Progress(step >= self.steps, f"step {step} of {self.steps}")

    def before_link(self, rundir: Path, link: int) -> None:
        (rundir / f"before{link}").touch()

    def argv(self, python: str, rundir: Path, link: int) -> list[str]:
        if self.crash:
            raise RuntimeError("boom")
        argv = [python, __file__, str(rundir), "--steps", str(self.steps),
                "--delay", str(self.delay)]
        if self.fail_at is not None:
            argv += ["--fail-at", str(self.fail_at)]
        if self.needs_hq:
            argv += ["--workers", str(self.workers)]
        return argv


def hq_workers(expected: int, timeout: float = 30) -> int:
    """How many HQ workers have joined, once `expected` have (or at the timeout)."""
    deadline = time.monotonic() + timeout
    while True:
        out = subprocess.run(["hq", "worker", "list", "--output-mode", "json"],
                             capture_output=True, text=True).stdout
        count = len(json.loads(out or "[]"))
        if count >= expected or time.monotonic() > deadline:
            return count
        time.sleep(0.2)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("rundir", type=Path)
    p.add_argument("--steps", type=int)
    p.add_argument("--delay", type=float)
    p.add_argument("--fail-at", type=int)
    p.add_argument("--workers", type=int, default=1)
    args = p.parse_args()
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(3))
    state = args.rundir / "state.json"
    step = json.loads(state.read_text())["step"] if state.exists() else 0
    (args.rundir / "python.txt").write_text(sys.executable)
    (args.rundir / "cwd.txt").write_text(os.getcwd())
    if "HQ_SERVER_DIR" in os.environ:
        workers = hq_workers(args.workers)
        (args.rundir / "hq.json").write_text(json.dumps({
            "listed": os.listdir(os.environ["HQ_SERVER_DIR"]), "hq": shutil.which("hq"),
            "workers": workers}))
    while step < args.steps:
        time.sleep(args.delay)
        step += 1
        if step == args.fail_at:
            sys.exit(1)
        tmp = args.rundir / ".state.json.tmp"
        tmp.write_text(json.dumps({"step": step}))
        os.replace(tmp, state)


if __name__ == "__main__":
    main()
