"""Run infretis with gutsrun instead of ProcessPoolExecutor.

    python -m gutsrun_infretis.launcher -i infretis.toml
    python -m gutsrun_infretis.launcher -i restart.toml --cores-per-task 2
    python -m gutsrun_infretis.launcher -i restart.toml --time-request 30m

Run it in the infretis run directory, like `infretisrun`. Each move runs as
an HQ task: on the server in $HQ_SERVER_DIR (or --hq-server-dir), as in a
chain's link; without one, the launcher runs its own HQ server and a
worker on all the cores it may use, for this run (server directory
`gutsrun-hq/`, logs `gutsrun-hq-server.log` and `gutsrun-hq-worker.log`).

It needs the infretis pinned in requirements.txt (the GUTS fork, branch
`guts`), and refuses another commit: its `scheduler` takes an
`executor_factory`, through which the launcher passes a factory that
returns a gutsrun TaskExecutor; its worker initializer needs no shared
state, its moves log to `worker{pin}.log`, its workers leave the shared
GROMACS inputs alone and stop their MD program on SIGTERM, it writes
restart.toml atomically, it stops gracefully on SIGTERM and SIGINT, and it
says why when it refuses to start. What the launcher adds, for this run
only:

- A `wmdrun` with `-pin on|auto`/`-pinoffset`/`-pinstride` is refused
  (HQ places tasks, so fixed offsets would put moves on the same cores),
  and so is one whose `-ntomp` differs from --cores-per-task (HQ sets
  OMP_NUM_THREADS to it; GROMACS aborts on a mismatch).
- `--time-request` (e.g. 30m) keeps a worker from starting a
  move unless it has that much time left. Off by default; set it to about
  the longest move plus the chain's SIGTERM margin.
- Once infretis's scheduler has returned, stop signals are ignored, so a
  second one cannot cut the shutdown short (exit -15, which a chain reads
  as a failure). A signal before the scheduler runs ends the launcher the
  default way: no move has run yet.

Exit codes: 0 = all steps done, 3 = stopped by a signal (restart from
restart.toml), 4 = infretis refused to start, also when the run is already
done (infretis's reason on stderr), other = error.
"""

import argparse
import contextlib
import json
import logging
import os
import re
import signal
import sys
from collections.abc import Callable
from importlib.metadata import distribution
from pathlib import Path
from typing import Any

from infretis.scheduler import scheduler
from infretis.setup import setup_config

from gutsrun import TaskExecutor, hq
from gutsrun.chain import ROOT
from gutsrun.executor import Backend

TESTED_COMMIT = "2502487"  # the fork's commit in requirements.txt
EXIT_DONE, EXIT_STOPPED, EXIT_REFUSED = 0, 3, 4

log = logging.getLogger("gutsrun_infretis")


# --- the executor ---------------------------------------------------------------


EXECUTORS: list[TaskExecutor] = []


def make_factory(make_backend: Callable[[], Backend], spool_dir: str,
                 resource_dict: dict[str, Any]) -> Callable[..., TaskExecutor]:
    """infretis's `executor_factory`: takes ProcessPoolExecutor's arguments
    and returns a TaskExecutor, which runs infretis's initializer before
    every call (each in a fresh task process)."""

    def factory(max_workers: int | None = None, initializer: Callable[..., object] | None = None,
                initargs: tuple[Any, ...] = (), mp_context: object = None) -> TaskExecutor:
        ex = TaskExecutor(
            make_backend(), spool_dir, max_workers=max_workers,
            initializer=initializer, initargs=initargs, resource_dict=resource_dict)
        EXECUTORS.append(ex)
        log.info("gutsrun executor: %s", ex.dir)
        return ex
    return factory


# --- checks -----------------------------------------------------------------------


def infretis_commit() -> str | None:
    """The installed infretis's git commit, if pip recorded one."""
    try:
        info = json.loads(distribution("infretis").read_text("direct_url.json") or "{}")
    except Exception:
        return None
    return info.get("vcs_info", {}).get("commit_id")


def check_infretis() -> None:
    """Exit if pip installed infretis from another commit than the pinned
    one. An infretis installed from a local directory records no commit:
    that only gets a warning."""
    commit = infretis_commit()
    if commit is None:
        log.warning("cannot tell which infretis commit is installed (tested: %s)",
                    TESTED_COMMIT)
    elif not commit.startswith(TESTED_COMMIT):
        sys.exit(f"infretis {commit[:7]} is installed, not the pinned {TESTED_COMMIT} "
                 "(requirements.txt). In an existing .venv, pip keeps the old one "
                 "because the version number is the same: add --force-reinstall.")


PINNING = re.compile(r"(?<!\S)(-pin\s+(on|auto)|-pinoffset|-pinstride)(?!\S)")


def check_wmdrun(wmdrun: list, cores: int) -> None:
    """Exit if a GROMACS command in `wmdrun` cannot run as an HQ task as it
    is: with pinning flags, or with -ntomp other than the task's cores."""
    for cmd in wmdrun:
        if not isinstance(cmd, str):
            continue  # LAMMPS and ASE examples use placeholders
        if m := PINNING.search(cmd):
            sys.exit(f"wmdrun has {m.group()} in {cmd!r}: under HQ, use -pin off "
                     "and no -pinoffset or -pinstride; HQ places the tasks, and "
                     "fixed offsets would put moves on the same cores")
        m = re.search(r"-ntomp\s+(\d+)", cmd)
        if m and int(m.group(1)) != cores:
            sys.exit(f"wmdrun has -ntomp {m.group(1)} but --cores-per-task is {cores}; "
                     "HQ sets OMP_NUM_THREADS to the task's cores and GROMACS aborts "
                     "when they differ")


# --- main -------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("-i", "--input", required=True)
    p.add_argument("--hq-server-dir", default=os.environ.get("HQ_SERVER_DIR"),
                   help="the HQ server to run moves on (default: $HQ_SERVER_DIR, else "
                        "a server of the launcher's own)")
    p.add_argument("--spool", default="gutsrun-spool")
    p.add_argument("--cores-per-task", type=int, default=1)
    p.add_argument("--time-request", help="start a move only on a worker with this "
                                          "much time left, e.g. 30m (default: off)")
    args = p.parse_args(argv)

    # Our own file; infretis logs to the root logger (sim.log, worker logs).
    handler = logging.FileHandler("gutsrun.log")
    handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    for name in ("gutsrun", "gutsrun_infretis"):
        logging.getLogger(name).addHandler(handler)
        logging.getLogger(name).setLevel(logging.INFO)
        logging.getLogger(name).propagate = False
    check_infretis()

    server_dir, hq_bin = args.hq_server_dir, None

    def make_backend() -> Backend:
        return hq.HQBackend(server_dir, time_request=args.time_request, hq_bin=hq_bin)

    resources = {"cores": args.cores_per_task, "cwd": os.getcwd()}
    factory = make_factory(make_backend, args.spool, resources)

    own_hq = contextlib.ExitStack()
    try:
        if server_dir is None:
            cores = len(os.sched_getaffinity(0))
            if args.cores_per_task > cores:
                sys.exit(f"--cores-per-task is {args.cores_per_task}, but this machine "
                         f"lets the launcher use only {cores} cores")
            server_dir = str(Path("gutsrun-hq").resolve())
            hq_bin = str(Path(own_hq.enter_context(hq.server_and_worker(
                ROOT, Path(server_dir), "gutsrun", cores))) / "hq")
            log.info("own hq server in %s, worker with %d cores", server_dir, cores)
        # infretis says why it refuses on its "main" logger, which only has
        # a NullHandler until the run starts; show the reason on stderr.
        to_stderr = logging.StreamHandler()
        to_stderr.setLevel(logging.WARNING)
        logging.getLogger("main").addHandler(to_stderr)
        try:
            config = setup_config(args.input)
        finally:
            logging.getLogger("main").removeHandler(to_stderr)
        if config is None:
            log.warning("infretis refused to start; its reason is on stderr")
            return EXIT_REFUSED
        if "wmdrun" in config["runner"]:
            check_wmdrun(config["runner"]["wmdrun"], args.cores_per_task)
        stopped = scheduler(config, executor_factory=factory)
        # infretis gave the stop signals back to Python's default handlers,
        # and a second signal now would end the launcher mid-shutdown.
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, signal.SIG_IGN)
        if stopped is None:
            return EXIT_DONE
        text = (f"stopped by {signal.Signals(stopped).name}; moves not yet saved "
                "are redone after the restart")
        log.warning("%s", text)
        print(text, file=sys.stderr)
        return EXIT_STOPPED
    except Exception:
        log.exception("run failed")
        raise
    finally:
        for ex in EXECUTORS:
            ex.shutdown(wait=False, cancel_futures=True)
        own_hq.close()  # after the executors: they cancel their HQ jobs


if __name__ == "__main__":
    sys.exit(main())
