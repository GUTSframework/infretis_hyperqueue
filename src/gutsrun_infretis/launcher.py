"""Run infretis with gutsrun instead of ProcessPoolExecutor.

    python -m gutsrun_infretis.launcher -i infretis.toml --backend local
    python -m gutsrun_infretis.launcher -i restart.toml --backend hq --cores-per-task 2

Run it in the infretis run directory, like `infretisrun`. It needs the
infretis pinned in requirements.txt (the GUTS fork, branch `guts`): its
`scheduler` takes an `executor_factory`, through which the launcher passes
a factory that returns a gutsrun TaskExecutor; its worker initializer
needs no shared state, its moves log to `worker{pin}.log`, its workers
leave the shared GROMACS inputs alone and stop their MD program on
SIGTERM, and it stops gracefully on SIGTERM and SIGINT. What the launcher
adds, for this run only:

- `restart.toml` is backed up and parse-checked at start.
- Under HQ, a `wmdrun` with `-pin on|auto`/`-pinoffset`/`-pinstride` is refused
  (HQ places tasks, so fixed offsets would put moves on the same cores),
  and so is one whose `-ntomp` differs from --cores-per-task (HQ sets
  OMP_NUM_THREADS to it; GROMACS aborts on a mismatch).
- A stop signal that comes before infretis's scheduler takes over the
  signals also stops the run. Once the scheduler has returned, and after
  the first stop, stop signals are ignored (`scancel` signals every
  process of the job, and the link passes on a second copy).

Exit codes: 0 = all steps done, 3 = stopped by a signal (restart from
restart.toml), 4 = infretis refused to start (its reason on stderr and in
gutsrun.log), other = error.
"""

import argparse
import inspect
import json
import logging
import os
import re
import shutil
import signal
import sys
from collections.abc import Callable
from importlib.metadata import distribution
from pathlib import Path
from types import FrameType
from typing import Any

import infretis.asyncrunner
import infretis.scheduler
import tomli
from infretis.classes.engines.factory import create_engines
from infretis.scheduler import scheduler
from infretis.setup import setup_config

from gutsrun import TaskExecutor
from gutsrun.backends import Backend

TESTED_COMMIT = "bf813ee"  # the fork's commit in requirements.txt
EXIT_DONE, EXIT_STOPPED, EXIT_REFUSED = 0, 3, 4
STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)

log = logging.getLogger("gutsrun_infretis")


class Stop(BaseException):
    """Raised in the main thread to stop the run, outside infretis's scheduler."""


# --- the executor ---------------------------------------------------------------


EXECUTORS: list[TaskExecutor] = []


def make_factory(make_backend: Callable[[int | None], Backend], spool_dir: str,
                 resource_dict: dict[str, Any]) -> Callable[..., TaskExecutor]:
    """infretis's `executor_factory`: takes ProcessPoolExecutor's arguments
    and returns a TaskExecutor, which runs infretis's initializer before
    every call (each in a fresh task process)."""

    def factory(max_workers: int | None = None, initializer: Callable[..., object] | None = None,
                initargs: tuple[Any, ...] = (), mp_context: object = None) -> TaskExecutor:
        ex = TaskExecutor(
            make_backend(max_workers), spool_dir, max_workers=max_workers,
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


def missing_features() -> list[str]:
    """What the launcher relies on and the installed infretis lacks."""
    checks = {
        "the executor_factory hook":
            "executor_factory" in inspect.signature(scheduler).parameters,
        "the graceful stop": hasattr(infretis.scheduler, "StopRequest"),
        "GROMACS inputs written once at setup":
            "write_inputs" in inspect.signature(create_engines).parameters,
        "workers that stop their MD program on SIGTERM":
            hasattr(infretis.asyncrunner, "WorkerTerminated"),
    }
    return [name for name, present in checks.items() if not present]


def check_infretis() -> None:
    """Exit if infretis lacks what the launcher relies on (missing_features)."""
    commit = infretis_commit()
    log.info("infretis commit %s (tested: %s)", commit, TESTED_COMMIT)
    if missing := missing_features():
        sys.exit(f"infretis {commit or '(unknown version)'} lacks {', '.join(missing)}. "
                 f"Install the infretis pinned in requirements.txt ({TESTED_COMMIT}); "
                 "in an existing .venv, pip keeps the old one because the version "
                 "number is the same: add --force-reinstall.")


def stop_once(signum: int, frame: FrameType | None) -> None:
    """Before infretis's scheduler runs: stop, and ignore further stop
    signals to the exit."""
    ignore_stop_signals()
    raise Stop(signal.Signals(signum).name)


def ignore_stop_signals() -> None:
    """Ignore stop signals to the exit (also while Python shuts down, which
    restores default handlers, not SIG_IGN)."""
    for sig in STOP_SIGNALS:
        signal.signal(sig, signal.SIG_IGN)


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


def check_restart_file(inp: str) -> None:
    """Back up restart.toml and make sure it parses: a truncated file would
    otherwise only show when infretis fails to restart."""
    path = Path("restart.toml")
    if not path.exists():
        return
    try:
        cstep = tomli.loads(path.read_text())["current"]["cstep"]
    except Exception as e:
        sys.exit(f"restart.toml does not parse ({e}); restore it from "
                 "restart.toml.bak.* before restarting")
    shutil.copy2(path, f"restart.toml.bak.{cstep}")
    if inp != "restart.toml":
        log.warning("restart.toml exists but input is %s; infretis will refuse", inp)


class Collect(logging.Handler):
    """Keeps the messages of the records it gets."""

    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def setup(inp: str) -> tuple[dict[str, Any] | None, str]:
    """infretis's `setup_config(inp)`, and the reason it logged if it
    returned None (it does not run, and says why on its "main" logger)."""
    reasons = Collect()
    main = logging.getLogger("main")
    main.addHandler(reasons)
    try:
        config = setup_config(inp)
    finally:
        main.removeHandler(reasons)
    return config, "; ".join(reasons.messages) or "no reason given"


def done(inp: str) -> bool:
    """Whether the run in `inp` has done all its steps."""
    try:
        config = tomli.loads(Path(inp).read_text())
        return config["current"]["cstep"] >= config["simulation"]["steps"]
    except (OSError, KeyError, tomli.TOMLDecodeError):
        return False


# --- main -------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("-i", "--input", required=True)
    p.add_argument("--backend", choices=["local", "hq"], default="local")
    p.add_argument("--hq-server-dir", default=os.environ.get("HQ_SERVER_DIR"))
    p.add_argument("--spool", default="gutsrun-spool")
    p.add_argument("--cores-per-task", type=int, default=1)
    p.add_argument("--crash-limit", type=int, default=2)
    args = p.parse_args(argv)

    # Our own file; infretis logs to the root logger (sim.log, worker logs).
    handler = logging.FileHandler("gutsrun.log")
    handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    for name in ("gutsrun", "gutsrun_infretis"):
        logging.getLogger(name).addHandler(handler)
        logging.getLogger(name).setLevel(logging.INFO)
        logging.getLogger(name).propagate = False
    check_infretis()
    check_restart_file(args.input)

    def local_backend(max_workers: int | None) -> Backend:
        from gutsrun.backends.local import LocalBackend

        return LocalBackend(max_workers=max_workers)

    def hq_backend(max_workers: int | None) -> Backend:
        from gutsrun.backends.hq import HQBackend

        return HQBackend(args.hq_server_dir, crash_limit=args.crash_limit)

    make_backend = local_backend if args.backend == "local" else hq_backend

    resources = {"cores": args.cores_per_task, "cwd": os.getcwd()}
    factory = make_factory(make_backend, args.spool, resources)
    for sig in STOP_SIGNALS:
        signal.signal(sig, stop_once)

    try:
        config, reason = setup(args.input)
        if config is None:
            if done(args.input):
                log.info("nothing to do: %s", reason)
                print(f"nothing to do: {reason}", file=sys.stderr)
                return EXIT_DONE
            log.warning("infretis refused to start: %s", reason)
            print(f"infretis refused to start: {reason}", file=sys.stderr)
            return EXIT_REFUSED
        if args.backend == "hq" and "wmdrun" in config["runner"]:
            check_wmdrun(config["runner"]["wmdrun"], args.cores_per_task)
        stopped = scheduler(config, executor_factory=factory)
        ignore_stop_signals()
        if stopped is None:
            return EXIT_DONE
        raise Stop(signal.Signals(stopped).name)
    except Stop as s:
        text = f"stopped by {s}; moves not yet saved are redone after the restart"
        log.warning("%s", text)
        print(text, file=sys.stderr)
        return EXIT_STOPPED
    except Exception:
        log.exception("run failed")
        raise
    finally:
        ignore_stop_signals()  # a late signal must not interrupt the shutdown
        for ex in EXECUTORS:
            ex.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    sys.exit(main())
