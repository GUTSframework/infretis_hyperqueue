"""Run infretis with gutsrun instead of ProcessPoolExecutor, unforked.

    python -m gutsrun_infretis.launcher -i infretis.toml --backend local
    python -m gutsrun_infretis.launcher -i restart.toml --backend hq --cores-per-task 2

Run it in the infretis run directory, like `infretisrun`. What it changes,
for this run only (infretis itself stays unmodified):

- `concurrent.futures.ProcessPoolExecutor` is replaced by a factory that
  returns a gutsrun TaskExecutor. infretis looks it up at call time, so
  no fork of infretis is needed.
- `restart.toml` is written via temp + rename, and backed up and
  parse-checked at start: infretis writes it in place, so a kill during
  the write can leave a truncated file, the only one to restart from.
- SIGTERM/SIGINT wait out a running `treat_output` (which updates the
  paths and restart.toml together), then stop: in-flight moves are
  cancelled and redone after restart from their `locked` entry.
- The parent's `as_completed` loop sleeps instead of spinning (infretis's
  loop keeps a core 92 % busy).
- Under HQ, `-pin`/`-pinoffset`/`-pinstride` are removed from `wmdrun`
  (HQ places tasks), and `-ntomp` must equal --cores-per-task (HQ sets
  OMP_NUM_THREADS to it; GROMACS aborts on a mismatch).
- The infretis functions it copies or relies on are checked at start
  against their source at the tested commit (`KNOWN_SOURCES`); a
  mismatch stops the run unless --allow-other-infretis is given. Other
  infretis commits with the same functions run fine.
- `gutsrun-timings.jsonl`: per-move timings (see gutsrun.executor).

Exit codes: 0 = all steps done, 3 = stopped by a signal (restart from
restart.toml), 4 = infretis refused to start (see sim.log: nothing left
to do, or an active path's files are missing), other = error.

Parts of this file are adapted from infretis (https://github.com/infretis/infretis,
commit a42e039; see PROVENANCE.md), which carries this licence:

    MIT License

    Copyright (c) 2022 infretis

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
"""

import argparse
import concurrent.futures
import hashlib
import inspect
import json
import logging
import os
import re
import shutil
import signal
import sys
import time
from importlib.metadata import distribution
from pathlib import Path

import tomli
import tomli_w

import gutsrun_infretis.worker  # noqa: F401  (imports infretis.setup first)
from gutsrun import TaskExecutor
from infretis.asyncrunner import future_list
from infretis.classes.repex import REPEX_state
from infretis.setup import setup_config
from infretis.scheduler import scheduler

TESTED_COMMIT = "a42e039ba49eaf3c2b4021d6298c15e55f389735"
# sha256 (first 16 hex) of inspect.getsource() at TESTED_COMMIT. The launcher
# copies the first three and replaces the factory call in the fourth.
KNOWN_SOURCES = {
    "REPEX_state.write_toml": "a3dbdbcc527c9c9d",
    "future_list.as_completed": "987641bc2f36a670",
    "worker_initializer": "388c6696d0a234f3",
    "aiorunner.__init__": "8f6009038fb50d42",
}
# Test hook: seconds to hold inside treat_output after touching
# `treat_output.entered`, so a test can signal at exactly that point.
HOLD_ENV = "GUTSRUN_INFRETIS_TEST_HOLD_TREAT"
EXIT_DONE, EXIT_STOPPED, EXIT_REFUSED = 0, 3, 4

log = logging.getLogger("gutsrun_infretis")


class Stop(BaseException):
    """Raised in the main thread to stop the run between moves."""


# --- the executor ---------------------------------------------------------------


class InfretisExecutor(TaskExecutor):
    """Wraps each call in `worker.run_move` (per-pin log, import order)."""

    def submit(self, fn, /, *args, **kwargs):
        if args or kwargs:
            raise TypeError("infretis submits partial(run_md, md_item) only")
        return super().submit(gutsrun_infretis.worker.run_move, fn)


EXECUTORS: list[InfretisExecutor] = []


def make_factory(make_backend, spool_dir, resource_dict, timings_path=None):
    def factory(max_workers=None, initializer=None, initargs=(), mp_context=None):
        # infretis passes (worker_initializer, (counter, config)); keep the config.
        config = initargs[1]
        ex = InfretisExecutor(
            make_backend(max_workers), spool_dir, max_workers=max_workers,
            initializer=gutsrun_infretis.worker.init, initargs=(config,),
            resource_dict=resource_dict, timings_path=timings_path)
        EXECUTORS.append(ex)
        log.info("gutsrun executor: spool %s, epoch %s", ex.spool, ex.epoch)
        return ex
    return factory


# --- patches ----------------------------------------------------------------------


def infretis_commit():
    try:
        info = json.loads(distribution("infretis").read_text("direct_url.json") or "{}")
    except Exception:
        return None
    return info.get("vcs_info", {}).get("commit_id")


def changed_sources() -> list[str]:
    """Names in KNOWN_SOURCES whose infretis source differs from the tested one."""
    from infretis.asyncrunner import aiorunner, worker_initializer

    objects = {
        "REPEX_state.write_toml": REPEX_state.write_toml,
        "future_list.as_completed": future_list.as_completed,
        "worker_initializer": worker_initializer,
        "aiorunner.__init__": aiorunner.__init__,
    }
    return [name for name, obj in objects.items()
            if hashlib.sha256(inspect.getsource(obj).encode()).hexdigest()[:16]
            != KNOWN_SOURCES[name]]


def check_infretis(allow_other: bool):
    commit = infretis_commit()
    changed = changed_sources()
    log.info("infretis commit %s (tested: %s); changed functions: %s",
             commit, TESTED_COMMIT, changed or "none")
    if changed and not allow_other:
        sys.exit(f"infretis {commit or '(unknown version)'} differs from the tested "
                 f"{TESTED_COMMIT[:7]} in functions the launcher copies or relies on: "
                 f"{', '.join(changed)}. Check src/gutsrun_infretis/launcher.py against "
                 "them, or pass --allow-other-infretis to run anyway.")


def write_toml_atomic(self):
    """REPEX_state.write_toml at a42e039 (repex.py:764), with temp + rename."""
    self.config["current"]["active"] = self.live_paths()
    locked_ep = []
    for tup in self.locked:
        locked_ep.append(([int(tup0 + self._offset) for tup0 in tup[0]], tup[1]))
    self.config["current"]["locked"] = locked_ep
    self.config["current"]["rng_state"] = self.rgen.bit_generator.state
    self.config["current"]["frac"] = {}
    for key in sorted(self.traj_data.keys()):
        fracs = [str(i) for i in self.traj_data[key]["frac"]]
        self.config["current"]["frac"][str(key)] = fracs
    with open("./.restart.toml.tmp", "wb") as f:
        tomli_w.dump(self.config, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace("./.restart.toml.tmp", "./restart.toml")


def as_completed_sleeping(self, interval=0.005):
    """future_list.as_completed (asyncrunner.py:243) without the busy spin."""
    while self._futures:
        for fut in list(self._futures):
            if fut.done():
                self._futures.remove(fut)
                return fut
        time.sleep(interval)
    return None


class GracefulStop:
    """Defers SIGTERM/SIGINT while treat_output runs, then stops."""

    def __init__(self):
        self.in_treat = False
        self.requested = None

    def install(self):
        original = REPEX_state.treat_output
        guard = self

        hold = float(os.environ.get(HOLD_ENV, "0"))

        def treat_output(state, md_items):
            guard.in_treat = True
            if hold:
                Path("treat_output.entered").touch()
                time.sleep(hold)
            try:
                return original(state, md_items)
            finally:
                guard.in_treat = False
                if guard.requested is not None:
                    raise Stop(guard.requested)

        REPEX_state.treat_output = treat_output
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self.handle)

    def handle(self, signum, frame):
        self.requested = signal.Signals(signum).name
        log.warning("%s received; stopping%s", self.requested,
                    " after treat_output" if self.in_treat else "")
        if not self.in_treat:
            raise Stop(self.requested)


def strip_pinning(wmdrun: list, cores: int) -> list:
    """Remove GROMACS pinning flags; check -ntomp against the task's cores."""
    out = []
    for cmd in wmdrun:
        if not isinstance(cmd, str):
            out.append(cmd)  # ASE examples use placeholders
            continue
        new = re.sub(r"\s-pin\s+\S+|\s-pinoffset\s+\S+|\s-pinstride\s+\S+", "", cmd)
        m = re.search(r"-ntomp\s+(\d+)", new)
        if m and int(m.group(1)) != cores:
            sys.exit(f"wmdrun has -ntomp {m.group(1)} but --cores-per-task is {cores}; "
                     "HQ sets OMP_NUM_THREADS to the task's cores and GROMACS aborts "
                     "when they differ")
        out.append(new)
    return out


def check_restart_file(inp: str):
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


# --- main -------------------------------------------------------------------------


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("-i", "--input", required=True)
    p.add_argument("--backend", choices=["local", "hq"], default="local")
    p.add_argument("--hq-server-dir", default=os.environ.get("HQ_SERVER_DIR"))
    p.add_argument("--spool", default="gutsrun-spool")
    p.add_argument("--cores-per-task", type=int, default=1)
    p.add_argument("--crash-limit", type=int, default=2)
    p.add_argument("--allow-other-infretis", action="store_true")
    args = p.parse_args(argv)

    # Our own file; infretis logs to the root logger (sim.log, worker logs).
    handler = logging.FileHandler("gutsrun.log")
    handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    for name in ("gutsrun", "gutsrun_infretis"):
        logging.getLogger(name).addHandler(handler)
        logging.getLogger(name).setLevel(logging.INFO)
        logging.getLogger(name).propagate = False
    check_infretis(args.allow_other_infretis)
    check_restart_file(args.input)

    if args.backend == "local":
        from gutsrun.backends.local import LocalBackend

        def make_backend(max_workers):
            return LocalBackend(max_workers=max_workers)
    else:
        from gutsrun.backends.hq import HQBackend

        def make_backend(max_workers):
            return HQBackend(args.hq_server_dir, crash_limit=args.crash_limit)

    resources = {"cores": args.cores_per_task, "cwd": os.getcwd()}
    concurrent.futures.ProcessPoolExecutor = make_factory(
        make_backend, args.spool, resources, timings_path="gutsrun-timings.jsonl")
    REPEX_state.write_toml = write_toml_atomic
    future_list.as_completed = as_completed_sleeping
    stop = GracefulStop()
    stop.install()

    try:
        config = setup_config(args.input)
        if config is None:
            log.warning("infretis refused to start (setup_config returned None)")
            return EXIT_REFUSED
        if args.backend == "hq" and "wmdrun" in config["runner"]:
            config["runner"]["wmdrun"] = strip_pinning(config["runner"]["wmdrun"],
                                                       args.cores_per_task)
        scheduler(config)
        return EXIT_DONE
    except Stop as s:
        log.warning("stopped by %s", s)
        return EXIT_STOPPED
    finally:
        for ex in EXECUTORS:
            ex.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    sys.exit(main())
