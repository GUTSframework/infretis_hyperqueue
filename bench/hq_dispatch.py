"""Measure HyperQueue's per-task round trip on a local cluster.

For each kind of task, submit one task, wait until it has finished, repeat
(sequential, like one infretis pin dispatching moves one after another).
"Finished" is detected the way the executor will: the task writes a marker
file at its very end and we poll for it every 2 ms. We do not time
`Client.wait_for_jobs`: in hyperqueue 0.26.2 it polls the server once per
second (`crates/pyhq/src/client/job.rs:324`), so it adds up to 1 s per call.

Kinds:
- program:  `true` followed by writing the marker (HQ's own overhead + a shell)
- function: an empty Python function task (adds a fresh Python + cloudpickle)
- infretis: a Python function task doing what `worker_initializer` does for
            RUNDIR (the per-move cold start under HQ)

Uses `hyperqueue.cluster.LocalCluster` (server + one worker in-process), so
no `hq` binary is needed. Python tasks run with this interpreter
(`sys.executable`); HQ's default is whatever `python3` is on PATH.

Usage (inside `nix develop`):

    .venv/bin/python bench/hq_dispatch.py --rundir runs/puck-w4 --repeat 20
"""

import argparse
import json
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

from hyperqueue import Job
from hyperqueue.cluster import LocalCluster, WorkerConfig
from hyperqueue.task.function import PythonEnv


def noop(marker):
    Path(marker).touch()


def infretis_init(rundir, inp, marker):
    # Mirrors bench/cold_start.py: infretis.setup first (import-order bug).
    import importlib.util  # noqa: F401
    os.chdir(rundir)
    from infretis.setup import setup_config
    from infretis.classes.engines.factory import create_engines
    from infretis.classes.orderparameter import create_orderparameters

    config = setup_config(inp)
    engines, _ = create_engines(config)
    create_orderparameters(engines, config)
    Path(marker).touch()


def round_trips(client, make_job, repeat, scratch):
    times = []
    for i in range(repeat):
        marker = Path(scratch) / f"done-{time.monotonic_ns()}-{i}"
        job = make_job(str(marker))
        start = time.perf_counter()
        submitted = client.submit(job)
        while not marker.exists():
            time.sleep(0.002)
        times.append(time.perf_counter() - start)
        client.wait_for_jobs([submitted])  # not timed; raises if the task failed
        client.forget(submitted)
    return times


def stats(times):
    s = sorted(times)
    return {
        "n": len(s),
        "median_s": round(statistics.median(s), 4),
        "min_s": round(s[0], 4),
        "max_s": round(s[-1], 4),
        # The first task may include worker warm-up.
        "median_after_first_s": round(statistics.median(times[1:]), 4) if len(times) > 1 else None,
    }


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--rundir", type=Path, required=True)
    p.add_argument("--repeat", type=int, default=20)
    p.add_argument("--cores", type=int, default=1)
    args = p.parse_args()
    rundir = args.rundir.resolve()
    inp = "restart.toml" if (rundir / "restart.toml").exists() else "infretis.toml"

    results = {}
    with tempfile.TemporaryDirectory(prefix="hq-dispatch-") as scratch:
        server_dir = os.path.join(scratch, "server")
        # server_dir must be a str: the FFI rejects pathlib.Path.
        with LocalCluster(server_dir=server_dir,
                          worker_config=WorkerConfig(cores=args.cores)) as cluster:
            client = cluster.client(python_env=PythonEnv(python_bin=sys.executable))
            # Tasks run in the scratch dir and discard stdout/stderr, so
            # nothing lands in the current directory.
            common = {"cwd": scratch, "stdout": None, "stderr": None}

            def program_job(marker):
                job = Job()
                job.program(["sh", "-c", f'true && touch "{marker}"'], **common)
                return job

            def function_job(marker):
                job = Job()
                job.function(noop, args=(marker,), **common)
                return job

            def infretis_job(marker):
                job = Job()
                job.function(infretis_init, args=(str(rundir), inp, marker), **common)
                return job

            for name, make in (("program", program_job),
                               ("function", function_job),
                               ("infretis", infretis_job)):
                results[name] = stats(round_trips(client, make, args.repeat, scratch))
                print(name, results[name], file=sys.stderr, flush=True)

    print(json.dumps({"rundir": str(rundir), "cores": args.cores, **results}, indent=2))


if __name__ == "__main__":
    main()
