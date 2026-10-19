# infretis on HPC via HyperQueue

GUTS Fast Track: make [infretis](https://github.com/infretis/infretis)
usable on HPC clusters, with multi-node runs that survive walltime limits.
[HyperQueue](https://github.com/It4innovations/hyperqueue) (HQ) is the Task
Scheduler. Development on DAS-6; Snellius is the first real target, LUMI and
JSC later.

Plan, decisions and results live in the vault (`~/code/vault`, project
`guts`):

- plan: `projects/guts/plans/2026-10-15-fast-track-infretis-on-hpc-via-hyperqueue.md`
- architecture: `notes/2026-10-15-guts-architecture-diagram.md`, and
  `~/code/GUTS/GUTSnotes/design.md`

## Components

How this repo maps onto the GUTSnotes design:

| Component | Here |
|---|---|
| Trajectory Sampler + Result Manager | infretis's MC loop, unchanged |
| Session | the infretis run directory (`load/`, `restart.toml`, `infretis_data.txt`) |
| Task Scheduler | HQ server |
| Process Manager | HQ workers |
| Resource Manager | the self-chaining `sbatch` script (step 7) |
| Job Submitter | site files + `sbatch` |

```
Slurm job ──▶ HQ server + HQ workers ──▶ one HQ task per MC move
                    ▲
infretis MC loop ── HQExecutor (replaces ProcessPoolExecutor)
```

The HPC part is generic and knows nothing about MD; it is meant to grow into
the shared GUTS HPC packages. Only the adapter knows about infretis.

## Layout

```
flake.nix               dev shell: hq 0.26.2, Python 3.12, GROMACS, LAMMPS
requirements.txt        direct pins (infretis, inftools, hyperqueue client)
requirements-lock.txt   exact versions, installed by setup-venv.sh
scripts/get-hq.sh       pinned hq binary into ./bin, for clusters without Nix
scripts/setup-venv.sh   .venv from the lock file
scripts/sync.sh         rsync the repo to a login node, pull runs/<site>/ back
scripts/wait-jobs.sh    poll a login node's squeue until jobs finish, optionally pull
scripts/das6/           DAS-6 probes: login node (read-only), one-node and two-node jobs
sites/das6.toml         DAS-6 site knobs for the sbatch (TBD fields filled by step 6)
ASSUMPTIONS.md          version pins and cluster assumptions
PROVENANCE.md           where each file comes from, and under which terms
bench/bare_run.py       run bare infretis on an example, record run.json
bench/move_times.py     per-move time statistics from sim.log
bench/cold_start.py     cold start of a fresh infretis worker process (N at once)
bench/fs_latency.py     cross-node file visibility on a shared FS (ping-pong)
bench/hq_dispatch.py    HQ per-task round trip (program vs Python function task)
bench/executor_roundtrip.py  round trip of a trivial call through gutsrun
bench/step5.sh          each case through bare / gutsrun local / gutsrun HQ, back to back
bench/timings_summary.py  per-phase medians from gutsrun-timings.jsonl
examples/               prepared infentory cases (examples/README.md)
src/gutsrun/            generic: TaskExecutor + backends (no MD imports)
src/gutsrun_infretis/   infretis adapter + launcher (the only infretis-aware code)
tests/                  pytest suite (gate C; adapter tests need infretis)
```

`runs/<site>/` holds outputs and is not tracked.

## gutsrun

`gutsrun.TaskExecutor` is a `concurrent.futures.Executor`: each `submit`
runs as one backend task, a fresh `python -m gutsrun.task` process.

```python
from gutsrun import TaskExecutor
from gutsrun.backends.hq import HQBackend        # or backends.local.LocalBackend

ex = TaskExecutor(HQBackend(server_dir), "runs/x/spool",
                  initializer=init, initargs=(cfg,),        # runs before every call
                  resource_dict={"cores": 2})               # default per call
fut = ex.submit(fn, arg, resource_dict={"cores": 4, "cwd": "...", "env": {...}})
```

- **Spool directory**, one subdirectory per executor instance (the
  *epoch*): `in/` holds the pickled calls, `out/` the results
  (`<tid>.<instance>.pkl`, temp + rename), `log/` stdout/stderr. `CURRENT`
  names the live epoch. A new executor takes over the spool and deletes
  older epochs. Tasks of an older epoch neither start nor publish.
- **Completion** from result files (listed every 20 ms). Failures from the
  backend every 2 s (HQ: one batched `get_failed_tasks` call). The first
  result for a task wins; later ones are deleted. Files of finished
  tasks are deleted; failed tasks keep their logs.
- **HQ tasks are `program()` tasks**, not Python function tasks. They
  carry only two paths (payload rule), and need no HQ Python environment.
  HQ reruns a task after a worker crash (`crash_limit`, default 2). The
  rerun gets a new instance number.
- **Like `ProcessPoolExecutor` with spawn:** calls and results must pickle
  by reference; the caller's `sys.path` is sent along. Exceptions arrive
  with the remote traceback as `__cause__`. Unlike it, the initializer
  runs before every call.
- Both backends set `OMP_NUM_THREADS` to the task's cores (HQ does so for
  every task).

## Running infretis through gutsrun

In an infretis run directory, instead of `infretisrun -i infretis.toml`:

```bash
PYTHONPATH=<repo>/src python -m gutsrun_infretis.launcher -i infretis.toml --backend local
HQ_SERVER_DIR=... PYTHONPATH=<repo>/src python -m gutsrun_infretis.launcher \
    -i restart.toml --backend hq --cores-per-task 2
```

or `bench/bare_run.py ... --runner gutsrun [--backend hq]`. The launcher
swaps in `TaskExecutor` (infretis unforked) and fixes, for this run only:
atomic `restart.toml` + backup at start, graceful SIGTERM/SIGINT (waits
out `treat_output`), no busy spin in the parent (92 % → 2 % of a core),
`wmdrun` pinning flags stripped under HQ, `-ntomp` checked against
`--cores-per-task`. At start it checks the source of the four infretis
functions it copies or relies on against the tested commit (`a42e039`);
other infretis versions run if those are unchanged, otherwise it stops
(`--allow-other-infretis` overrides). Exit codes: 0 done, 3 stopped by a
signal, 4 infretis refused to start. Logs: `gutsrun.log`; moves log to
`worker{pin}.log` as before; per-move timings go to
`gutsrun-timings.jsonl` (`bench/timings_summary.py` summarises them).

## Tests

```bash
nix develop --command .venv/bin/python -m pytest          # all but slow (~40 s)
nix develop --command .venv/bin/python -m pytest -m slow  # LAMMPS exact check (~1 min)
.venv/bin/python -m pytest -m "not hq"                    # without the dev shell: gutsrun only
```

The HQ tests start `hq server` and `hq worker` processes, which need TCP
on localhost. They cannot run inside Claude Code's sandbox.

## Getting started (laptop)

```bash
nix develop
scripts/setup-venv.sh
.venv/bin/python -c 'import infretis, hyperqueue'
```

## Status

| Step | | State |
|---|---|---|
| 1 | Repo setup | done |
| 2 | Bare infretis baseline + MD move times | done on the laptop (GROMACS at real size still open) |
| 3 | `gutsrun` + test suite (gate C) | done on the laptop; gate C passed (24/24) |
| 4 | infretis adapter + launcher | done on the laptop; exact check passes (LAMMPS) |
| 5 | HQ backend, local (gate A, D1) | done on the laptop; gate A passed; D1 numbers in the vault |
| 6 | DAS-6 preflight | done, except the MD engines (they come the researchers' way) |
| 7 | DAS-6, one node (gate B) | |
| 8 | DAS-6, multi-node + drills | |
| 9 | Validation + metrics | |
| 10 | Snellius port (gate D) | |
