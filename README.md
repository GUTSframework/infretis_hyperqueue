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
ASSUMPTIONS.md          version pins and cluster assumptions
bench/bare_run.py       run bare infretis on an example, record run.json
bench/move_times.py     per-move time statistics from sim.log
bench/cold_start.py     cold start of a fresh infretis worker process
examples/               prepared infentory cases (examples/README.md)
```

Planned (later steps): `src/gutsrun/` (generic: HQExecutor, backends, site
files), `src/gutsrun_infretis/` (adapter + launcher), `sites/`, `tests/`;
`runs/` holds outputs and is not tracked.

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
| 3 | `gutsrun` + test suite (gate C) | |
| 4 | infretis adapter + launcher | |
| 5 | HQ backend, local (gate A, D1) | |
| 6 | DAS-6 preflight | |
| 7 | DAS-6, one node (gate B) | |
| 8 | DAS-6, multi-node + drills | |
| 9 | Validation + metrics | |
| 10 | Snellius port (gate D) | |
