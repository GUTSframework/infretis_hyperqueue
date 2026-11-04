# Assumptions

What this repo takes as given. If one turns out wrong, update this file
(and record what happened in the project's notes).

## Version pins

| What | Pin | Used for | Why this version |
|---|---|---|---|
| infretis | GUTS fork, branch `guts`, git `2502487`: upstream `a42e039` (main, 2026-05-26; reports version 2026.1.1) plus twelve changes meant for upstream | the simulation | PyPI 2026.1.1 and `main` differ in move physics; infentory installs from `main` (the code and the measurements name one version). The changes: restarts from the run's seed, atomic `restart.toml`, no busy spin, an import-order bug, seeded ASE velocities, the `executor_factory` hook, workers that stop their MD program on SIGTERM, worker logs by pin without shared state, a graceful stop on SIGTERM/SIGINT, GROMACS's shared inputs written once at setup, a finished run refused before setup changes it, and no unread task failures at a stop |
| inftools | git `ca33eae` (main, 2026-09-25) | `inft infinit` makes initial paths (`puckering`); `inft wham` is the usual analysis of a run | `main` at the time; nothing newer needed |
| Python | 3.12 | everything | numpy 1.26.4 has no wheels for 3.13 |
| numpy | 1.26.4 | infretis | infretis pins numpy below 2 (`^1.21.2`) |
| HQ | 0.26.2, binary only (driven through its CLI) | tasks, workers, completion events | measured in `../hyperqueue_test`; open jobs and `hq journal` need 0.21 or later. Before moving to 0.27.x, rerun the 1024-task autoalloc regression check, and check the event format of `hq journal stream`, which is HQ's own |
| nixpkgs | `151fa4e` (`flake.lock`) | the dev shell: Python, GROMACS, LAMMPS, ruff, pyright, jq | none in particular; the lock keeps the dev shell reproducible. Updating it changes the GROMACS and LAMMPS versions below |
| GROMACS | 2026.3 (nixpkgs; conda-forge on DAS-6) | `water_boiling`, `puckering` | nixpkgs' version, matched on DAS-6; production clusters use their own module |
| LAMMPS | 22Jul2025_update4 (nixpkgs), 29Aug2024 (conda-forge on DAS-6), REAXFF + QEQ enabled | `water_dissociation` (needs REAXFF and QEQ) | on DAS-6 the newest conda-forge build whose `lmp` runs there (below) |

Exact Python package versions: `requirements-lock.txt`.

## Runtime design

- MD engines come from the cluster, usually as a site module whose name
  differs per cluster (reported); a site file's `[environment] setup`
  loads it. GUTS does not ship engines.
- One HQ task per MC move, each a fresh Python process: starting one
  costs about 1 s on DAS-6 (mostly imports). Production moves take minutes
  to hours, up to about a day (reported), so this is a few percent at most
  and long-running "warm" workers per pin are not planned.
- `restart.toml` is the only restart authority: no HQ `--journal` for chained
  runs, a fresh `HQ_SERVER_DIR` per Slurm job.
- infretis comes from the GUTS fork: upstream plus fixes offered upstream,
  nothing gutsrun-specific. The launcher adds only checks for this run
  (`wmdrun` under HQ, the infretis it needs).
- Tasks carry paths and indices, never data.
- One node per MD move; HQ multi-node tasks are experimental.

## DAS-6 (development)

What the code and job scripts rely on, measured on DAS-6 or reported by
its admins.

- We have an account. The login node is reachable only from a university
  network or through eduVPN.
- Partition `defq` (32 nodes of 24 cores); Slurm commands work without
  `module load prun`. Never pass `--mem`: Slurm has no memory configured.
  Accounting is disabled (`sacct` does not work): a job's own log is the
  only record of how it ended.
- Jobs are limited to 15 min walltime 08:00–20:00; longer runs only at night
  or weekends, or with permission from das-sysadm@cs.vu.nl. Our own rule: at
  most 2 nodes at a time during the day.
- Program execution must happen on compute nodes, never on the head node.
- `/home` (NFS) is shared between fs0 and the compute nodes. Anything that
  waits for another node's file lists the directory, never retries
  `open()`/`exists()` on the name: a node that looked a name up before it
  existed can keep missing it for 30 s or more. A file replaced in place is
  seen at once.
- `/usr/bin/python3.12` exists on fs0 and the compute nodes (patch levels
  differ); the default `python3` is 3.6.8. The venv is built on fs0.
- Tasks run Python from the venv on `/home`. A copy unpacked to `/local`
  per job would start each task 0.15–0.3 s faster, for 2 s of unpacking
  per job; the Fast Track does without it, for simplicity.
- Node-local disk at `/local` (NVMe); shared scratch at `/var/scratch/$USER`
  (40 GB quota); `/home` has a 5 GB quota.
- The soft limit on open files in a job is 1024 (hard: 131072).
- No GROMACS or LAMMPS from the site; users install software themselves.
  `scripts/das6/install-engines.sh` installs conda-forge's builds (CPU,
  no MPI) in `/var/scratch/$USER/miniforge3`: GROMACS 2026.3, the
  laptop's version, and LAMMPS 29Aug2024 as one exact build. DAS-6's
  glibc is older than 2.29; conda-forge's LAMMPS builds from 2026 need
  2.34 or 2.38. Those built since conda-forge/lammps-feedstock#276
  declare 2.38, and conda skips them; earlier ones declare 2.17, so conda
  installs them and `lmp` fails to start. The pinned build needs no more
  than 2.14. Install:
  about 44k inodes and 1.2 GB. `/home` and `/var/scratch` have no file
  limit (`quota` shows 0). conda-forge's `gmx` picks AVX2_256 on AMD
  EPYC; production clusters use their own optimised builds, so DAS-6
  move times show plumbing, not production speed.
- Every `defq` node has 1 to 4 NVIDIA GPUs of one type (A2, A4000, A5000,
  A6000 or A100), named as a node feature, so `constraint = "A4000"` in
  `gutsrun.toml` picks a type (`sinfo -p defq -N -o "%N %G %f"`). With
  `--exclusive`, a job sees the node's GPUs without `--gres`, and the HQ
  worker detects them; with `--gres=gpu:N`, Slurm also sets
  `ROCR_VISIBLE_DEVICES`, and HQ then reports AMD GPUs that are not there
  (harmless: moves ask for `gpus/nvidia`). `nvidia-smi` is not on `PATH`
  on the nodes, and `libcuda.so.1` is not in the linker cache: it is in
  `/cm/local/apps/cuda-driver/libs/current/lib64` (driver 595.91.07, CUDA
  13.2), which the `cuda*/toolkit` modules also put on `LD_LIBRARY_PATH`.
  `scripts/das6/install-gromacs-gpu.sh` installs conda-forge's CUDA
  GROMACS 2026.3 (CUDA runtime 12.9), which runs with it.

## Snellius (first target)

- SURF documents HyperQueue: a module `HyperQueue/0.19.0` (stack `2023`),
  the server running persistently on a login node, and workers in Slurm
  jobs (`srun --overlap hq worker start --manager slurm`). We pin 0.26.2
  (client and server must match), so we bring our own binary
  (`scripts/get-hq.sh`) and run the server inside each job; the module is
  not used.

## Examples (infentory, local clone `0640581`)

- `water_boiling`: GROMACS, CPU, initial paths included. The shipped
  `wmdrun` list must be rewritten (one entry per worker, no overlapping
  pinning).
- `water_dissociation/lammps`: tutorial; starts from a plain MD run.
- `puckering`: overhead stress test; initial paths from `infinit`.
- `2d_potential`: ASE toy, plumbing only.
