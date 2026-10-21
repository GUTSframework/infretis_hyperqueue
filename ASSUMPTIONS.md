# Assumptions

What this repo takes as given. If one turns out wrong, update this file
(and record what happened in the project's notes).

## Version pins

| What | Pin | Why |
|---|---|---|
| infretis | GUTS fork, branch `guts`, git `5df681e`: upstream `a42e039` (main, 2026-05-26; reports version 2026.1.1) plus six fixes meant for upstream | PyPI 2026.1.1 and `main` differ in move physics; infentory installs from `main` (the code and the measurements name one version). The fixes: restarts from the run's seed, atomic `restart.toml`, no busy spin, an import-order bug, seeded ASE velocities, and the `executor_factory` hook the launcher needs |
| inftools | git `ca33eae` (main, 2026-09-25) | `infinit`, WHAM analysis |
| Python | 3.12 | infretis pins numpy < 2; numpy 1.26.4 has no 3.13 wheel |
| numpy | 1.26.4 | from infretis (`^1.21.2`) |
| HQ | 0.26.2, binary and PyPI client | measured in `../hyperqueue_test`; client and server must match. Before moving to 0.27.x, rerun the 1024-task autoalloc regression check |
| nixpkgs | `151fa4e` (same lock as `../hyperqueue_test`) | |
| GROMACS | 2026.3 (nixpkgs; conda-forge on DAS-6) | production clusters use their own module |
| LAMMPS | 22Jul2025_update4 (nixpkgs), 29Aug2024 (conda-forge on DAS-6), REAXFF + QEQ enabled | water_dissociation; on DAS-6 the newest conda-forge build whose `lmp` runs there (below) |

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
  nothing gutsrun-specific. gutsrun-specific changes (graceful stop,
  GROMACS input handling) stay in the launcher.
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
  seen at once; `spool/CURRENT` relies on that.
- `/usr/bin/python3.12` exists on fs0 and the compute nodes (patch levels
  differ); the default `python3` is 3.6.8. The venv is built on fs0.
- Workers run a node-local copy of the venv, unpacked to `/local` at job
  start (0.15–0.3 s faster per task start than from `/home`, 2 s to unpack
  per job), so everything is called as `python -m`:
  entry-point scripts keep the `/home` venv in their shebang.
- Node-local disk at `/local` (NVMe); shared scratch at `/var/scratch/$USER`
  (40 GB quota); `/home` has a 5 GB quota.
- The soft limit on open files in a job is 1024 (hard: 131072).
- No GROMACS or LAMMPS from the site; users install software themselves.
  `scripts/das6/install-engines.sh` installs conda-forge's builds (CPU,
  no MPI) in `/var/scratch/$USER/miniforge3`: GROMACS 2026.3, the
  laptop's version, and LAMMPS 29Aug2024 as one exact build. DAS-6's
  glibc is older than 2.29; conda-forge's LAMMPS builds from 2026 need
  2.34 or 2.38 while declaring 2.17, so conda installs them and `lmp`
  fails to start. The pinned build needs no more than 2.14. Install:
  about 44k inodes and 1.2 GB. `/home` and `/var/scratch` have no file
  limit (`quota` shows 0). conda-forge's `gmx` picks AVX2_256 on AMD
  EPYC; production clusters use their own optimised builds, so DAS-6
  move times show plumbing, not production speed.
- Every `defq` node has at least one GPU (A2 to A100, per `sinfo`); GPU
  jobs are untested.

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
