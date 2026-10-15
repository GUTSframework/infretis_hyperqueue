# Assumptions

What this repo takes as given. If one turns out wrong, record it in the vault
(`projects/guts/log/`) and update this file.

## Version pins

| What | Pin | Why |
|---|---|---|
| infretis | git `a42e039` (main, 2026-05-26; reports version 2026.1.1) | PyPI 2026.1.1 and `main` differ in move physics; infentory installs from `main` (review S11) |
| inftools | git `ca33eae` (main, 2026-09-25) | `infinit`, WHAM analysis |
| Python | 3.12 | infretis pins numpy < 2; numpy 1.26.4 has no 3.13 wheel |
| numpy | 1.26.4 | from infretis (`^1.21.2`) |
| HQ | 0.26.2, binary and PyPI client | measured in `../hyperqueue_test`; client and server must match. Before moving to 0.27.x, rerun the 1024-task autoalloc regression check |
| nixpkgs | `151fa4e` (same lock as `../hyperqueue_test`) | |
| GROMACS | 2026.3 (nixpkgs) | laptop only; clusters use their own module |
| LAMMPS | 22Jul2025_update4 (nixpkgs), REAXFF + QEQ enabled | water_dissociation |

Exact Python package versions: `requirements-lock.txt`.

## Runtime design

- One HQ task per MC move by default. Warm workers per pin only if decision
  point D1 (step 5) says so.
- `restart.toml` is the only restart authority: no HQ `--journal` for chained
  runs, a fresh `HQ_SERVER_DIR` per Slurm job.
- No infretis fork: a launcher replaces `ProcessPoolExecutor` at run time.
- Tasks carry paths and indices, never data.
- One node per MD move; HQ multi-node tasks are experimental.

## DAS-6 (development)

- We have an account. Slurm via `module load prun`; partition `defq`.
- Jobs are limited to 15 min walltime 08:00–20:00; longer runs only at night
  or weekends, or with permission from das-sysadm@cs.vu.nl.
- Program execution must happen on compute nodes, never on the head node.
- `/home` is shared between fs0 and the compute nodes. Cross-node file
  visibility latency is unmeasured (step 6).
- Python 3.12.11 exists on fs0 as `python3.12`; the default `python3` is
  3.6.8. Not yet checked on the compute nodes.
- Compute nodes: 24-core EPYC, 128 GB, 100G Ethernet (VU cluster, 34 nodes).
- GPU nodes for chignolin: unknown.

## Snellius (first target)

- No account yet (ask the project lead). HPC-time budget not yet set.
- No SURF guidance found on running an HQ server inside a job.

## Examples (infentory, local clone `0640581`)

- `water_boiling`: GROMACS, CPU, initial paths included. The shipped
  `wmdrun` list must be rewritten (one entry per worker, no overlapping
  pinning).
- `water_dissociation/lammps`: tutorial; starts from a plain MD run.
- `puckering`: overhead stress test; initial paths from `infinit`.
- `2d_potential`: ASE toy, plumbing only.
