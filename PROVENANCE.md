# Provenance

Where the files in this repo come from, and under which terms. Checked
2026-10-16. The repo itself has **no licence yet** (decision pending).

## Summary

| Origin | Files | Terms |
|---|---|---|
| Written for this repo | `src/gutsrun/`, `src/gutsrun_infretis/`, `tests/`, `bench/`, `scripts/setup-venv.sh`, `examples/puckering/prepare.sh` + `assemble.py`, `examples/README.md`, docs | ours (licence to choose) |
| `../hyperqueue_test` (same author, unlicensed) | `flake.nix`, `flake.lock`, `scripts/get-hq.sh` | ours (same copyright holder) |
| **infentory** (public, **no licence**) | everything under `examples/` except the files listed as ours | **all rights reserved** by default |
| Generated | `requirements-lock.txt`, `examples/water_boiling/gromacs_input/conf.g96` (from infentory's `conf.gro` via `gmx editconf`, so infentory data) | as their source |

Not copied (used as dependencies or by reference only): HyperQueue (MIT),
GROMACS force fields (`#include`d from the GROMACS installation), LAMMPS,
ASE, gutscore (nothing taken), aimmd/asyncmd (nothing taken).

## infretis (MIT)

No infretis code is copied here. The fixes and hooks we need are in the
GUTS fork (github.com/GUTSframework/infretis, branch `guts`), a
dependency installed from `requirements.txt`.

## infentory-derived files (no licence)

infentory commit `0640581` (github.com/infretis/infentory; GitHub reports
no licence, and the repo has no LICENSE file). Without a licence, copying
and redistributing these files is not granted, even though the repo is
public. Files:

- `examples/2d_potential/` (all three files)
- `examples/water_boiling/` (all files; `conf.g96` generated from them;
  changes listed in `examples/README.md`)
- `examples/water_dissociation_lammps/` (all files, incl. ~400 in
  `load_copy/`). `lammps_input/qeq_ff.water` is a ReaxFF parameter set
  whose header asks to cite Achtyl et al., Nat. Comm. 6, 6539 (2015).
- `examples/puckering/`: `gromacs_input/*`, `prep/*.mdp`,
  `prep/order-check.toml`, `infretis0.toml`

Options before sharing this repo outside the project:
1. ask the infretis developers to add a licence to infentory (MIT, like
   infretis);
2. or stop shipping the files: fetch them from infentory at the pinned
   commit and apply our changes as a patch, so only our changes live here.
