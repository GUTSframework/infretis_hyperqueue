# Examples

Prepared copies of [infentory](https://github.com/infretis/infentory) cases,
taken from commit `0640581` (2026-05-17). `bench/bare_run.py` copies one into
a run directory, renames `load_copy/` to `load/` and applies overrides
(workers, steps, `wmdrun`), so the files here stay as close to infentory as
possible. Changes against infentory are listed per example.

| Example | Engine | Role | Changes |
|---|---|---|---|
| `2d_potential` | ASE (toy) | plumbing, exact checks | none |
| `water_boiling` | GROMACS (CPU) | **blocked**: the shipped initial paths are truncated (every `traj.trr` holds 1 frame, `traj.txt` refers to 4–102), so it needs new initial paths | `wmdrun` reduced to one entry for `workers = 1` (the original had 2 entries with overlapping pinning); added `gromacs_input/conf.g96` (`gmx editconf -f conf.gro -o conf.g96`, GROMACS 2026.3): the infretis GROMACS engine reads it and infentory ships only `conf.gro`; commented out `ref_t = 396` and `gen_temp` in `grompp.mdp`, which the current engine rejects (it sets `ref-t` and `dt` itself from `infretis.toml`), so the thermostat now runs at 573.15 K instead of 396 K |
| `water_dissociation_lammps` | LAMMPS (ReaxFF, 96 atoms) | step 2 LAMMPS case | from `water_dissociation/lammps/step2_iretis`; `load/` stored as `load_copy/`; `lammps_input/` moved inside, so `input_path = 'lammps_input'` and `pair_coeff` reads `../lammps_input/qeq_ff.water` |
| `puckering` | GROMACS (CPU), oxane in TIP3P water, ~890 atoms | step 2 GROMACS case: short frames (`subcycles = 2`), so per-move time is mostly `gmx` process start-up | inputs from `gromacs_input/`, `step1_equilibration/`, `step2_md_run/md.mdp` and `step3_infretis/infretis0.toml`; no initial paths shipped, so `prepare.sh` follows the README pipeline (solvate, EM/NVT/NPT, 1 ns MD, `inft infinit` with interfaces 10 and 90) into `runs/prepared/puckering/` |

`prepare.sh` unsets `OMP_NUM_THREADS`: `gmx mdrun` aborts when it differs
from `-ntomp`. This matters under HQ: HQ 0.26.2 sets `OMP_NUM_THREADS` to
the task's allocated CPU count for every task unless the task environment
already sets it (`crates/hyperqueue/src/worker/start/program.rs:295`). So
GROMACS `-ntomp` must equal the task's `--cpus`, or the task must set
`OMP_NUM_THREADS` itself.
