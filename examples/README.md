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
