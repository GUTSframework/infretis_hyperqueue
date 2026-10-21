# Quick start: DAS-6 with LAMMPS

From an empty account on DAS-6 to an infretis run chained over several
Slurm jobs. It uses the `water_dissociation_lammps` example (ReaxFF water,
4 workers; see [examples/README.md](examples/README.md)). Setting up takes
about half an hour, mostly the engine install. The test run takes three
daytime links of 14 minutes, plus queue time.

What `gutsrun-infretis` does, and every setting, is in the
[README](README.md).

> **Restarts with more than one worker are not exact yet.** Every link
> after the first restarts infretis from `restart.toml`. With
> `workers > 1`, infretis's restart has three known defects:
>
> - A move that was running at the stop is redone with new random
>   numbers unless it was among the most recently started moves. The
>   moves still running at a stop are mostly the long ones, so this
>   biases a run towards short paths.
> - The first `workers - 1` moves after a restart reuse the random
>   streams of the redone moves, so those pairs are correlated.
> - A redone move that is still running at the next stop is dropped
>   instead of redone again.
>
> The infretis fork this repository pins does not fix these yet. With
> `workers = 1` restarts are exact. For now:
>
> - treat results of chained runs with `workers > 1` as preliminary;
> - make links long (night or weekend links on DAS-6), since the effect
>   grows with the number of restarts per move;
> - use this example to check that everything works, not for results.

## 1. Log in

`fs0.das6.cs.vu.nl` is reachable only from a university network or
through eduVPN:

```bash
ssh <user>@fs0.das6.cs.vu.nl
```

Everything below runs on fs0. Never start MD on fs0 itself; the chain
runs it on compute nodes.

## 2. Install, once

```bash
git clone https://github.com/GUTSframework/infretis_hyperqueue.git ~/infretis_hyperqueue
cd ~/infretis_hyperqueue
PYTHON=/usr/bin/python3.12 PIP_ONLY_BINARY=:all: PIP_NO_CACHE_DIR=1 bash scripts/setup-venv.sh
scripts/get-hq.sh
bash scripts/das6/install-engines.sh
```

- `setup-venv.sh` builds `.venv` with infretis (the GUTS fork) and
  `gutsrun-infretis`. It ends with `ok: …/infretis/__init__.py`.
  (fs0's default `python3` is 3.6, hence `PYTHON`.)
- `get-hq.sh` puts the HyperQueue binary in `bin/hq` and prints its
  version.
- `install-engines.sh` installs conda-forge GROMACS and LAMMPS into
  `/var/scratch/$USER/miniforge3`, about 1.2 GB, since DAS-6 provides no
  MD engines. `sites/das6.toml` puts their `gmx` and `lmp` on `PATH` in
  every job; nothing else changes your environment.

## 3. Prepare a run directory

`/home` has a 5 GB quota, so runs go to `/var/scratch/$USER` (40 GB),
which the compute nodes share:

```bash
mkdir -p /var/scratch/$USER/runs
cp -r ~/infretis_hyperqueue/examples/water_dissociation_lammps /var/scratch/$USER/runs/water-test
cd /var/scratch/$USER/runs/water-test
mv load_copy load
```

The example ships its initial paths as `load_copy/`. infretis reads
`load/` and changes it during the run, so the copy keeps the original
intact.

Then add `gutsrun.toml` next to `infretis.toml`:

```toml
site = "das6"
links = 4             # a cap on the chain; the test needs about 3
time = "00:14:00"     # walltime per link: DAS-6 allows 15 minutes 08:00-20:00
name = "water-test"
```

The simulation itself (`steps = 1000`, `workers = 4`, the engine) is set
in `infretis.toml`.

## 4. Check and submit

```bash
~/infretis_hyperqueue/.venv/bin/gutsrun-infretis submit --check
~/infretis_hyperqueue/.venv/bin/gutsrun-infretis submit
```

`--check` runs the checks and shows the `sbatch` command without
submitting anything. `submit` queues link 1. Each link queues the next one
before it starts its own work.

To save typing, put the venv on your `PATH`:

```bash
export PATH=~/infretis_hyperqueue/.venv/bin:$PATH
```

## 5. Follow the run

```bash
gutsrun-infretis status            # where the run is, and each link's job
gutsrun-infretis status --follow   # again whenever a link changes, until the end
gutsrun-infretis stop              # stop the chain; restart.toml stays valid
```

A link of 14 minutes manages roughly 350 steps of this example. Each one
ends "stopped by a signal" shortly before its walltime: Slurm sends
SIGTERM `margin` seconds early (default 90) and sometimes up to a minute
earlier still. The next link continues from there. The link that reaches `steps = 1000` ends the chain
and cancels the link it had queued.

What to look at in the run directory:

- `infretis_data.txt`: infretis's results, one row per new path.
- `restart.toml`: `current.cstep` is the step reached.
- `.gutsrun/link<n>-<job>.out`: each link's log, including infretis's
  output. Look here first when a link failed.
- `gutsrun-stops.jsonl`: per link stop, the moves that will be redone
  and the move time lost.

## 6. Longer runs

- **Night and weekend links.** Outside 08:00–20:00 longer jobs are allowed:
  `gutsrun-infretis submit --night --time 03:00:00`. Without `--night`,
  `submit` refuses a walltime of 15 minutes or more. `--night` only lifts
  that check: submit such links in the evening or at the weekend
  (DAS-6 policy, not enforced by Slurm).
- **More steps.** The run is done when `current.cstep` reaches
  `simulation.steps`. To run further, raise `steps` in `restart.toml` (not
  `infretis.toml`) and `submit` again.
- **Disk.** infretis keeps every path's files by default, about 0.25 MB
  per step for this example. For long runs set `delete_old = true` and
  `delete_old_all = true` under `[output]` in `infretis.toml`. infretis
  then deletes the files of old paths, keeping the initial paths and the
  most recent ones, so `load/` stays roughly the same size.

## 7. Your own LAMMPS case

Start from your own infretis run directory (`infretis.toml`, `load/`,
the LAMMPS inputs, the order parameter) and add `gutsrun.toml` as above.
Before submitting:

- **Workers.** A DAS-6 node has 24 cores. With `cores_per_task = 1` (the
  default: one core per LAMMPS call), up to 24 workers fit on one node.
  See the warning above about `workers > 1`.
- **`wmdrun`.** LAMMPS ignores its values, but infretis looks it up per
  worker: keep one entry per worker (`wmdrun = [0, 0, 0, 0]` for 4), or
  leave it out.
- **`lmp`.** `lmp = 'lmp'` in `[engine]` finds the installed LAMMPS
  (29Aug2024, CPU, no MPI, with REAXFF). A case that needs other LAMMPS
  packages may not run with it.
- **A dry run.** `submit --check` before every first `submit`.

When something fails, keep the run directory as it is. The failing link's
`.gutsrun/link<n>-<job>.out` and `gutsrun.log` are what we need to see.
