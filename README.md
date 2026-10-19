# infretis on HPC via HyperQueue

Run an [infretis](https://github.com/infretis/infretis) simulation on a
Slurm cluster for longer than one job's walltime. `gutsrun-infretis` runs it
as a chain of Slurm jobs ("links"): each link runs infretis until shortly
before its walltime, stops it cleanly, and the next link continues from
`restart.toml`. Inside each job, [HyperQueue](https://github.com/It4innovations/hyperqueue)
(HQ) runs each Monte Carlo move as its own task.

You keep your infretis run directory as it is (`infretis.toml`, `load/`,
the engine's inputs); the chain runs in it.

Part of the GUTS Fast Track. For how it is built, tested and measured, see
[DEVELOPMENT.md](DEVELOPMENT.md).

## How a run goes

```
login node                     compute node, one Slurm job per link
----------                     ------------------------------------
gutsrun-infretis submit ─────▶ link 1  submits link 2 (starts after link 1 ends)
                                       unpacks the venv to node-local disk
                                       starts an HQ server and worker
                                       runs infretis from infretis.toml
                                       ... SIGTERM 90 s before the walltime:
                                       infretis stops once restart.toml is
                                       consistent; unfinished moves are redone
                               link 2  continues from restart.toml, submits link 3
                                 ...
                               link n  the run reaches its steps: cancels link n+1
```

A link that fails, or finds `restart.toml` unreadable, cancels the next
one, so a broken run does not burn through its links.

## Quick start

Once per cluster, on its login node, in a copy of this repository:

```bash
scripts/setup-venv.sh       # .venv with infretis and gutsrun-infretis (see the script for clusters)
scripts/get-hq.sh           # the hq binary, into bin/
```

The cluster needs a site file, `sites/<site>.toml` (DAS-6: `das6`; see
[Adding a cluster](#adding-a-cluster)).

Then, for each run, in the run directory on a file system the compute nodes
share:

```bash
cd /path/to/my-run
~/infretis_hyperqueue/.venv/bin/gutsrun-infretis submit --site das6 --links 20
gutsrun-infretis submit --check    # the checks only: shows the sbatch command, submits nothing
gutsrun-infretis status            # where the run is, and each link's job
gutsrun-infretis status --follow   # again when a link changes (progress: every 5 min), until the end
gutsrun-infretis stop              # stop the chain; restart.toml stays valid
```

(The examples call it by its path once; with `.venv/bin` on your `PATH`,
`gutsrun-infretis` is enough.) `status` exits with 0 once none of the
chain's jobs is queued or running, and 10 while some are, for scripts.
From another machine, `scripts/wait-jobs.sh <host> --run <rundir>` waits
over ssh until the chain is over.

## Settings

Instead of options, a run directory can hold its settings in
`gutsrun.toml`; options on the command line override it:

```toml
site = "das6"
links = 20             # a cap on the whole chain (default 10)
time = "00:14:00"      # walltime per link (default: just under the site's daytime limit)
margin = 90            # seconds before the walltime that SIGTERM comes (default 90)
cores_per_task = 1     # cores per infretis move, i.e. per MD engine call (default 1)
name = "water-run-3"   # label in the Slurm job names (default: the directory's name)
```

The simulation itself (steps, workers, engines) stays in `infretis.toml`.
To label a run beyond `name`, use version control: a commit message or a
README in the run directory.

## What a chain leaves behind

infretis's own outputs, as without gutsrun, plus:

- `.gutsrun/link<n>-<job>.out`: each link's log, including infretis's
  output. The first place to look when a link failed.
- `.gutsrun/links.jsonl`: one line per event (submitted, started, ended,
  with the result and the step reached). `status` reads it.
- `.gutsrun/chain.json`: the chain's settings.
- `gutsrun.log`, `gutsrun-timings.jsonl`: the launcher's log and per-move
  timings.
- `restart.toml.bak.<step>`: a copy of `restart.toml` at each link's start.

## Running longer, starting again

The run is done when `restart.toml`'s `current.cstep` reaches
`simulation.steps`. To run further, raise `steps` in `restart.toml` and
`submit` again. `submit` also starts a new chain after one ran out of
links, or was stopped; it refuses while a chain's jobs are still queued or
running in the directory.

## Adding a cluster

Copy `sites/template.toml` to `sites/<name>.toml` and fill it in: the
template explains every key and what each value means. Two things that
differ between clusters:

- **Policy limits.** Some clusters limit the walltime by policy rather
  than in Slurm, e.g. DAS-6's 15 minutes during the day.
  `max_walltime_day` holds such a limit; `submit` then refuses a longer
  `time` unless given `--night`. Where Slurm itself enforces the limits,
  leave it out.
- **Where Python starts from.** With `local_venv = true`, each job unpacks
  the venv to node-local disk, which helps when the venv is on NFS.

## DAS-6

The login node (`fs0`) is reachable only from a university network or
through eduVPN. Daytime jobs (08:00–20:00) may run 15 minutes at most;
`submit` picks 14 minutes unless told otherwise. `/home` has a 5 GB quota,
so put run directories with MD output in `/var/scratch/$USER`.
