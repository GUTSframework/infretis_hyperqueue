# infretis on HPC via HyperQueue

Run an [infretis](https://github.com/infretis/infretis) simulation on a
Slurm cluster for longer than one job's walltime. `gutsrun-infretis` runs it
as a chain of Slurm jobs ("links"): each link runs infretis until shortly
before its walltime, stops it cleanly, and the next link continues from
`restart.toml`. Inside each job, [HyperQueue](https://github.com/It4innovations/hyperqueue)
(HQ) runs each Monte Carlo move as its own task.

You keep your infretis run directory as it is (`infretis.toml`, `load/`,
the engine's inputs); the chain runs in it.

It runs infretis from the [GUTS fork](https://github.com/GUTSframework/infretis)
(branch `guts`): upstream infretis plus a few fixes that are offered
upstream, among them restarts that continue the run's own random streams
(upstream restarts from seed 0's) and an atomic `restart.toml`. Its
results match upstream's for runs that are never restarted.

Restarts are exact only with `workers = 1`. With more workers, a restart
redoes some interrupted moves with new random numbers, gives a few moves
the same random stream, and can drop a move that was already redone once.
The fork does not fix this yet; [QUICKSTART.md](QUICKSTART.md) says what
it means for a run.

Part of the GUTS Fast Track. For how it is built, tested and measured, see
[DEVELOPMENT.md](DEVELOPMENT.md). Terms such as link, move and HQ task:
[GLOSSARY.md](GLOSSARY.md).

## How a run goes

```
login node                     compute node, one Slurm job per link
----------                     ------------------------------------
gutsrun-infretis submit ─────▶ link 1  submits link 2 (starts after link 1 ends)
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

New to this? [QUICKSTART.md](QUICKSTART.md) walks through a first run
step by step, on DAS-6 with LAMMPS.

Once per cluster, on its login node, in a copy of this repository. It
needs Python 3.12 (set `PYTHON` if it is not called `python3.12`), git,
curl, sha256sum and tar:

```bash
scripts/setup-venv.sh       # .venv with infretis (the fork); bin/gutsrun-infretis (see the script for clusters)
scripts/get-hq.sh           # the hq binary, into bin/
```

Then put the checkout's `bin/` on your `PATH`, e.g. in `~/.bashrc`:

```bash
export PATH=~/infretis_hyperqueue/bin:$PATH
```

It holds only `gutsrun-infretis` and `hq`, so the venv's own `python`
stays out of your way.

The cluster needs a site file, `sites/<site>.toml` (DAS-6: `das6`; see
[Adding a cluster](#adding-a-cluster)).

Then, for each run, in the run directory on a file system the compute nodes
share:

```bash
cd /path/to/my-run
gutsrun-infretis submit --check --site das6 --links 20   # the checks only: shows the sbatch command, submits nothing
gutsrun-infretis submit --site das6 --links 20
gutsrun-infretis status            # where the run is, and each link's job
gutsrun-infretis status --follow   # again whenever it changes, until the end
gutsrun-infretis stop              # stop the chain; restart.toml stays valid
```

A `stop` within about a second of a link starting can show that link as
"failed (exit -15)": infretis was still loading and had written nothing, so
the run continues normally with the next `submit`.

For scripts: `status` exits with 0 when the chain is over (none of its
jobs is queued or running) and with 10 while it is still active.
`status --json` prints the same report as JSON; `--help` lists its keys.
From another machine, e.g. your laptop, `scripts/wait-jobs.sh <host> --run
<rundir>` waits over ssh until the chain is over. It needs a copy of this
repository and `jq` there, and ssh to the login node without a password
prompt (a key or an agent).

## Settings

Instead of options, a run directory can hold its settings in
`gutsrun.toml`; options on the command line override it:

```toml
site = "das6"
links = 20             # a cap on the whole chain (default 10)
time = "00:14:00"      # walltime per link (default: just under the site's max_walltime)
margin = 90            # seconds before the walltime that SIGTERM comes (default 90)
cores_per_task = 1     # cores per infretis move, i.e. per MD engine call (default 1)
gpus_per_task = 0      # GPUs per infretis move (default 0)
nodes = 1              # nodes per link, each with an HQ worker on all its cores (default 1)
time_request = "30m"   # start a move only if its link has this much time left (default: off)
name = "water-run-3"   # label in the Slurm job names (default: the directory's name)
```

The simulation itself (steps, workers, engines) stays in `infretis.toml`.
`submit` and `submit --check` first print the walltime per link and where
it came from: `--time`, `gutsrun.toml`, or the site's `max_walltime`.
More nodes only help if infretis has more `workers` than one node's cores
hold (`workers` × `cores_per_task` > the site's `cores_per_node`).
`time_request` keeps HQ from starting a move that its link would cut off
at the walltime; the move waits for the next link instead. Set it to about
your longest move plus `margin`. `hq task explain <job> <task>` (with
`HQ_SERVER_DIR` set to the link's `.gutsrun/hq-<job>`) says why a move
waits.
To label a run beyond `name`, use version control: a commit message or a
README in the run directory.

## What a chain leaves behind

infretis's own outputs, as without gutsrun, plus:

- `.gutsrun/link<n>-<job>.out`: each link's log, including infretis's
  output. The first place to look when a link failed.
- `.gutsrun/links.jsonl`: one line per event (submitted, started, ended,
  with the result and the step reached). `status` reads it.
- `.gutsrun/chain.json`: the chain's settings.
- `.gutsrun/link<n>-<job>-hq-server.log`, `-hq-worker.log` and, with
  `nodes` > 1, `-hq-worker-<node>.log` for each other node: HQ's own logs.
- `gutsrun-spool/<executor>/log/`, one directory per link: stdout and
  stderr of moves that failed or were cancelled at a stop (those of
  finished moves are deleted). Delete old ones when you like.
- `gutsrun.log`: the launcher's log.

infretis writes its data to `infretis_data.txt`, unless that file exists
when a run starts fresh: then to `infretis_data_1.txt` (`_2`, ...), and
`restart.toml`'s `output.data_file` names the file in use. So if link 1
stops before infretis completes its first move (an early `stop`, a crash, a
walltime too short for one move), `infretis_data.txt` keeps only its
header, and the run's data go to `infretis_data_1.txt`.

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
  `max_walltime` holds such a limit; a chain without its own `time`
  then gets just under it, and without either `submit` asks for `time`. Nothing stops a longer `time`: keeping to the
  policy is up to you. Where Slurm itself enforces the limits, leave it
  out.
- **How the MD engine is loaded.** Usually a site module, whose name
  differs from cluster to cluster. `[environment] setup` holds the lines
  that load it (e.g. `module load GROMACS`); each link runs them before
  it starts HQ and infretis, and every move runs in the environment they
  leave.

## DAS-6

The login node (`fs0`) is reachable only from a university network or
through eduVPN. Daytime jobs (08:00–20:00) may run 15 minutes at most;
`submit` picks 14 minutes unless told otherwise. `/home` has a 5 GB quota,
so put run directories with MD output in `/var/scratch/$USER`.
