# Glossary

The terms used in this repository, grouped by where they come from. How
they fit together is at the end.

## The chain (gutsrun, Slurm)

- **Run directory**: an infretis run's directory (`infretis.toml`,
  `load/`, the engine's inputs). The chain runs in it and keeps all its
  state there, on a file system the compute nodes share.
- **Chain**: the whole run as `gutsrun-infretis submit` starts it: a
  series of links. Its settings are in `.gutsrun/chain.json`, its history
  in `.gutsrun/links.jsonl`.
- **Link**: one Slurm job of the chain, with walltime `time` on `nodes`
  nodes. Each link queues the next one when it starts, runs infretis
  until shortly before its walltime, and stops it cleanly. At most `links`
  links per chain.
- **Margin**: how many seconds before the walltime Slurm sends a link
  SIGTERM (default 90), the signal to stop.
- **Site**: what gutsrun knows about a cluster, in `sites/<site>.toml`:
  partition, cores per node, `max_walltime`, the setup lines that load the
  MD engine.
- **Workload**: what a link runs, and how it tells how far the run got.
  For infretis: the launcher, and `restart.toml`.

## The simulation (infretis)

- **Path**: a short MD trajectory. infretis samples paths that cross
  interfaces of an order parameter.
- **Ensemble**: the set of paths for one interface. infretis holds one
  current path per ensemble.
- **Move** (Monte Carlo move): infretis picks an ensemble and its path,
  runs the MD engine to make a new trial path, and accepts or rejects it.
  Seconds long in the examples, minutes to a day in production.
- **Step**: one finished move. `current.cstep` in `restart.toml` counts
  them; the run is done at `simulation.steps`.
- **infretis worker**: a slot for one move in flight. infretis keeps up
  to `workers` moves running at once. Not the same as an HQ worker.
- **Pin**: an infretis worker's index, 0 to `workers - 1`. Moves log to
  `worker{pin}.log`, and `wmdrun` has one entry per pin.
- **`wmdrun`**: infretis's list of MD run commands, one per pin (e.g.
  `gmx mdrun -ntomp 2`). GROMACS uses it; LAMMPS ignores the values.
- **`restart.toml`**: infretis's record of the run: the step reached,
  the current paths, the moves in flight. The only state that crosses
  from one link to the next.

## Running the moves (HyperQueue, gutsrun)

- **HQ server**: HyperQueue's scheduler. It knows the workers and the
  tasks, and decides which task runs where. One per link, started and
  stopped by the link.
- **HQ worker**: a process on one node that offers that node's cores to
  the server and starts the tasks the server gives it, many at once. One
  per node of a link.
- **HQ job**: a container of tasks, not tied to a worker. The launcher
  opens one per link and adds one task per move; a stop cancels it, with
  all its tasks.
- **HQ task**: one process on one worker, with the cores it asked for
  (`cores_per_task`). Here: one move. If its worker crashes, HQ reruns it
  as a new instance.
- **`cores_per_task`**: cores per move, i.e. per MD engine call.
- **`time_request`**: HQ starts a task only on a worker with at least this
  much time left in its link (off by default).
- **Launcher**: `python -m gutsrun_infretis.launcher`, which runs
  infretis with a TaskExecutor in place of its own process pool.
- **TaskExecutor**: gutsrun's `concurrent.futures.Executor` that turns
  each call infretis submits into an HQ task.
- **Spool**: `gutsrun-spool/`, where the executor and its tasks exchange
  calls and results as files, and where logs of failed moves stay.

## How they fit

```
chain ─┬─ link 1 (Slurm job): HQ server, an HQ worker per node, one HQ job
       │     infretis (launcher): up to `workers` moves in flight
       │       move, move, move, ...: each an HQ task; each finished one a step
       │     SIGTERM (margin): finish saving the current move, write
       │     restart.toml, stop
       ├─ link 2: fresh HQ, continues from restart.toml, redoes the moves
       │          that were cut off
       └─ ...
```

A link runs many moves: in [QUICKSTART.md](QUICKSTART.md)'s example, a
14-minute link manages about 350 steps with 4 workers. A move longer than
what is left of a link is cut off and redone in the next one;
`time_request` keeps it from starting there in the first place.
