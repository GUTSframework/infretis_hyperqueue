# Development

How this repository is built, tested and measured. For using it, see
[README.md](README.md).

## Background

GUTS Fast Track: make infretis usable on HPC clusters, with multi-node runs
that survive walltime limits. HQ is the Task Scheduler. Development on
DAS-6; Snellius is the first real target, LUMI and JSC later.

## Components

How this repo maps onto the GUTSnotes design:

| Component | Here |
|---|---|
| Trajectory Sampler + Result Manager | infretis's MC loop, unchanged |
| Session | the infretis run directory (`load/`, `restart.toml`, `infretis_data.txt`) |
| Task Scheduler | HQ server |
| Process Manager | HQ workers |
| Resource Manager | the chain of Slurm jobs (`gutsrun.chain`, with `gutsrun.hq`) |
| Job Submitter | site files + `sbatch` |

The HPC part (`src/gutsrun/`) is generic and knows nothing about MD; it is
meant to grow into the shared GUTS HPC packages. Only the adapter
(`src/gutsrun_infretis/`) knows about infretis.

### Where things run

One link is one Slurm job. Everything that must outlive a job lives in the
run directory on the shared file system; HQ is started fresh by each link.
Arrows are processes starting, signalling or talking to each other, and
files read or written.

```mermaid
flowchart LR
    subgraph login["login node"]
        cmd["gutsrun-infretis<br/>submit, status, stop"]
    end
    subgraph first["first node of the link's Slurm job"]
        link["link<br/>python -m gutsrun.chain link"]
        hqs["hq server"]
        hqw["hq worker"]
        launcher["launcher<br/>infretis MC loop + TaskExecutor"]
        task["task processes<br/>python -m gutsrun.task:<br/>one move each, running lmp or gmx"]
    end
    more["other nodes, if nodes > 1:<br/>an hq worker and its task processes,<br/>as on the first node"]
    next["next link<br/>(queued: afterany this one)"]
    subgraph shared["run directory, shared file system"]
        chainfiles[".gutsrun/: chain.json, links.jsonl,<br/>link and HQ logs"]
        spooldir["gutsrun-spool/: calls, results,<br/>logs of failed moves"]
        state["infretis.toml, restart.toml, load/,<br/>infretis_data.txt, worker{pin}/"]
    end

    cmd -- "sbatch link 1;<br/>stop: scancel --batch" --> link
    cmd -- "reads" --> chainfiles
    link -- "sbatch" --> next
    link -- "starts" --> hqs & hqw
    link -- "srun" --> more
    link -- "starts, passes SIGTERM on" --> launcher
    link -- "writes" --> chainfiles
    launcher -- "hq submit per move,<br/>hq job cancel" --> hqs
    hqs -- "task events<br/>(hq journal stream)" --> launcher
    hqs --> hqw & more
    hqw -- "starts" --> task
    launcher -- "calls, results" --- spooldir
    task -- "calls, results" --- spooldir
    task -- "MD files" --> state
    launcher -- "treat_output: paths, data,<br/>restart.toml last" --> state
```

Without a chain (`python -m gutsrun_infretis.launcher` on a laptop), there
is no link: the launcher starts an hq server and one worker itself, in
`gutsrun-hq/` of the run directory, and stops them at its end.

### Modules

Arrows are imports. Nothing in `gutsrun` imports the adapter or infretis,
and `task.py` imports nothing at all: a task process reaches infretis only
by unpickling the call it was given (the dotted arrow). The launcher
imports `gutsrun.chain` only for `ROOT`, the checkout that holds `bin/hq`.
Which processes start which: [Where things run](#where-things-run).

```mermaid
flowchart TB
    subgraph adapter["gutsrun_infretis: adapter"]
        ichain["chain.py<br/>InfretisRun workload,<br/>the gutsrun-infretis command"]
        launcher["launcher.py<br/>executor factory, checks,<br/>exit codes"]
    end
    subgraph generic["gutsrun: generic, no MD"]
        gchain["chain.py<br/>the chain of links, Workload"]
        gsite["site.py<br/>sites/*.toml, setup lines"]
        ghq["hq.py<br/>HQ in a Slurm job,<br/>HQBackend: the hq CLI"]
        executor["executor.py<br/>TaskExecutor, Backend, Resources"]
        task["task.py<br/>one task process"]
    end
    subgraph infretis["infretis, GUTS fork"]
        sched["scheduler.scheduler<br/>executor_factory hook, graceful stop"]
        setup["setup.setup_config"]
        runmd["core.tis.run_md, engines"]
    end

    ichain --> gchain & gsite
    launcher --> gchain & ghq & executor
    launcher --> sched & setup
    gchain --> ghq & gsite
    ghq --> executor
    task -. "unpickles and runs" .-> runmd
```

### One move

What happens between infretis picking a move and saving its result. Each
move is one HQ task of the link's HQ job.

```mermaid
sequenceDiagram
    participant R as infretis loop<br/>(launcher process)
    participant X as TaskExecutor<br/>(watcher thread)
    participant S as spool<br/>(shared file system)
    participant H as HQ server and worker
    participant T as task process

    R->>R: REPEX_state.pick: ensemble, path, random stream
    R->>X: submit(partial(run_md, md_items))
    X->>S: in/{tid}.pkl: sys.path and the call
    X->>H: hq submit --job {link's job} --array {tid}:<br/>python -m gutsrun.task {dir} {tid}
    H->>T: starts it on a worker, with its cores
    T->>S: read in/{tid}.pkl
    T->>T: worker_initializer: build the engines<br/>run_md: logs to worker{pin}.log, runs lmp or gmx
    alt the move returns
        T->>S: out/{tid}.pkl
        H->>X: task-finished, in hq journal stream
        X->>S: read out/{tid}.pkl
        X->>R: the future's result
        R->>R: treat_output: path files, infretis_data.txt,<br/>restart.toml last
        R->>X: submit the next move
    else the move raises
        T->>H: end of the traceback in $HQ_ERROR_FILENAME, exit 1
        H->>X: task-failed with that text, in hq journal stream
        X->>R: the future fails: TaskFailed
        Note over R: infretis ends the run (exit 1),<br/>and the chain stops
    end
    Note over R,T: SIGTERM waits until treat_output is done, then infretis<br/>cancels the moves in flight and the launcher exits 3. The next<br/>link redoes them from restart.toml's locked list.
```

## Layout

```
flake.nix               dev shell: hq 0.26.2, Python 3.12, GROMACS, LAMMPS
pyproject.toml          the gutsrun package (src/), the gutsrun-infretis command, pytest
requirements.txt        direct pins (infretis, inftools)
requirements-lock.txt   exact versions, installed by setup-venv.sh
scripts/get-hq.sh       pinned hq binary into ./bin, for clusters without Nix
scripts/setup-venv.sh   .venv from the lock file, plus this checkout (editable)
scripts/sync.sh         rsync the repo to a login node, pull runs/<site>/ back
scripts/wait-jobs.sh    poll a login node's squeue until jobs finish, optionally pull
scripts/das6/           probe-login.sh (what the login node has, read-only);
                        install-engines.sh (GROMACS, LAMMPS from conda-forge)
sites/                  one file per cluster; template.toml documents every key
ASSUMPTIONS.md          version pins and cluster assumptions
PROVENANCE.md           where each file comes from, and under which terms
bench/bare_run.py       run bare infretis on an example, record run.json
bench/chain.py          chains of an example with either runner (the bare-vs-gutsrun rows)
bench/move_times.py     per-move time statistics from sim.log
examples/               prepared infentory cases (examples/README.md)
src/gutsrun/            generic: TaskExecutor; hq.py runs its tasks on HQ; chain.py chains Slurm jobs; site.py (no MD imports)
src/gutsrun_infretis/   infretis adapter, launcher, chain workload and command (the only infretis-aware code)
tests/                  pytest suite (adapter tests need infretis)
```

`runs/` holds bench outputs and is not tracked.

## gutsrun

`gutsrun.TaskExecutor` is a `concurrent.futures.Executor`: each `submit`
runs as one HQ task, a fresh `python -m gutsrun.task` process. The
`Backend` protocol in `executor.py` is the seam between the two: the
tests use fake backends there.

```python
from gutsrun import TaskExecutor
from gutsrun.hq import HQBackend

ex = TaskExecutor(HQBackend(server_dir), "runs/x/spool",
                  initializer=init, initargs=(cfg,),        # runs before every call
                  resource_dict={"cores": 2})               # default per call
fut = ex.submit(fn, arg, resource_dict={"cores": 4, "cwd": "...", "env": {...}})
```

- **Spool directory**, one subdirectory per executor (layout in
  `executor.py`'s docstring): `in/` holds the pickled calls, `out/` the
  results (`<tid>.pkl`), `log/` stdout/stderr (`<tid>.<instance>.{out,err}`;
  HQ deletes them when the task finishes, `:rm-if-finished`, and keeps
  those of failed and cancelled tasks). Files are written plainly: each
  is read only after its writer closed it. Older executors' directories
  stay. Nothing protects an executor from tasks of an earlier one on the
  same HQ server: in a chain each link has a fresh server, and the
  launcher's own server lives as long as the launcher.
- **HQ through its CLI only.** `HQBackend` opens one HQ job (`hq job
  open`) and submits each call as the task of that job whose id is the
  executor's task id (`hq submit --job J --array <tid>`): a program task
  carrying two paths (payload rule), which needs no HQ Python package. One
  `hq journal stream` reports when tasks end, starting with every event
  since the server started; if it ends while the executor runs, the moves
  in flight fail (the run stops, and its next start redoes them). The event
  format is HQ's own: check it when upgrading HQ. HQ 0.26.2 cancels whole
  jobs only, so shutting down cancels the job (`hq job cancel J
  --reason ...`), with SIGINT to each task's process group. HQ reruns a
  task after a worker crash (up to HQ's default limit); the rerun gets a
  new instance number.
- **Completion** from the backend (under HQ, `task-finished` events): the
  executor then reads `out/<tid>.pkl`, retrying for up to 60 s in case the
  shared file system shows it late. Inputs and results of ended tasks are
  deleted.
- **Errors:** a call that raises (or whose value does not pickle) makes
  the task write its traceback to stderr, and its last ~1,900 characters
  to `$HQ_ERROR_FILENAME` (with `--task-dir`), and exit 1. HQ reports that
  text in the `task-failed` event, and the future fails with
  `TaskFailed("task <tid>: <traceback>")`, naming the stderr log. The
  caller gets a message, not the call's own exception: nothing in infretis
  or gutsrun depends on the type.
- **`--time-request`** (`HQBackend(time_request=...)`, off by default): a
  worker with less time left than that does not start the task. A worker
  inside a Slurm job is expected to take its end time from Slurm (not
  yet checked on DAS-6).
- **Like `ProcessPoolExecutor` with spawn:** calls and results must pickle
  by reference; the caller's `sys.path` is sent along. Unlike it, the
  initializer runs before every call, and failures arrive as `TaskFailed`.
- HQ sets `OMP_NUM_THREADS` to the task's cores for every task.

## The launcher

In an infretis run directory, instead of `infretisrun -i infretis.toml`:

```bash
.venv/bin/python -m gutsrun_infretis.launcher -i infretis.toml
HQ_SERVER_DIR=... .venv/bin/python -m gutsrun_infretis.launcher \
    -i restart.toml --cores-per-task 2 [--time-request 30m]
```

or `bench/bare_run.py ... --runner gutsrun`. Moves run on the HQ server
in `$HQ_SERVER_DIR` (a chain's link sets it). Without one, the launcher
starts its own server and a worker on all the cores it may use, for the
run (`gutsrun-hq/`, removed at the end; logs `gutsrun-hq-*.log`), in a
session of their own so that a Ctrl-C stops infretis gracefully before
HQ. There is no HQ-free mode: plain `infretisrun` is the baseline. The
launcher
passes infretis a `TaskExecutor` factory (`executor_factory`, a hook in the
GUTS fork, `requirements.txt`), which runs infretis's own worker
initializer before every move. In the fork, the initializer needs no
shared state, each move logs to `worker{pin}.log`, the workers leave the
shared GROMACS inputs alone (the parent writes them once at setup, and
refuses engines that share an `input_path` with different settings) and
stop their MD program on SIGTERM, infretis stops gracefully on
SIGTERM/SIGINT (after the result it is recording, `restart.toml` last),
and a refused setup (a finished run included) says why on stderr. The
launcher adds a check of `wmdrun`: no pinning flags (HQ places the
tasks), and `-ntomp` equal to `--cores-per-task`; a `wmdrun` that fails it
is refused, not rewritten. At start it refuses an infretis that pip
installed from another commit than the pin (one installed from a local
directory records no commit, and only gets a warning). The fork's other
fixes (atomic `restart.toml`, no busy spin in the parent, restarts from
the run's seed) apply to bare runs too. Exit codes: 0 done, 1 an error
(traceback in `gutsrun.log` and on stderr), 3 stopped by a signal, 4
infretis refused to start, also for a run that is already done (its
reason on stderr). Logs: `gutsrun.log`; moves log to `worker{pin}.log`, as
under plain infretis.

## Chains

`gutsrun.chain` is the generic chain: what one link does, step by step, is
in its module docstring. It reads the site file through `gutsrun.site` and
starts HQ through `gutsrun.hq`; neither imports it. A *workload* (a small class, see `Workload` there)
says what a link runs and how far the run got; `gutsrun_infretis.chain`
has the infretis one and the `gutsrun-infretis` command, `bench/chain.py`
one that runs `bench/bare_run.py` with either runner. A link's own process
runs `.venv/bin/python -m gutsrun.chain link ...` and imports only the
standard library, so it reads nothing from site-packages on the shared
file system; the workload's command then runs from the project venv.
The link writes the HQ server's access file (host, ports the OS picked as
free, keys) before starting the server, so with `nodes` > 1 one srun step
runs `hq worker start` directly on each of the other nodes: a worker that
starts first retries every 2 s until the server listens.

Bench chains on DAS-6, from the repo root on `fs0`:

```bash
.venv/bin/python bench/chain.py 2d gutsrun --steps 1000 --time 00:05:00
.venv/bin/python bench/chain.py 2d bare --steps 3000 --time 00:04:00
.venv/bin/gutsrun-infretis status runs/das6/<name>
```

To run a link without Slurm, e.g. on the laptop, `bench/chain.py
--no-submit` prepares the run and its chain and prints the spec's path;
then `GUTSRUN_CORES=4 .venv/bin/python -m
gutsrun.chain link <spec> 1` runs link 1 here (inside `nix develop` for hq).
The tests do the same through `start(spec, submit_first=False)`.

## Tests

```bash
nix develop --command .venv/bin/python -m pytest          # all but slow (~1 min)
nix develop --command .venv/bin/python -m pytest -m slow  # LAMMPS exact check (~1 min)
.venv/bin/python -m pytest -m "not hq"                    # without hq: chain, site, executor on fakes
```

Without Nix, drop the `nix develop --command` prefix, and call ruff and
pyright as `.venv/bin/ruff` and `.venv/bin/pyright` if you installed them
there ([Getting started (local)](#getting-started-local)).

Lint and type check (both configured in `pyproject.toml`; tests are
exempt from the type-annotation rule):

```bash
nix develop --command ruff check .
nix develop --command pyright
```

The chain tests fake `sbatch`, `scancel` and `squeue` and run links as
local processes. Every test that needs HQ is marked `hq` (conftest adds
the mark to tests using its `hq_cluster` or `make_executor` fixture, and
the launcher tests start their own server). The HQ tests start `hq server` and `hq worker` processes,
which need TCP on localhost. They cannot run inside Claude Code's sandbox.

## Getting started (local)

On any Linux x86-64 machine: a laptop, a workstation, a login node.

With Nix, the dev shell provides everything (hq 0.26.2, Python 3.12,
GROMACS, LAMMPS, ruff, pyright, jq):

```bash
nix develop
scripts/setup-venv.sh
.venv/bin/python -c 'import infretis'
```

Without Nix, you need Python 3.12 and git (infretis and inftools install
from git):

```bash
PYTHON=python3.12 scripts/setup-venv.sh
scripts/get-hq.sh                  # the same pinned hq, into bin/
export PATH="$PWD/bin:$PATH"       # tests look for hq on PATH; chain links find bin/hq themselves
.venv/bin/python -c 'import infretis'
```

The rest of what the dev shell provides, and what needs it:

| Needed for | Without Nix |
|---|---|
| tests marked `hq` | `hq` on `PATH`, as above; without it run `pytest -m "not hq"` |
| the slow exact check | `lmp` with REAXFF and QEQ on `PATH`, e.g. conda-forge `lammps` (`scripts/das6/install-engines.sh` shows one install) |
| GROMACS examples (`puckering`) | `gmx` on `PATH` |
| lint and type check | `.venv/bin/python -m pip install ruff pyright`. Not pinned, so versions may differ from the dev shell's; the PyPI `pyright` downloads Node.js on first use |
| `scripts/wait-jobs.sh --run` | `jq` |

On NixOS without the dev shell, pip's wheels (numpy, mdtraj) do not find
`libstdc++` and `zlib`: use `nix develop`.

## DAS-6 workflow

The login node (`fs0`) is reachable only from a university network or
through eduVPN. Code goes there with `scripts/sync.sh <host> push` (which
writes `REVISION`, since `.git` is not copied), and `runs/das6/` comes back
with `pull`. `scripts/wait-jobs.sh <host> <jobid>` (or `--run <rundir>` for a
chain) waits on the laptop until jobs are done.
