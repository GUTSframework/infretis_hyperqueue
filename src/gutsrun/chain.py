"""Run one long job as a chain of Slurm jobs ("links"), each continuing
where the previous one stopped.

Generic: what runs, and how far it got, is up to a *workload* (see
`Workload`). A chain's spec names the workload's class as
`package.module:Class`, e.g. `gutsrun_infretis.chain:InfretisRun`, plus the
keyword arguments to create it with. `gutsrun_infretis.chain` provides the
`gutsrun-infretis` command, which calls `start()`, `status()` and `stop()`.

A link runs this module with the project venv's Python:

    .venv/bin/python -m gutsrun.chain link <rundir>/.gutsrun/chain.json <n>

Standard library only (as are gutsrun.hq and gutsrun.site, which it
uses), so that a link's own process imports nothing from site-packages on
the shared file system.

Each link:

1. Stops if `stop()` was called (exit 0). Link > 1: stops if the
   workload's state is unreadable (exit 1) or the run is done (exit 0).
2. Submits the next link right away with the dependency `afterany` on this
   one: Slurm starts it once this job has ended in any state (finished,
   failed, cancelled, timed out). A link that is killed is still followed.
3. Raises its limit on open files: the soft limit (1024 on DAS-6 compute
   nodes) is the one in force, and a process may raise it up to the hard
   limit. An HQ server and worker with many tasks hold many files and
   sockets at once.
4. If the site sets `[environment] setup`, runs those lines in bash, e.g.
   `module load` for the programs the tasks call. The HQ workers, and so
   their tasks, and the workload run in the environment they leave.
5. If the workload needs HQ: starts an HQ server and one worker inside the
   job, in a fresh server directory on the shared file system, without a
   journal. The workload's own state is what a restart continues from.
   With `nodes` > 1, starts one HQ worker on each of the job's other nodes
   too, in one srun step (`srun ... hq worker start`). At the end of the
   link, stopping the server ends these workers.
6. Runs the workload's command in the run directory, passing SIGTERM and
   SIGINT on. Slurm sends SIGTERM `margin` seconds before the walltime
   (`--signal=B:TERM@margin`), and may send it up to 60 s earlier than asked.
   A signal that comes before the workload has set up its own handler
   (for infretis: while it imports, about a second) kills it; the link then
   reports "failed (exit -15)", although nothing was written.
7. Cancels the next link if the run is done, failed or stopped, or the link
   itself crashed. A workload exits with EXIT_STOPPED when a signal stopped
   it and it can continue.

Everything the chain writes goes to <rundir>/.gutsrun/: chain.json (the
spec), links.jsonl (what happened: one JSON object per line), the link logs
link<n>-<job>.out (including the workload's output) and the HQ logs
link<n>-<job>-hq-server.log, -hq-worker.log and, per other node,
-hq-worker-<node>.log.
"""

import argparse
import contextlib
import getpass
import importlib
import json
import logging
import os
import resource
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import Any, Protocol

from gutsrun import hq
from gutsrun.site import Site, SiteError, setup_env

EXIT_STOPPED = 3
# A link's result when a signal stopped its workload and the next link may
# continue. The walltime signal and a plain `scancel` are both SIGTERM, so
# the link cannot tell them apart; its log says when the stop came.
STOPPED_BY_SIGNAL = "stopped by a signal (walltime or scancel)"
# The project checkout this module runs from (src/gutsrun/chain.py): sites/,
# .venv and bin/ live there.
ROOT = Path(__file__).resolve().parents[2]

log = logging.getLogger("gutsrun.chain")


class ChainError(Exception):
    """A chain cannot start, or a link cannot run its workload."""


class StateError(Exception):
    """The workload's state is unreadable."""


@dataclass
class Progress:
    """How far a run got."""

    done: bool
    text: str  # for the log, e.g. "step 526 of 1000"


class Workload(Protocol):
    """What a chain runs. Created from the spec's `workload` and `args` in
    every link, so it must be importable with the standard library only."""

    @property
    def needs_hq(self) -> bool:
        """Start an HQ server and worker in each link."""
        ...

    def progress(self, rundir: Path) -> Progress:
        """How far the run got: not done if it has not started; raises
        StateError if the state is there but cannot be read."""
        ...

    def before_link(self, rundir: Path, link: int) -> None:
        """Called in links after the first, before anything else runs."""
        ...

    def argv(self, python: str, rundir: Path, link: int) -> list[str]:
        """The command one link runs, in the run directory, with `python`
        (the project venv's interpreter)."""
        ...


@dataclass
class Spec:
    """One chain, saved as <rundir>/.gutsrun/chain.json for its links."""

    rundir: str  # the run's directory, on the shared file system
    site: str  # sites/<site>.toml
    name: str  # in the Slurm job names
    workload: str  # "package.module:Class"
    args: dict[str, Any] = field(default_factory=dict)  # keyword arguments for the workload
    pythonpath: list[str] = field(default_factory=list)  # where else to import it from
    time: str = "00:14:00"  # walltime per link, in Slurm's format
    margin: int = 90  # seconds before the walltime that SIGTERM is asked for
    links: int = 10  # most links in the chain
    nodes: int = 1  # per link; nodes after the first only run HQ workers
    root: str = str(ROOT)  # project checkout: sites/, .venv, bin/

    @property
    def dir(self) -> Path:
        """<rundir>/.gutsrun/, where the chain's own files go."""
        return Path(self.rundir) / ".gutsrun"

    @property
    def path(self) -> Path:
        return self.dir / "chain.json"

    @property
    def record(self) -> Path:
        return self.dir / "links.jsonl"

    @property
    def stop_file(self) -> Path:
        return self.dir / "stop"

    def save(self) -> None:
        """Write the spec to `path`, for the links to read."""
        self.path.write_text(json.dumps(asdict(self), indent=2) + "\n")

    @classmethod
    def load(cls, path: str | Path) -> "Spec":
        return cls(**json.loads(Path(path).read_text()))

    def make_workload(self) -> Workload:
        """Import the workload's class (also from `pythonpath`) and create it."""
        for path in reversed(self.pythonpath):
            if path not in sys.path:
                sys.path.insert(0, path)
        module, _, name = self.workload.partition(":")
        return getattr(importlib.import_module(module), name)(**self.args)


def load_spec(rundir: str | Path) -> Spec:
    """The spec of the chain last started in `rundir`."""
    path = Path(rundir) / ".gutsrun" / "chain.json"
    if not path.exists():
        raise ChainError(f"no chain was started in {rundir} ({path} is missing)")
    return Spec.load(path)


def setup_logging() -> None:
    """Log to stdout as "[HH:MM:SS] message": a link's stdout is its Slurm log."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%H:%M:%S"))
    parent = logging.getLogger("gutsrun")  # this module's and gutsrun.hq's
    parent.addHandler(handler)
    parent.setLevel(logging.INFO)
    parent.propagate = False


def seconds(walltime: str) -> int:
    """A Slurm walltime in seconds. Slurm's formats: "M", "M:S", "H:M:S",
    "D-H", "D-H:M" and "D-H:M:S"; hours may exceed 23."""
    days, _, rest = walltime.rpartition("-")
    parts = [int(p) for p in rest.split(":")]
    if len(parts) > 3:
        raise ValueError(f"not a Slurm walltime: {walltime!r}")
    if days:
        hours, minutes, secs = parts + [0] * (3 - len(parts))
    elif len(parts) == 3:
        hours, minutes, secs = parts
    else:
        hours, (minutes, secs) = 0, (parts + [0])[:2]
    return int(days or 0) * 86400 + hours * 3600 + minutes * 60 + secs


def walltime(secs: int) -> str:
    """Seconds as a Slurm walltime, "H:MM:SS"."""
    return f"{secs // 3600}:{secs % 3600 // 60:02d}:{secs % 60:02d}"


def check_time(spec: Spec, site: Site, night: bool = False) -> None:
    """Refuse a walltime at or over the site's daytime limit (unless `night`),
    or too short to run anything before SIGTERM."""
    limit = site.max_walltime_day
    if limit and not night and seconds(spec.time) >= seconds(limit):
        raise ChainError(f"time {spec.time} is not below the daytime limit {limit} "
                         "(allow with --night)")
    if seconds(spec.time) <= spec.margin + 60:
        raise ChainError(f"time {spec.time} leaves no time to run before SIGTERM "
                         f"(margin {spec.margin} s, which Slurm may bring forward by 60 s)")


def revision(root: str | Path) -> str:
    """The project's commit: the REVISION file on a cluster copy (written by
    scripts/sync.sh push, since .git is not copied), else git; "unknown"
    without either."""
    path = Path(root) / "REVISION"
    if path.exists():
        return path.read_text().strip()
    try:
        out = subprocess.run(["git", "-C", str(root), "describe", "--always", "--dirty"],
                             capture_output=True, text=True)
    except OSError:  # no git
        return "unknown"
    return out.stdout.strip() or "unknown"


# --- the record and the queue ----------------------------------------------------------


def note(spec: Spec, event: str, **fields: Any) -> None:
    """Append an event to the chain's record, links.jsonl."""
    entry = {"time": datetime.now().isoformat(timespec="seconds"), "event": event, **fields}
    with open(spec.record, "a") as f:
        f.write(json.dumps(entry) + "\n")


def events(spec: Spec) -> list[dict[str, Any]]:
    """The record of the chain last started: events since its "chain" event.
    A chain whose link 1 sbatch refused never started: its "chain" event
    and the "not submitted" one that follows it are left out."""
    if not spec.record.exists():
        return []
    entries = []
    for line in spec.record.read_text().splitlines():
        if not line:
            continue
        entry = json.loads(line)
        if entry["event"] == "not submitted" and entries and entries[-1]["event"] == "chain":
            entries.pop()
        else:
            entries.append(entry)
    starts = [i for i, e in enumerate(entries) if e["event"] == "chain"]
    return entries[starts[-1]:] if starts else entries


def queue(user: str | None = None) -> dict[str, str] | None:
    """The user's jobs in Slurm's queue, job id -> "STATE TIME REASON"; None
    if squeue does not answer, which is not "no jobs"."""
    try:
        out = subprocess.run(["squeue", "-h", "-u", user or getpass.getuser(),
                              "-o", "%i %T %M %R"], capture_output=True, text=True,
                             check=True).stdout
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    return {job: rest for job, _, rest in (line.partition(" ") for line in out.splitlines())
            if job}


def chain_jobs(spec: Spec) -> list[tuple[int, str]]:
    """(link, job id) of every link submitted or started in the chain, in order."""
    keys = [(e["link"], e["job"]) for e in events(spec) if e["event"] in ("submitted", "started")]
    return list(dict.fromkeys(keys))


# --- submitting, status, stopping ------------------------------------------------------


def sbatch_argv(spec: Spec, site: Site, link: int, after: str | None = None) -> list[str]:
    """The sbatch command for one link, `after` the job it follows. `exec`
    makes Python the job's main process, which gets Slurm's signal."""
    python = Path(spec.root) / ".venv" / "bin" / "python"
    command = (f"exec {shlex.quote(str(python))} -m gutsrun.chain link "
               f"{shlex.quote(str(spec.path))} {link}")
    argv = ["sbatch", "--parsable", f"--job-name=gutsrun-{spec.name}-{link}",
            f"--partition={site.partition}", f"--nodes={spec.nodes}", f"--time={spec.time}",
            f"--signal=B:TERM@{spec.margin}", f"--chdir={spec.rundir}",
            f"--output={spec.dir / f'link{link}-%j.out'}"]
    if site.exclusive:
        argv.append("--exclusive")
    if after:
        argv.append(f"--dependency=afterany:{after}")
    return [*argv, f"--wrap={command}"]


def submit(spec: Spec, site: Site, link: int, after: str | None = None) -> str:
    """Submit one link and note it in the record; returns its job id."""
    out = subprocess.run(sbatch_argv(spec, site, link, after), capture_output=True,
                         text=True, check=True).stdout
    job = out.strip().split(";")[0]  # --parsable prints "<id>" or "<id>;<cluster>"
    note(spec, "submitted", link=link, job=job)
    return job


@dataclass
class Plan:
    """What `check()` found: everything `start()` needs to go ahead."""

    site: Site
    progress: Progress  # the run's, before the chain
    revision: str  # of the gutsrun checkout


def check(spec: Spec, night: bool = False, submitting: bool = True) -> Plan:
    """Everything `start()` checks, without writing anything. Raises
    ChainError, or StateError if the run's state is unreadable. A run
    directory can take a new chain once the last one has no jobs left in
    the queue. `submitting=False` skips the check for sbatch, for a first
    link run by hand."""
    root = Path(spec.root)
    site = Site(root, spec.site)
    check_time(spec, site, night)
    if submitting and not shutil.which("sbatch"):
        raise ChainError("no sbatch on PATH; submit from the cluster's login node")
    if not Path(spec.rundir).is_dir():
        raise ChainError(f"no run directory {spec.rundir}")
    if submitted := [e["job"] for e in events(spec) if e["event"] == "submitted"]:
        queued = queue()
        if queued is None:
            raise ChainError("squeue does not answer, so it is unclear whether the chain "
                             f"already started in {spec.rundir} still runs")
        if live := [job for job in submitted if job in queued]:
            raise ChainError(f"a chain is still queued or running in {spec.rundir} "
                             f"(jobs {', '.join(live)}); stop it first")
    if not (root / ".venv" / "bin" / "python").exists():
        raise ChainError(f"no .venv in {root}; run scripts/setup-venv.sh first")
    workload = spec.make_workload()
    if spec.nodes < 1:
        raise ChainError(f"nodes = {spec.nodes}; need at least 1")
    if spec.nodes > 1 and not workload.needs_hq:
        raise ChainError(f"nodes = {spec.nodes}, but the workload does not run its tasks "
                         "through HQ, so it would use only the first node")
    progress = workload.progress(Path(spec.rundir))
    if progress.done:
        raise ChainError(f"the run in {spec.rundir} is done: {progress.text}")
    if workload.needs_hq and not ((root / "bin" / "hq").exists() or shutil.which("hq")):
        raise ChainError("no hq in bin/ or on PATH; run scripts/get-hq.sh")
    rev = revision(root)
    if rev == "unknown":
        log.warning("warning: no REVISION file and no git; the runs will not record "
                    "which code they ran (scripts/sync.sh push writes REVISION)")
    return Plan(site, progress, rev)


def start(spec: Spec, *, night: bool = False, submit_first: bool = True) -> str:
    """Check (see `check()`) and save a new chain in an existing run
    directory and submit its first link; returns the job id. With
    `submit_first=False`, returns the spec's path instead, to run
    `python -m gutsrun.chain link <spec> 1` by hand.

    If sbatch refuses link 1, puts the previous chain's spec and stop file
    back, so that `status` still reports on that chain, and raises
    ChainError. No job of the previous chain is left to read them (see
    `check()`); the new spec cannot be written only once link 1 is
    accepted, since link 1 may start and read it at once."""
    plan = check(spec, night, submitting=submit_first)
    spec.dir.mkdir(exist_ok=True)
    previous = spec.path.read_bytes() if spec.path.exists() else None
    was_stopped = spec.stop_file.exists()
    spec.stop_file.unlink(missing_ok=True)
    spec.save()
    # Noted before submitting: link 1 may start, and note "started", at once.
    note(spec, "chain", name=spec.name, site=spec.site, time=spec.time, links=spec.links,
         revision=plan.revision, progress=plan.progress.text)
    if not submit_first:
        return str(spec.path)
    try:
        return submit(spec, plan.site, 1)
    except (subprocess.CalledProcessError, OSError) as e:
        error = (e.stderr or "").strip() if isinstance(e, subprocess.CalledProcessError) else ""
        error = error or str(e)
        note(spec, "not submitted", link=1, error=error)
        if previous is None:
            spec.path.unlink()
        else:
            spec.path.write_bytes(previous)
        if was_stopped:
            spec.stop_file.touch()
        raise ChainError(f"sbatch refused link 1, nothing submitted: {error}") from e


@dataclass
class Report:
    """The state of a chain, from `status()`."""

    progress: str  # the run's, e.g. "step 526 of 1000"
    links: list[str]  # one line per link
    states: list[str]  # per link, its state without times, to spot changes
    over: str | None  # why the chain is over, if it is
    active: bool  # a job of the chain is queued or running (or squeue does not answer)

    def lines(self) -> list[str]:
        """The report as text, one line each."""
        return [self.progress, *self.links] + ([f"chain over: {self.over}"] if self.over else [])


def why_over(spec: Spec, record: list[dict[str, Any]]) -> str:
    """Why a chain with no job left is over, from its record: the run is
    done, the chain was stopped, or else what happened to its last link."""
    ended = {(e["link"], e["job"]): e for e in record if e["event"] == "ended"}
    started = {(e["link"], e["job"]): e for e in record if e["event"] == "started"}
    if any(e["result"].startswith("done") for e in ended.values()):
        return "the run is done"
    if spec.stop_file.exists():
        return "stopped by request; `submit` again to continue"
    jobs = list(dict.fromkeys((e["link"], e["job"]) for e in record
                              if e["event"] in ("submitted", "started")))
    if not jobs:
        return "no link was submitted"
    link, job = jobs[-1]
    log_file = spec.dir / f"link{link}-{job}.out"
    if (link, job) in ended:
        result = ended[(link, job)]["result"]
        if result == STOPPED_BY_SIGNAL and link >= spec.links:
            ran = "its only link ran" if spec.links == 1 else f"all {spec.links} links ran"
            return f"{ran}; `submit` again to continue"
        return f"link {link}: {result}; see {log_file}"
    if (link, job) in started:
        return (f"link {link} gone without a word after starting on "
                f"{started[(link, job)]['host']}; see {log_file}")
    before = [(n, j) for n, j in jobs if n == link - 1]
    where = (f"; see {spec.dir / f'link{before[-1][0]}-{before[-1][1]}.out'}"
             if before else "")
    return f"link {link} was cancelled before it started{where}"


def status(spec: Spec) -> Report:
    """A report on the chain: the run's progress, each link's state, and
    whether and why the chain is over."""
    try:
        progress = (f"{spec.name} ({spec.site}): "
                    f"{spec.make_workload().progress(Path(spec.rundir)).text}")
    except StateError as e:
        progress = f"{spec.name} ({spec.site}): {e}"
    record = events(spec)
    ended = {(e["link"], e["job"]): e for e in record if e["event"] == "ended"}
    started = {(e["link"], e["job"]): e for e in record if e["event"] == "started"}
    queued = queue()
    links, states = [], []
    if queued is None:
        links.append("(squeue does not answer)")
        states.append("no squeue")
    active = queued is None
    for link, job in chain_jobs(spec):
        head = f"link {link:>2}  job {job:<10}"
        if queued and job in queued:
            active = True
            state, _, rest = queued[job].partition(" ")
            elapsed, _, reason = rest.partition(" ")
            links.append(f"{head} {state} {elapsed} {reason}")
            states.append(f"{link} {state} {reason}")
        elif (link, job) in ended:
            e = ended[(link, job)]
            links.append(f"{head} ended {e['time']}: {e['result']}, {e['progress']}")
            states.append(f"{link} ended")
        elif (link, job) in started:
            links.append(f"{head} gone without a word after starting on "
                         f"{started[(link, job)]['host']}; see {spec.dir}/link{link}-{job}.out")
            states.append(f"{link} gone")
        elif queued is not None:
            links.append(f"{head} cancelled before it started")
            states.append(f"{link} cancelled")
    over = why_over(spec, record) if not active and record else None
    return Report(progress, links, states, over, active)


def follow(spec: Spec, interval: float = 30, progress_every: float = 300) -> None:
    """Print the report when a link's state changes, and otherwise at most
    every `progress_every` seconds while the run progresses; until no job
    of the chain is left."""
    shown: Report | None = None  # the report last printed
    printed = 0.0
    while True:
        report = status(spec)
        changed = shown is None or report.states != shown.states
        progressed = (shown is not None and report.progress != shown.progress
                      and time.monotonic() - printed >= progress_every)
        if changed or progressed or not report.active:
            print(f"--- {datetime.now():%H:%M:%S}", *report.lines(), sep="\n", flush=True)
            printed = time.monotonic()
            shown = report
        if not report.active:
            return
        time.sleep(interval)


def stop(spec: Spec) -> list[str]:
    """Stop the chain: no further link starts, queued links are cancelled,
    and a running link gets SIGTERM, which its workload handles as at the
    walltime. Returns what it did, one line each."""
    spec.stop_file.touch()
    queued = queue()
    if queued is None:
        raise ChainError("squeue does not answer; no link will start, but a running "
                         "one keeps running")
    done = []
    for link, job in chain_jobs(spec):
        if job not in queued:
            continue
        if queued[job].startswith("RUNNING"):
            # --batch: only the job's main process, the link, which passes it on.
            subprocess.run(["scancel", "--batch", "--signal=TERM", job], check=True)
            done.append(f"link {link} (job {job}): sent SIGTERM")
        else:
            subprocess.run(["scancel", job], check=True)
            done.append(f"link {link} (job {job}): cancelled")
    return done or ["no job of the chain is queued or running"]


# --- one link --------------------------------------------------------------------------


class Forwarder:
    """Passes SIGTERM and SIGINT on to the workload once it runs; one that
    arrives earlier keeps the workload from starting."""

    def __init__(self) -> None:
        self.child: subprocess.Popen[bytes] | None = None
        self.received: str | None = None
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, self.handle)

    def handle(self, signum: int, _frame: FrameType | None) -> None:
        self.received = signal.Signals(signum).name
        log.info(f"{self.received}: stopping the workload")
        if self.child is not None:
            self.child.send_signal(signum)

    def start(self, argv: list[str], cwd: Path | None = None,
              env: dict[str, str] | None = None) -> subprocess.Popen[bytes]:
        """Start the workload. A signal in the moment while it starts is
        lost: the workload then runs on until the walltime signal."""
        self.child = subprocess.Popen(argv, cwd=cwd, env=env)
        return self.child


def run_link(spec_path: str | Path, link: int) -> int:
    """Run link number `link` of a chain (steps 1-8 in the module docstring);
    returns the job's exit code."""
    started = time.monotonic()
    forwarder = Forwarder()
    spec = Spec.load(spec_path)
    rundir = Path(spec.rundir)
    site = Site(spec.root, spec.site)
    workload = spec.make_workload()
    job = os.environ.get("SLURM_JOB_ID", f"nojob-{os.getpid()}")
    log_prefix = str(spec.dir / f"link{link}-{job}")
    host = socket.gethostname()
    log.info(f"link {link}/{spec.links} of {spec.name} on {host}, job {job}, "
             f"code {revision(spec.root)}")
    note(spec, "started", link=link, job=job, host=host)

    def end(code: int, result: str, progress: str) -> int:
        note(spec, "ended", link=link, job=job, exit=code, result=result, progress=progress)
        return code

    next_job = None

    def cancel_next(reason: str) -> None:
        if next_job is not None:
            subprocess.run(["scancel", next_job])
            log.info(f"cancelled next link {next_job} ({reason})")

    # Anything not foreseen below (a bug, a workload that raises, a missing
    # interpreter) still ends the link in the record and cancels the next
    # one, which would otherwise fail the same way.
    try:
        if spec.stop_file.exists():
            log.info("the chain was stopped; not running")
            return end(0, "not run: chain stopped", "")
        if link > 1:
            try:
                progress = workload.progress(rundir)
            except StateError as e:
                log.info(f"{e}; chain stops")
                return end(1, "unreadable state", str(e))
            if progress.done:
                log.info(f"done: {progress.text}")
                return end(0, "done before it ran", progress.text)
            log.info(f"continuing at {progress.text}")
            workload.before_link(rundir, link)

        if link < spec.links and shutil.which("sbatch"):
            try:
                next_job = submit(spec, site, link + 1, after=job)
                log.info(f"next link: job {next_job} (afterany:{job})")
            except subprocess.CalledProcessError as e:
                log.info(f"could not submit the next link ({e.stderr.strip()}); "
                         "this one runs anyway")

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
        log.info(f"open files: {soft} -> {hard}")

        exit_code = EXIT_STOPPED
        try:
            with contextlib.ExitStack() as stack:
                python = str(Path(spec.root) / ".venv" / "bin" / "python")
                env = dict(os.environ)
                if site.setup:
                    log.info(f"site setup: {site.setup!r}")
                    env = setup_env(site.setup, env)
                if workload.needs_hq:
                    server = spec.dir / f"hq-{job}"
                    hq_dir = stack.enter_context(hq.server_and_worker(
                        spec.root, server, log_prefix, site.cores, env))
                    if spec.nodes > 1:
                        stack.enter_context(hq.workers_elsewhere(
                            spec.root, server, log_prefix, spec.nodes - 1, site.cores, env))
                    env["HQ_SERVER_DIR"] = str(server)
                    env["PATH"] = os.pathsep.join([hq_dir, env.get("PATH", "")])
                argv = workload.argv(python, rundir, link)
                if forwarder.received:
                    log.info("not starting the workload")
                else:
                    log.info(f"running: {shlex.join(argv)}")
                    child = forwarder.start(argv, cwd=rundir, env=env)
                    exit_code = child.wait()  # resumes after a forwarded signal
                    forwarder.child = None
                    log.info(f"workload exit {exit_code}")
        except (ChainError, SiteError, hq.HQError) as e:
            log.info(str(e))
            cancel_next("failed")
            return end(1, "failed", str(e))

        try:
            progress = workload.progress(rundir)
        except StateError as e:
            log.info(f"{e} after the run; chain stops")
            cancel_next("failed")
            return end(1, "unreadable state", str(e))
        if progress.done:
            log.info(f"done: {progress.text}")
            cancel_next("done")
            return end(0, "done", progress.text)
        if exit_code == EXIT_STOPPED:
            if spec.stop_file.exists():
                log.info(f"stopped by request; run: {progress.text}")
                cancel_next("chain stopped")
                return end(0, "stopped by request", progress.text)
            then = (f"link {link + 1} continues" if next_job
                    else f"no further link (links={spec.links})")
            ran = walltime(round(time.monotonic() - started))
            log.info(f"{STOPPED_BY_SIGNAL} after {ran} of {spec.time}; "
                     f"run: {progress.text}; {then}")
            return end(0, STOPPED_BY_SIGNAL, progress.text)
        log.info(f"workload failed (exit {exit_code}); run: {progress.text}; chain stops")
        cancel_next("failed")
        return end(1, f"failed (exit {exit_code})", progress.text)
    except Exception as e:
        log.exception("link crashed")
        cancel_next("crashed")
        return end(1, f"crashed: {type(e).__name__}: {e}", "")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    link = commands.add_parser("link", help="run one link (inside its Slurm job)")
    link.add_argument("spec", type=Path)
    link.add_argument("link", type=int)
    args = parser.parse_args(argv)
    setup_logging()
    return run_link(args.spec, args.link)


if __name__ == "__main__":
    # Through the imported module: workloads raise gutsrun.chain.StateError,
    # which is not this __main__ module's class of the same name.
    from gutsrun.chain import main as imported_main
    sys.exit(imported_main())
