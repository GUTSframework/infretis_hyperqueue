"""Run one long job as a chain of Slurm jobs ("links"), each continuing
where the previous one stopped.

Generic: what runs, and how far it got, is up to a *workload* (see
`Workload`). A chain's spec names the workload's class as
`package.module:Class`, e.g. `gutsrun_infretis.chain:InfretisRun`, plus the
keyword arguments to create it with. `gutsrun_infretis.chain` provides the
`gutsrun-infretis` command, which calls `start()`, `status()` and `stop()`.

A link runs this module with the project venv's Python:

    .venv/bin/python -m gutsrun.slurm link <rundir>/.gutsrun/chain.json <n>
    .venv/bin/python -m gutsrun.slurm pack     (bin/venv.tar from .venv)

Standard library only, so that a link's own process imports nothing from
site-packages on the shared file system.

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
4. If the site sets `[node] local_venv`, unpacks bin/venv.tar to node-local disk
   and runs that copy: starting Python from a node-local disk is faster
   than from NFS, and spares the file server. First removes copies left by
   jobs that are gone (a job killed at its walltime skips its cleanup).
5. If the workload needs HQ: starts an HQ server and one worker inside the
   job, in a fresh server directory on the shared file system, without a
   journal. The workload's own state is what a restart continues from.
6. Runs the workload's command in the run directory, passing SIGTERM and
   SIGINT on. Slurm sends SIGTERM `margin` seconds before the walltime
   (`--signal=B:TERM@margin`), and may send it up to 60 s earlier than asked.
7. Cancels the next link if the run is done, failed or stopped. A workload
   exits with EXIT_STOPPED when a signal stopped it and it can continue.

Everything the chain writes goes to <rundir>/.gutsrun/: chain.json (the
spec), links.jsonl (what happened: one JSON object per line), the link logs
link<n>-<job>.out (including the workload's output) and the HQ logs
link<n>-<job>-hq-server.log and -hq-worker.log.
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
import tomllib
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import Any, Protocol

EXIT_STOPPED = 3
HQ_VERSION = "0.26.2"  # keep in sync with flake.nix and scripts/get-hq.sh
# The project checkout this module runs from (src/gutsrun/slurm.py): sites/,
# .venv and bin/ live there.
ROOT = Path(__file__).resolve().parents[2]

log = logging.getLogger("gutsrun.slurm")


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
        (the venv copy's interpreter, or the project venv's)."""
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
    root: str = str(ROOT)  # project checkout: sites/, .venv, bin/

    @property
    def dir(self) -> Path:
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
        self.path.write_text(json.dumps(asdict(self), indent=2) + "\n")

    @classmethod
    def load(cls, path: str | Path) -> "Spec":
        return cls(**json.loads(Path(path).read_text()))

    def make_workload(self) -> Workload:
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


class Site:
    """sites/<name>.toml; sites/template.toml documents every key. The
    environment variables GUTSRUN_LOCAL_DISK and GUTSRUN_CORES override it,
    for tests and for trying a link on a machine without Slurm."""

    KEYS = {"slurm": {"partition", "cores_per_node", "exclusive", "max_walltime_day"},
            "node": {"local_venv", "local_disk"}}

    def __init__(self, root: str | Path, name: str):
        path = Path(root) / "sites" / f"{name}.toml"
        if not path.exists():
            known = sorted(p.stem for p in path.parent.glob("*.toml") if p.stem != "template")
            raise ChainError(f"no site {name!r}; known: {', '.join(known)} "
                             "(a new one starts as a copy of sites/template.toml)")
        data = tomllib.loads(path.read_text())
        for table, keys in data.items():
            if table not in self.KEYS:
                raise ChainError(f"{path}: unknown table [{table}]")
            if unknown := set(keys) - self.KEYS[table]:
                raise ChainError(f"{path}: unknown in [{table}]: {', '.join(sorted(unknown))}")
        slurm, node = data.get("slurm", {}), data.get("node", {})
        self.partition: str = slurm["partition"]
        self.cores: int = int(os.environ.get("GUTSRUN_CORES") or slurm["cores_per_node"])
        self.exclusive: bool = slurm.get("exclusive", True)
        if not self.exclusive:
            raise ChainError(f"{path}: exclusive = false is not supported yet: the HQ "
                             "worker would claim every core of a node it shares")
        self.max_walltime_day: str | None = slurm.get("max_walltime_day")
        self.local_venv: bool = node.get("local_venv", False)
        self.local_disk: str | None = (os.environ.get("GUTSRUN_LOCAL_DISK")
                                       or node.get("local_disk"))
        if self.local_venv and not self.local_disk:
            raise ChainError(f"{path}: local_venv = true needs local_disk")


def setup_logging() -> None:
    """Log to stdout as "[HH:MM:SS] message": a link's stdout is its Slurm log."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%H:%M:%S"))
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.propagate = False


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
    out = subprocess.run(["git", "-C", str(root), "describe", "--always", "--dirty"],
                         capture_output=True, text=True)
    return out.stdout.strip() or "unknown"


# --- the record and the queue ----------------------------------------------------------


def note(spec: Spec, event: str, **fields: Any) -> None:
    """Append an event to the chain's record, links.jsonl."""
    entry = {"time": datetime.now().isoformat(timespec="seconds"), "event": event, **fields}
    with open(spec.record, "a") as f:
        f.write(json.dumps(entry) + "\n")


def events(spec: Spec) -> list[dict[str, Any]]:
    """The record of the chain last started: events since its "chain" event."""
    if not spec.record.exists():
        return []
    entries = [json.loads(line) for line in spec.record.read_text().splitlines() if line]
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


# --- the venv copy on node-local disk ------------------------------------------------


def venv_stale(root: str | Path) -> bool:
    """True if bin/venv.tar is missing or older than the venv. pyvenv.cfg
    changes when the venv is created, site-packages when a package is
    installed or removed."""
    root = Path(root)
    tarball = root / "bin" / "venv.tar"
    if not tarball.exists():
        return True
    sources = [root / ".venv" / "pyvenv.cfg", *root.glob(".venv/lib/python3*/site-packages"),
               root / "requirements-lock.txt"]
    return any(p.exists() and p.stat().st_mtime > tarball.stat().st_mtime for p in sources)


def pack_venv(root: str | Path) -> None:
    """Pack .venv into bin/venv.tar, via a temporary file, so a running job
    never reads a half-written tarball."""
    root = Path(root)
    (root / "bin").mkdir(exist_ok=True)
    tmp = root / "bin" / ".venv.tar.tmp"
    subprocess.run(["tar", "-C", str(root), "-cf", str(tmp), ".venv"], check=True)
    os.replace(tmp, root / "bin" / "venv.tar")


def sweep_venvs(base: Path, user: str) -> None:
    """Remove the venv copies of this user's jobs that are no longer queued.
    Removes nothing if squeue does not answer."""
    jobs = queue(user)
    if jobs is None:
        return
    prefix = f"{user}-gutsrun-"
    for copy in base.glob(prefix + "*"):
        if copy.is_dir() and copy.name.removeprefix(prefix) not in jobs:
            shutil.rmtree(copy, ignore_errors=True)
            log.info(f"removed stale {copy}")


@contextlib.contextmanager
def local_venv(root: str | Path, local_disk: str, job: str) -> Iterator[str]:
    """Unpack the venv to node-local disk for the `with` block; yields the
    copy's Python and removes the copy when the block ends, also on errors."""
    root = Path(root)
    if venv_stale(root):
        raise ChainError("bin/venv.tar is missing or older than .venv; "
                         "run `.venv/bin/python -m gutsrun.slurm pack`")
    user = getpass.getuser()  # $LOGNAME/$USER, else the password database
    base = Path(local_disk)
    sweep_venvs(base, user)
    target = base / f"{user}-gutsrun-{job}"
    started = time.monotonic()
    try:
        target.mkdir(parents=True)
        subprocess.run(["tar", "-C", str(target), "-xf", str(root / "bin" / "venv.tar")],
                       check=True)
    except (OSError, subprocess.CalledProcessError) as e:
        shutil.rmtree(target, ignore_errors=True)
        raise ChainError(f"unpacking the venv to {target} failed: {e}") from e
    log.info(f"venv unpacked to {target} in {time.monotonic() - started:.1f} s")
    try:
        yield str(target / ".venv" / "bin" / "python")
    finally:
        shutil.rmtree(target, ignore_errors=True)


# --- HQ inside the job ---------------------------------------------------------------


def wait_listed(directory: Path, name: str, timeout: float) -> bool:
    """Wait until `name` shows up in a listing of `directory`.

    Polls: a file-change notifier (inotify) sees only changes made on the
    same machine, not files another node writes over NFS. And it lists the
    directory instead of retrying the name, because NFS can keep answering
    "no such file" for a name it looked up before the file existed, for
    30 s or more; a listing shows a new file within one poll."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if name in os.listdir(directory):
            return True
        time.sleep(0.2)
    return False


@contextlib.contextmanager
def hq_server(root: str | Path, server: Path, log_prefix: str, cores: int) -> Iterator[str]:
    """Run an HQ server and one worker on this node for the `with` block;
    yields the directory of the hq binary. Stops both and removes the
    server directory when the block ends, also on errors."""
    bundled = Path(root) / "bin" / "hq"
    hq = str(bundled) if bundled.exists() else shutil.which("hq")
    if hq is None:
        raise ChainError("no hq in bin/ or on PATH (scripts/get-hq.sh)")
    version = subprocess.run([hq, "--version"], capture_output=True, text=True).stdout.strip()
    if not version.endswith(f"v{HQ_VERSION}"):
        raise ChainError(f"need hq {HQ_VERSION}, have: {version}")
    shutil.rmtree(server, ignore_errors=True)
    # Created here, so that the wait below lists an existing directory.
    server.mkdir(parents=True)
    processes: list[subprocess.Popen[bytes]] = []
    try:
        with open(f"{log_prefix}-hq-server.log", "wb") as out:
            processes.append(subprocess.Popen(
                [hq, "server", "start", "--server-dir", str(server)],
                stdout=out, stderr=subprocess.STDOUT))
        if not wait_listed(server, "hq-current", 60):
            raise ChainError(f"hq server did not start; see {log_prefix}-hq-server.log")
        with open(f"{log_prefix}-hq-worker.log", "wb") as out:
            processes.append(subprocess.Popen(
                [hq, "worker", "start", "--server-dir", str(server), "--cpus", str(cores)],
                stdout=out, stderr=subprocess.STDOUT))
        log.info(f"hq server and worker started in {server}")
        yield str(Path(hq).parent)
    finally:
        subprocess.run([hq, "server", "stop", "--server-dir", str(server)],
                       capture_output=True)
        for process in processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        shutil.rmtree(server, ignore_errors=True)


# --- submitting, status, stopping ------------------------------------------------------


def sbatch_argv(spec: Spec, site: Site, link: int, after: str | None = None) -> list[str]:
    """The sbatch command for one link, `after` the job it follows. `exec`
    makes Python the job's main process, which gets Slurm's signal."""
    python = Path(spec.root) / ".venv" / "bin" / "python"
    command = (f"exec {shlex.quote(str(python))} -m gutsrun.slurm link "
               f"{shlex.quote(str(spec.path))} {link}")
    argv = ["sbatch", "--parsable", f"--job-name=gutsrun-{spec.name}-{link}",
            f"--partition={site.partition}", "--nodes=1", f"--time={spec.time}",
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


def start(spec: Spec, *, night: bool = False, submit_first: bool = True) -> str:
    """Check and save a new chain in an existing run directory and submit
    its first link; returns the job id. With `submit_first=False`, returns
    the spec's path instead, to run `python -m gutsrun.slurm link <spec> 1`
    by hand. A run directory can take a new chain once the last one has
    no jobs left in the queue."""
    root = Path(spec.root)
    site = Site(root, spec.site)
    check_time(spec, site, night)
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
    progress = workload.progress(Path(spec.rundir))  # StateError: let the caller say
    if progress.done:
        raise ChainError(f"the run in {spec.rundir} is done: {progress.text}")
    if workload.needs_hq and not ((root / "bin" / "hq").exists() or shutil.which("hq")):
        raise ChainError("no hq in bin/ or on PATH; run scripts/get-hq.sh")
    rev = revision(root)
    if rev == "unknown":
        log.warning("warning: no REVISION file and no git; the runs will not record "
                    "which code they ran (scripts/sync.sh push writes REVISION)")
    if site.local_venv and venv_stale(root):
        log.info("packing .venv into bin/venv.tar")
        pack_venv(root)
    spec.dir.mkdir(exist_ok=True)
    spec.stop_file.unlink(missing_ok=True)
    spec.save()
    note(spec, "chain", name=spec.name, site=spec.site, time=spec.time, links=spec.links,
         revision=rev, progress=progress.text)
    return submit(spec, site, 1) if submit_first else str(spec.path)


def status(spec: Spec) -> tuple[list[str], bool]:
    """A report on the chain, one line each: the run's progress, then each
    link. Also whether any of its jobs is still queued or running (True
    if squeue does not answer)."""
    try:
        lines = [f"{spec.name} ({spec.site}): "
                 f"{spec.make_workload().progress(Path(spec.rundir)).text}"]
    except StateError as e:
        lines = [f"{spec.name} ({spec.site}): {e}"]
    if spec.stop_file.exists():
        lines[0] += "; stopped by request"
    record = events(spec)
    ended = {(e["link"], e["job"]): e for e in record if e["event"] == "ended"}
    started = {(e["link"], e["job"]): e for e in record if e["event"] == "started"}
    queued = queue()
    if queued is None:
        lines.append("(squeue does not answer)")
    active = queued is None
    for link, job in chain_jobs(spec):
        head = f"link {link:>2}  job {job:<10}"
        if queued and job in queued:
            active = True
            lines.append(f"{head} {queued[job]}")
        elif (link, job) in ended:
            e = ended[(link, job)]
            lines.append(f"{head} ended {e['time']}: {e['result']}, {e['progress']}")
        elif (link, job) in started:
            lines.append(f"{head} gone without a word after starting on "
                         f"{started[(link, job)]['host']}; see {spec.dir}/link{link}-{job}.out")
        elif queued is not None:
            lines.append(f"{head} cancelled before it started")
    return lines, active


def follow(spec: Spec, interval: float = 30) -> None:
    """Print the status whenever it changes, until no job of the chain is left."""
    last: list[str] = []
    while True:
        lines, active = status(spec)
        if lines != last:
            print(f"--- {datetime.now():%H:%M:%S}", *lines, sep="\n", flush=True)
            last = lines
        if not active:
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


def run_link(spec_path: str | Path, link: int) -> int:
    """Run link number `link` of a chain (steps 1-7 in the module docstring);
    returns the job's exit code."""
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

    next_job = None
    if link < spec.links and shutil.which("sbatch"):
        try:
            next_job = submit(spec, site, link + 1, after=job)
            log.info(f"next link: job {next_job} (afterany:{job})")
        except subprocess.CalledProcessError as e:
            log.info(f"could not submit the next link ({e.stderr.strip()}); "
                     "this one runs anyway")

    def cancel_next(reason: str) -> None:
        if next_job is not None:
            subprocess.run(["scancel", next_job])
            log.info(f"cancelled next link {next_job} ({reason})")

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    log.info(f"open files: {soft} -> {hard}")

    exit_code = EXIT_STOPPED
    try:
        with contextlib.ExitStack() as stack:
            python = str(Path(spec.root) / ".venv" / "bin" / "python")
            if site.local_venv:
                assert site.local_disk  # Site checks it
                python = stack.enter_context(local_venv(spec.root, site.local_disk, job))
            env = dict(os.environ)
            if workload.needs_hq:
                server = spec.dir / f"hq-{job}"
                hq_dir = stack.enter_context(hq_server(spec.root, server, log_prefix,
                                                       site.cores))
                env["HQ_SERVER_DIR"] = str(server)
                env["PATH"] = os.pathsep.join([hq_dir, env.get("PATH", "")])
            argv = workload.argv(python, rundir, link)
            if forwarder.received:
                log.info("not starting the workload")
            else:
                log.info(f"running: {shlex.join(argv)}")
                forwarder.child = subprocess.Popen(argv, cwd=rundir, env=env)
                exit_code = forwarder.child.wait()  # resumes after a forwarded signal
                forwarder.child = None
                log.info(f"workload exit {exit_code}")
    except ChainError as e:
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
            log.info(f"stopped at {progress.text}; the chain was stopped")
            cancel_next("chain stopped")
            return end(0, "stopped by request", progress.text)
        then = (f"link {link + 1} continues" if next_job
                else f"no further link (links={spec.links})")
        log.info(f"stopped at {progress.text}; {then}")
        return end(0, "stopped at the walltime", progress.text)
    log.info(f"workload failed (exit {exit_code}) at {progress.text}; chain stops")
    cancel_next("failed")
    return end(1, f"failed (exit {exit_code})", progress.text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    link = commands.add_parser("link", help="run one link (inside its Slurm job)")
    link.add_argument("spec", type=Path)
    link.add_argument("link", type=int)
    pack = commands.add_parser("pack", help="pack .venv into bin/venv.tar")
    pack.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args(argv)
    setup_logging()
    if args.command == "link":
        return run_link(args.spec, args.link)
    pack_venv(args.root)
    print(args.root / "bin" / "venv.tar")
    return 0


if __name__ == "__main__":
    # Through the imported module: workloads raise gutsrun.slurm.StateError,
    # which is not this __main__ module's class of the same name.
    from gutsrun.slurm import main as imported_main
    sys.exit(imported_main())
