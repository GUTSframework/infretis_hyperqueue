"""Run one long job as a chain of Slurm jobs ("links"), each continuing
where the previous one stopped.

Generic: what runs, and how far it got, is up to a *workload* (see
`Workload`). A chain's spec file names the workload's class as
`package.module:Class`, e.g. `gutsrun_infretis.chain:InfretisRun`, plus the
keyword arguments to create it with. `gutsrun_infretis.chain` also provides
the command that starts an infretis chain; it calls `start()`.

Standard library only: a link runs this module with the cluster's system
Python (the site file's `[python] interpreter`), before the project's venv
is unpacked.

    python3.12 -m gutsrun.slurm link <spec.json> <n>   (what a link job runs)
    python3.12 -m gutsrun.slurm pack                   (bin/venv.tar from .venv)

Each link:

1. Link > 1: stops if the workload's state is unreadable (exit 1) or the
   run is done (exit 0).
2. Submits the next link right away with the dependency `afterany` on this
   one: Slurm starts it once this job has ended in any state (finished,
   failed, cancelled, timed out). A link that is killed is still followed.
3. Raises its limit on open files: the soft limit (1024 on DAS-6 compute
   nodes) is the one in force, and a process may raise it up to the hard
   limit. An HQ server and worker with many tasks hold many files and
   sockets at once.
4. If the site sets `local_venv`, unpacks bin/venv.tar to node-local disk
   and runs that copy: starting Python from a node-local disk is faster
   than from NFS, and spares the file server. First removes copies left by
   jobs that are gone (a job killed at its walltime skips its cleanup).
5. If the workload needs HQ: starts an HQ server and one worker inside the
   job, in a fresh server directory on the shared file system, without a
   journal. The workload's own state is what a restart continues from.
6. Runs the workload's command, passing SIGTERM and SIGINT on. Slurm sends
   SIGTERM `margin` seconds before the walltime (`--signal=B:TERM@margin`),
   and may send it up to 60 s earlier than asked.
7. Cancels the next link if the run is done or failed. A workload exits
   with EXIT_STOPPED when a signal stopped it and it can continue.

Outputs in runs/<site>/: <name>/ (the run directory), <name>.chain.json (the
spec), <name>-link<n>-<job>.out (the link's log, including the workload's
output), and the HQ logs <name>-link<n>-<job>-hq-server.log and -hq-worker.log.
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
from pathlib import Path
from types import FrameType
from typing import Protocol

EXIT_STOPPED = 3
HQ_VERSION = "0.26.2"  # keep in sync with flake.nix and scripts/get-hq.sh

log = logging.getLogger("gutsrun.slurm")


class ChainError(Exception):
    """A chain cannot start, or a link cannot run its workload."""


class StateError(Exception):
    """The workload's state is missing or unreadable."""


@dataclass
class Progress:
    """How far a run got."""

    done: bool
    text: str  # for the log, e.g. "step 526 of 1000"


class Workload(Protocol):
    """What a chain runs. Created from the spec's `workload` and `args` in
    every link, so it must be importable with the standard library only."""

    needs_hq: bool  # start an HQ server and worker in each link

    def progress(self, rundir: Path) -> Progress:
        """How far the run got; raises StateError if that cannot be read."""
        ...

    def before_link(self, rundir: Path, link: int) -> None:
        """Called in links after the first, before anything else runs."""
        ...

    def argv(self, python: str, rundir: Path, link: int) -> list[str]:
        """The command one link runs, from the project root, with `python`
        (the venv copy's interpreter, or the project venv's)."""
        ...


@dataclass
class Spec:
    """One chain, saved as runs/<site>/<name>.chain.json for its links."""

    root: str  # project root: holds sites/, runs/, .venv and bin/
    site: str  # sites/<site>.toml
    name: str
    workload: str  # "package.module:Class"
    args: dict = field(default_factory=dict)  # keyword arguments for the workload
    time: str = "00:14:00"  # walltime per link, in Slurm's format
    margin: int = 90  # seconds before the walltime that SIGTERM is asked for
    links: int = 3  # most links in the chain

    @property
    def runs(self) -> Path:
        return Path(self.root) / "runs" / self.site

    @property
    def rundir(self) -> Path:
        return self.runs / self.name

    @property
    def path(self) -> Path:
        return self.runs / f"{self.name}.chain.json"

    def save(self) -> None:
        self.path.write_text(json.dumps(asdict(self), indent=2) + "\n")

    @classmethod
    def load(cls, path: str | Path) -> "Spec":
        return cls(**json.loads(Path(path).read_text()))

    def make_workload(self) -> Workload:
        module, _, name = self.workload.partition(":")
        return getattr(importlib.import_module(module), name)(**self.args)


class Site:
    """sites/<name>.toml. The environment variables GUTSRUN_LOCAL_DISK and
    GUTSRUN_CORES override it, for tests and for trying a link on a machine
    without Slurm."""

    def __init__(self, root: str | Path, name: str):
        self.data = tomllib.loads((Path(root) / "sites" / f"{name}.toml").read_text())
        slurm, python = self.data["slurm"], self.data["python"]
        self.partition: str = slurm["partition"]
        self.exclusive: bool = slurm.get("exclusive", False)
        self.max_walltime_day: str = slurm["max_walltime_day"]
        self.cores: int = int(os.environ.get("GUTSRUN_CORES") or slurm["cores_per_node"])
        self.interpreter: str = python["interpreter"]
        self.local_venv: bool = python.get("local_venv", False)
        self.local_disk: str = (os.environ.get("GUTSRUN_LOCAL_DISK")
                                or self.data["paths"]["local_disk"])


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


def check_time(spec: Spec, site: Site, night: bool = False) -> None:
    """Refuse a walltime at or over the site's daytime limit (unless `night`),
    or too short to run anything before SIGTERM."""
    if not night and seconds(spec.time) >= seconds(site.max_walltime_day):
        raise ChainError(f"time {spec.time} is not below the daytime limit "
                         f"{site.max_walltime_day} (allow with --night)")
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
    """Remove the venv copies of this user's jobs that are no longer in squeue.
    Removes nothing if squeue does not answer: that is not "no jobs"."""
    try:
        listed = subprocess.run(["squeue", "-h", "-u", user, "-o", "%i"], capture_output=True,
                                text=True, check=True).stdout
    except (FileNotFoundError, subprocess.CalledProcessError):
        return
    running = set(listed.split())
    prefix = f"{user}-gutsrun-"
    for copy in base.glob(prefix + "*"):
        if copy.is_dir() and copy.name.removeprefix(prefix) not in running:
            shutil.rmtree(copy, ignore_errors=True)
            log.info(f"removed stale {copy}")


@contextlib.contextmanager
def local_venv(root: str | Path, site: Site, job: str) -> Iterator[str]:
    """Unpack the venv to node-local disk for the `with` block; yields the
    copy's Python and removes the copy when the block ends, also on errors."""
    root = Path(root)
    if venv_stale(root):
        raise ChainError("bin/venv.tar is missing or older than .venv; "
                         "run `python3.12 -m gutsrun.slurm pack`")
    user = getpass.getuser()  # $LOGNAME/$USER, else the password database
    base = Path(site.local_disk)
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
    processes: list[subprocess.Popen] = []
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


# --- submitting ------------------------------------------------------------------------


def sbatch_argv(spec: Spec, site: Site, link: int, after: str | None = None) -> list[str]:
    """The sbatch command for one link, `after` the job it follows.

    `--wrap` gives the job's command line, which sbatch puts into a /bin/sh
    script; the list is passed without a shell, so the whole of `--wrap=...`
    arrives as one argument, and the paths inside are quoted for that sh.
    `exec` makes Python the job's main process, which gets Slurm's signal."""
    command = (f"exec env PYTHONPATH={shlex.quote(str(Path(spec.root) / 'src'))} "
               f"{shlex.quote(site.interpreter)} -m gutsrun.slurm link "
               f"{shlex.quote(str(spec.path))} {link}")
    argv = ["sbatch", "--parsable", f"--job-name=gutsrun-{spec.name}-{link}",
            f"--partition={site.partition}", "--nodes=1", f"--time={spec.time}",
            f"--signal=B:TERM@{spec.margin}", f"--chdir={spec.root}",
            f"--output={spec.runs / f'{spec.name}-link{link}-%j.out'}"]
    if site.exclusive:
        argv.append("--exclusive")
    if after:
        argv.append(f"--dependency=afterany:{after}")
    return [*argv, f"--wrap={command}"]


def submit(spec: Spec, site: Site, link: int, after: str | None = None) -> str:
    """Submit one link; returns its job id."""
    out = subprocess.run(sbatch_argv(spec, site, link, after), capture_output=True,
                         text=True, check=True).stdout
    return out.strip().split(";")[0]  # --parsable prints "<id>" or "<id>;<cluster>"


def start(spec: Spec, *, night: bool = False, submit_first: bool = True) -> str:
    """Check and save a new chain and submit its first link; returns the job
    id. With `submit_first=False`, returns the spec's path instead, to run
    `python -m gutsrun.slurm link <spec> 1` by hand."""
    root = Path(spec.root)
    site = Site(root, spec.site)
    check_time(spec, site, night)
    if spec.rundir.exists() or spec.path.exists():
        raise ChainError(f"{spec.rundir} or its spec exists; pick another name")
    if not (root / ".venv" / "bin" / "python").exists():
        raise ChainError("no .venv; run scripts/setup-venv.sh first")
    workload = spec.make_workload()
    if workload.needs_hq and not ((root / "bin" / "hq").exists() or shutil.which("hq")):
        raise ChainError("no hq in bin/ or on PATH; run scripts/get-hq.sh")
    if revision(root) == "unknown":
        log.warning("warning: no REVISION file and no git; the runs will not record "
                    "which code they ran (scripts/sync.sh push writes REVISION)")
    if site.local_venv and venv_stale(root):
        log.info("packing .venv into bin/venv.tar")
        pack_venv(root)
    spec.runs.mkdir(parents=True, exist_ok=True)
    spec.save()
    return submit(spec, site, 1) if submit_first else str(spec.path)


# --- one link --------------------------------------------------------------------------


class Forwarder:
    """Passes SIGTERM and SIGINT on to the workload once it runs; one that
    arrives earlier keeps the workload from starting."""

    def __init__(self) -> None:
        self.child: subprocess.Popen | None = None
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
    site = Site(spec.root, spec.site)
    workload = spec.make_workload()
    job = os.environ.get("SLURM_JOB_ID", f"nojob-{os.getpid()}")
    log_prefix = str(spec.runs / f"{spec.name}-link{link}-{job}")
    log.info(f"link {link}/{spec.links} of {spec.name} on {socket.gethostname()}, "
             f"job {job}, repo {revision(spec.root)}")

    if link > 1:
        try:
            progress = workload.progress(spec.rundir)
        except StateError as e:
            log.info(f"{e}; chain stops")
            return 1
        if progress.done:
            log.info(f"done: {progress.text}")
            return 0
        log.info(f"continuing at {progress.text}")
        workload.before_link(spec.rundir, link)

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
                python = stack.enter_context(local_venv(spec.root, site, job))
            env = dict(os.environ)
            if workload.needs_hq:
                server = spec.runs / f"{spec.name}-hq-{job}"
                hq_dir = stack.enter_context(hq_server(spec.root, server, log_prefix,
                                                       site.cores))
                env["HQ_SERVER_DIR"] = str(server)
                env["PATH"] = os.pathsep.join([hq_dir, env.get("PATH", "")])
            argv = workload.argv(python, spec.rundir, link)
            if forwarder.received:
                log.info("not starting the workload")
            else:
                log.info(f"running: {shlex.join(argv)}")
                forwarder.child = subprocess.Popen(argv, cwd=spec.root, env=env)
                exit_code = forwarder.child.wait()  # resumes after a forwarded signal
                forwarder.child = None
                log.info(f"workload exit {exit_code}")
    except ChainError as e:
        log.info(str(e))
        cancel_next("failed")
        return 1

    try:
        progress = workload.progress(spec.rundir)
    except StateError as e:
        log.info(f"{e} after the run; chain stops")
        cancel_next("failed")
        return 1
    if progress.done:
        log.info(f"done: {progress.text}")
        cancel_next("done")
        return 0
    if exit_code == EXIT_STOPPED:
        then = (f"link {link + 1} continues" if next_job
                else f"no further link (links={spec.links})")
        log.info(f"stopped at {progress.text}; {then}")
        return 0
    log.info(f"workload failed (exit {exit_code}) at {progress.text}; chain stops")
    cancel_next("failed")
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    link = commands.add_parser("link", help="run one link (inside its Slurm job)")
    link.add_argument("spec", type=Path)
    link.add_argument("link", type=int)
    pack = commands.add_parser("pack", help="pack .venv into bin/venv.tar")
    pack.add_argument("--root", type=Path, default=Path.cwd())
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
