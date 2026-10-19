"""Resource Manager v0: one run as a chain of Slurm jobs ("links").

Generic: what runs and how far it got is the workload's business. The
chain's spec file names the workload as "module:factory" plus its keyword
arguments; `gutsrun_infretis.chain` is the infretis one, and also the
command that starts a chain (it calls `start()`).

Standard library only: a link runs this with the site's system Python
(`[python] interpreter`), before the venv is unpacked.

    python3.12 -m gutsrun.slurm link <spec.json> <n>   (what a link job runs)
    python3.12 -m gutsrun.slurm pack                   (bin/venv.tar from .venv)

Each link:

1. link > 1: stops if the workload's state is unreadable (exit 1) or done
   (exit 0);
2. pre-submits the next link with afterany, unless it is the last, so a
   link that is killed or cancelled is still followed;
3. raises the soft open-files limit to the hard one;
4. with `local_venv`, runs a copy of the venv unpacked from bin/venv.tar to
   node-local disk (plan decision 15), after removing the copies of jobs
   that are gone (a walltime kill skips the cleanup);
5. if the workload needs HQ: a server and one worker inside the job, in a
   fresh server dir on the shared FS, without a journal;
6. runs the workload's command, passing SIGTERM (Slurm's
   `--signal=B:TERM@<margin>`; it may come up to 60 s early) and SIGINT on;
7. cancels the next link when the workload is done or failed. A workload
   exits EXIT_STOPPED when a signal stopped it and it can continue.

Waiting for a file that another node writes lists the directory and never
retries the name: on NFS, a failed lookup can stay cached for 30 s or more
(ASSUMPTIONS.md).

Outputs in runs/<site>/: <name>/ (the run), <name>.chain.json (the spec),
<name>-link<n>-<job>.out (the link's log, with the workload's output),
-hq-server.log, -hq-worker.log.
"""

import argparse
import contextlib
import getpass
import importlib
import json
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
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Protocol

EXIT_STOPPED = 3
HQ_VERSION = "0.26.2"  # keep in sync with flake.nix and scripts/get-hq.sh


class ChainError(Exception):
    """A chain cannot start, or a link cannot run its workload."""


class StateError(Exception):
    """The workload's state is missing or unreadable."""


@dataclass
class Progress:
    done: bool
    text: str  # e.g. "step 526 of 1000"


class Workload(Protocol):
    needs_hq: bool

    def progress(self, rundir: Path) -> Progress:
        """How far the run got; raises StateError if that cannot be read."""

    def before_link(self, rundir: Path, link: int) -> None:
        """Called by links after the first, before anything else runs."""

    def argv(self, python: str, rundir: Path, link: int) -> list[str]:
        """The command one link runs, from the project root."""


@dataclass
class Spec:
    root: str  # project root: sites/, runs/, .venv, bin/
    site: str
    name: str
    workload: str  # "module:factory"
    args: dict = field(default_factory=dict)
    time: str = "00:14:00"
    margin: int = 90  # seconds before the walltime that SIGTERM is asked for
    links: int = 3

    @property
    def runs(self) -> Path:
        return Path(self.root) / "runs" / self.site

    @property
    def rundir(self) -> Path:
        return self.runs / self.name

    @property
    def path(self) -> Path:
        return self.runs / f"{self.name}.chain.json"

    def save(self):
        self.path.write_text(json.dumps(asdict(self), indent=2) + "\n")

    @classmethod
    def load(cls, path) -> "Spec":
        return cls(**json.loads(Path(path).read_text()))

    def make_workload(self) -> Workload:
        module, _, factory = self.workload.partition(":")
        return getattr(importlib.import_module(module), factory)(**self.args)


class Site:
    """sites/<name>.toml. GUTSRUN_LOCAL_DISK and GUTSRUN_CORES override it
    (tests, and trying a link on a machine without Slurm)."""

    def __init__(self, root, name):
        self.data = tomllib.loads((Path(root) / "sites" / f"{name}.toml").read_text())
        slurm, python = self.data["slurm"], self.data["python"]
        self.partition = slurm["partition"]
        self.exclusive = slurm.get("exclusive", False)
        self.max_walltime_day = slurm["max_walltime_day"]
        self.cores = int(os.environ.get("GUTSRUN_CORES") or slurm["cores_per_node"])
        self.interpreter = python["interpreter"]
        self.local_venv = python.get("local_venv", False)
        self.local_disk = os.environ.get("GUTSRUN_LOCAL_DISK") or self.data["paths"]["local_disk"]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def seconds(hms: str) -> int:
    h, m, s = (int(x) for x in hms.split(":"))
    return h * 3600 + m * 60 + s


def check_time(spec: Spec, site: Site, night: bool = False):
    if not night and seconds(spec.time) >= seconds(site.max_walltime_day):
        raise ChainError(f"time {spec.time} is not below the daytime limit "
                         f"{site.max_walltime_day} (allow with night=True / --night)")
    if seconds(spec.time) <= spec.margin + 60:
        raise ChainError(f"time {spec.time} leaves no time to run before SIGTERM "
                         f"(margin {spec.margin} s, which Slurm may bring forward by 60 s)")


def revision(root) -> str:
    """REVISION (written by scripts/sync.sh push on a cluster copy), else git."""
    path = Path(root) / "REVISION"
    if path.exists():
        return path.read_text().strip()
    out = subprocess.run(["git", "-C", str(root), "describe", "--always", "--dirty"],
                         capture_output=True, text=True)
    return out.stdout.strip() or "unknown"


# --- the venv copy (plan decision 15) ----------------------------------------------


def venv_stale(root) -> bool:
    """bin/venv.tar is missing or older than the venv. pyvenv.cfg changes when
    the venv is created, site-packages when a package is installed or removed."""
    root = Path(root)
    tarball = root / "bin" / "venv.tar"
    if not tarball.exists():
        return True
    newer = [root / ".venv" / "pyvenv.cfg", *root.glob(".venv/lib/python3*/site-packages"),
             root / "requirements-lock.txt"]
    return any(p.exists() and p.stat().st_mtime > tarball.stat().st_mtime for p in newer)


def pack_venv(root):
    """.venv -> bin/venv.tar, via a temp file: a running job never reads half of it."""
    root = Path(root)
    (root / "bin").mkdir(exist_ok=True)
    tmp = root / "bin" / ".venv.tar.tmp"
    subprocess.run(["tar", "-C", str(root), "-cf", str(tmp), ".venv"], check=True)
    os.replace(tmp, root / "bin" / "venv.tar")


def sweep_venvs(base: Path, user: str):
    """Remove venv copies of this user's jobs that are no longer in squeue."""
    try:
        mine = set(subprocess.run(["squeue", "-h", "-u", user, "-o", "%i"], capture_output=True,
                                  text=True, check=True).stdout.split())
    except (FileNotFoundError, subprocess.CalledProcessError):
        return  # no answer is not "no jobs": never remove a running job's copy
    prefix = f"{user}-gutsrun-"
    for d in base.glob(prefix + "*"):
        if d.is_dir() and d.name.removeprefix(prefix) not in mine:
            shutil.rmtree(d, ignore_errors=True)
            log(f"removed stale {d}")


@contextlib.contextmanager
def local_venv(root, site: Site, job: str):
    """Yields the python of a venv copy on node-local disk; removes it after."""
    root = Path(root)
    if venv_stale(root):
        raise ChainError("bin/venv.tar is missing or older than .venv; "
                         "run `python3.12 -m gutsrun.slurm pack`")
    user = getpass.getuser()
    base = Path(site.local_disk)
    sweep_venvs(base, user)
    target = base / f"{user}-gutsrun-{job}"
    try:
        t0 = time.monotonic()
        target.mkdir(parents=True)
        subprocess.run(["tar", "-C", str(target), "-xf", str(root / "bin" / "venv.tar")],
                       check=True)
    except (OSError, subprocess.CalledProcessError) as e:
        shutil.rmtree(target, ignore_errors=True)
        raise ChainError(f"unpacking the venv to {target} failed: {e}") from e
    log(f"venv unpacked to {target} in {time.monotonic() - t0:.1f} s")
    try:
        yield str(target / ".venv" / "bin" / "python")
    finally:
        shutil.rmtree(target, ignore_errors=True)


# --- HQ inside the job (plan decisions 3 and 7) -----------------------------------


def wait_listed(directory: Path, name: str, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if name in os.listdir(directory):
            return True
        time.sleep(0.2)
    return False


@contextlib.contextmanager
def hq_server(root, server: Path, log_prefix: str, cores: int):
    """An HQ server and one worker on this node; yields the hq binary's directory."""
    local = Path(root) / "bin" / "hq"
    hq = str(local) if local.exists() else shutil.which("hq")
    if hq is None:
        raise ChainError("no hq in bin/ or on PATH (scripts/get-hq.sh)")
    version = subprocess.run([hq, "--version"], capture_output=True, text=True).stdout.strip()
    if not version.endswith(f"v{HQ_VERSION}"):
        raise ChainError(f"need hq {HQ_VERSION}, have: {version}")
    shutil.rmtree(server, ignore_errors=True)
    # Created here, so that the wait below lists an existing directory.
    server.mkdir(parents=True)
    procs = []
    try:
        with open(f"{log_prefix}-hq-server.log", "wb") as out:
            procs.append(subprocess.Popen([hq, "server", "start", "--server-dir", str(server)],
                                          stdout=out, stderr=subprocess.STDOUT))
        if not wait_listed(server, "hq-current", 60):
            raise ChainError(f"hq server did not start; see {log_prefix}-hq-server.log")
        with open(f"{log_prefix}-hq-worker.log", "wb") as out:
            procs.append(subprocess.Popen(
                [hq, "worker", "start", "--server-dir", str(server), "--cpus", str(cores)],
                stdout=out, stderr=subprocess.STDOUT))
        log(f"hq server and worker started in {server}")
        yield str(Path(hq).parent)
    finally:
        subprocess.run([hq, "server", "stop", "--server-dir", str(server)],
                       capture_output=True)
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        shutil.rmtree(server, ignore_errors=True)


# --- submitting -------------------------------------------------------------------


def sbatch_argv(spec: Spec, site: Site, link: int, after: str | None = None) -> list[str]:
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
    out = subprocess.run(sbatch_argv(spec, site, link, after), capture_output=True,
                         text=True, check=True).stdout
    return out.strip().split(";")[0]  # --parsable: "<id>[;<cluster>]"


def start(spec: Spec, *, night: bool = False, submit_first: bool = True) -> str:
    """Check, write the spec, submit link 1; returns its job id (or the spec's
    path with submit_first=False, to run `link <spec> 1` by hand)."""
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
    if site.local_venv and venv_stale(root):
        log("packing .venv into bin/venv.tar")
        pack_venv(root)
    spec.runs.mkdir(parents=True, exist_ok=True)
    spec.save()
    return submit(spec, site, 1) if submit_first else str(spec.path)


# --- one link ---------------------------------------------------------------------


class Forwarder:
    """SIGTERM/SIGINT go to the workload; before it starts, they keep it from starting."""

    def __init__(self):
        self.child: subprocess.Popen | None = None
        self.received: str | None = None
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self.handle)

    def handle(self, signum, frame):
        self.received = signal.Signals(signum).name
        log(f"{self.received}: stopping the workload")
        if self.child is not None:
            self.child.send_signal(signum)


def run_link(spec_path, link: int) -> int:
    forward = Forwarder()
    spec = Spec.load(spec_path)
    site = Site(spec.root, spec.site)
    workload = spec.make_workload()
    job = os.environ.get("SLURM_JOB_ID", f"nojob-{os.getpid()}")
    prefix = str(spec.runs / f"{spec.name}-link{link}-{job}")
    log(f"link {link}/{spec.links} of {spec.name} on {socket.gethostname()}, job {job}, "
        f"repo {revision(spec.root)}")

    if link > 1:
        try:
            progress = workload.progress(spec.rundir)
        except StateError as e:
            log(f"{e}; chain stops")
            return 1
        if progress.done:
            log(f"done: {progress.text}")
            return 0
        log(f"continuing at {progress.text}")
        workload.before_link(spec.rundir, link)

    nxt = None
    if link < spec.links and shutil.which("sbatch"):
        try:
            nxt = submit(spec, site, link + 1, after=job)
            log(f"next link: job {nxt} (afterany:{job})")
        except subprocess.CalledProcessError as e:
            log(f"could not submit the next link ({e.stderr.strip()}); this one runs anyway")

    def cancel_next(why):
        if nxt is not None:
            subprocess.run(["scancel", nxt])
            log(f"cancelled next link {nxt} ({why})")

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    log(f"open files: {soft} -> {hard}")

    rc = EXIT_STOPPED
    try:
        with contextlib.ExitStack() as stack:
            python = str(Path(spec.root) / ".venv" / "bin" / "python")
            if site.local_venv:
                python = stack.enter_context(local_venv(spec.root, site, job))
            env = dict(os.environ)
            if workload.needs_hq:
                server = spec.runs / f"{spec.name}-hq-{job}"
                hq_dir = stack.enter_context(hq_server(spec.root, server, prefix, site.cores))
                env["HQ_SERVER_DIR"] = str(server)
                env["PATH"] = os.pathsep.join([hq_dir, env.get("PATH", "")])
            argv = workload.argv(python, spec.rundir, link)
            if forward.received:
                log("not starting the workload")
            else:
                log(f"running: {shlex.join(argv)}")
                forward.child = subprocess.Popen(argv, cwd=spec.root, env=env)
                rc = forward.child.wait()  # a forwarded signal does not end the wait
                forward.child = None
                log(f"workload exit {rc}")
    except ChainError as e:
        log(str(e))
        cancel_next("failed")
        return 1

    try:
        progress = workload.progress(spec.rundir)
    except StateError as e:
        log(f"{e} after the run; chain stops")
        cancel_next("failed")
        return 1
    if progress.done:
        log(f"done: {progress.text}")
        cancel_next("done")
        return 0
    if rc == EXIT_STOPPED:
        then = f"link {link + 1} continues" if nxt else f"no further link (links={spec.links})"
        log(f"stopped at {progress.text}; {then}")
        return 0
    log(f"workload failed (exit {rc}) at {progress.text}; chain stops")
    cancel_next("failed")
    return 1


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("link", help="run one link (inside its Slurm job)")
    s.add_argument("spec", type=Path)
    s.add_argument("link", type=int)
    s = sub.add_parser("pack", help="pack .venv into bin/venv.tar")
    s.add_argument("--root", type=Path, default=Path.cwd())
    args = p.parse_args(argv)
    if args.cmd == "link":
        return run_link(args.spec, args.link)
    pack_venv(args.root)
    print(args.root / "bin" / "venv.tar")
    return 0


if __name__ == "__main__":
    # Through the imported module: workloads raise gutsrun.slurm.StateError,
    # which is not this __main__ module's class of the same name.
    from gutsrun.slurm import main as imported_main
    sys.exit(imported_main())
