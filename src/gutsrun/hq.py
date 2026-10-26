"""HyperQueue inside a Slurm job: the hq binary, a server and worker on the
link's node, and workers on the job's other nodes.

Standard library only: a link imports it.
"""

import contextlib
import logging
import os
import shlex
import shutil
import signal
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

VERSION = "0.26.2"  # keep in sync with flake.nix and scripts/get-hq.sh

log = logging.getLogger("gutsrun.hq")


class HQError(Exception):
    """No suitable hq binary, or the server did not start."""


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


def binary(root: str | Path) -> str:
    """The hq binary: bin/hq in the checkout, else the one on PATH, checked
    to be VERSION."""
    bundled = Path(root) / "bin" / "hq"
    hq = str(bundled) if bundled.exists() else shutil.which("hq")
    if hq is None:
        raise HQError("no hq in bin/ or on PATH (scripts/get-hq.sh)")
    version = subprocess.run([hq, "--version"], capture_output=True, text=True).stdout.strip()
    if not version.endswith(f"v{VERSION}"):
        raise HQError(f"need hq {VERSION}, have: {version}")
    return hq


@contextlib.contextmanager
def server_and_worker(root: str | Path, server: Path, log_prefix: str, cores: int,
                      env: dict[str, str] | None = None) -> Iterator[str]:
    """Run an HQ server and one worker on this node for the `with` block;
    yields the directory of the hq binary. The worker, and so its tasks,
    runs in `env`. Stops both and removes the server directory when the
    block ends, also on errors."""
    hq = binary(root)
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
            raise HQError(f"hq server did not start; see {log_prefix}-hq-server.log")
        with open(f"{log_prefix}-hq-worker.log", "wb") as out:
            processes.append(subprocess.Popen(
                [hq, "worker", "start", "--server-dir", str(server), "--cpus", str(cores)],
                stdout=out, stderr=subprocess.STDOUT, env=env))
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


@contextlib.contextmanager
def workers_elsewhere(root: str | Path, server: Path, others: int, cores: int,
                      command: list[str], env: dict[str, str] | None = None) -> Iterator[None]:
    """`command` on each of the job's `others` other nodes for the `with`
    block, in one srun step, in `env` (srun passes the environment on to
    the step); the command runs an HQ worker there. Stops them when the
    block ends by stopping the HQ server: each worker then exits. srun does
    not pass SIGTERM on (it kills the step), so it is signalled only if the
    step does not end by itself."""
    hq = binary(root)
    # --relative=1: the nodes after the first, which runs this link.
    # --cpus-per-task: a step gets one CPU per task unless told otherwise,
    # and the worker and its tasks would be confined to it.
    argv = ["srun", f"--nodes={others}", f"--ntasks={others}", "--ntasks-per-node=1",
            "--relative=1", f"--cpus-per-task={cores}", "--kill-on-bad-exit=0", *command]
    log.info(f"hq workers on {others} more node(s): {shlex.join(argv)}")
    step = subprocess.Popen(argv, env=env)
    try:
        yield
    finally:
        subprocess.run([hq, "server", "stop", "--server-dir", str(server)],
                       capture_output=True)
        try:
            step.wait(timeout=60)
        except subprocess.TimeoutExpired:
            log.info("hq workers on the other nodes still running after 60 s; ending srun")
            step.send_signal(signal.SIGTERM)
            try:
                step.wait(timeout=30)
            except subprocess.TimeoutExpired:
                step.kill()
                step.wait()
        log.info(f"hq workers on the other nodes: srun exit {step.returncode}")
