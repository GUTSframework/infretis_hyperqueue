"""Everything HyperQueue, through its `hq` CLI: a server and worker on the
link's node, workers on the job's other nodes, and `HQBackend`, which runs
tasks on a server.

Standard library only: a link imports it.
"""

import contextlib
import json
import logging
import queue
import shlex
import shutil
import socket
import subprocess
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from gutsrun.executor import Resources

VERSION = "0.26.2"  # keep in sync with flake.nix and scripts/get-hq.sh

log = logging.getLogger("gutsrun.hq")


class HQError(Exception):
    """No suitable hq binary, the server did not start, or hq refused a call."""


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


def free_port() -> int:
    """A TCP port nothing on this node listens on now. The server binds it
    a moment later; another process taking it in between makes the server
    fail to start, and the link with it."""
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@contextlib.contextmanager
def server_and_worker(root: str | Path, server: Path, log_prefix: str, cores: int,
                      env: dict[str, str] | None = None) -> Iterator[str]:
    """Run an HQ server and one worker on this node for the `with` block;
    yields the directory of the hq binary. The worker, and so its tasks,
    runs in `env`. Stops both and removes the server directory when the
    block ends, also on errors.

    The access file (`<server>/access.json`: host, ports, keys) is written
    before the server starts, so workers on other nodes can start at any
    time: they find it, and retry every 2 s until the server listens.

    Both run in a session of their own: a Ctrl-C in the terminal goes to
    the caller, which stops its tasks before the `with` block stops HQ."""
    hq = binary(root)
    shutil.rmtree(server, ignore_errors=True)
    server.mkdir(parents=True)
    access = server / "access.json"
    subprocess.run([hq, "server", "generate-access", str(access),
                    "--host", socket.gethostname(),
                    "--client-port", str(free_port()), "--worker-port", str(free_port())],
                   capture_output=True, check=True)
    processes: list[subprocess.Popen[bytes]] = []
    try:
        with open(f"{log_prefix}-hq-server.log", "wb") as out:
            processes.append(subprocess.Popen(
                [hq, "server", "start", "--server-dir", str(server), "--access-file", str(access)],
                stdout=out, stderr=subprocess.STDOUT, start_new_session=True))
        # The server writes hq-current on this node, so no NFS cache hides it.
        deadline = time.monotonic() + 60
        while not (server / "hq-current").exists():
            if processes[0].poll() is not None or time.monotonic() > deadline:
                raise HQError(f"hq server did not start; see {log_prefix}-hq-server.log")
            time.sleep(0.1)
        with open(f"{log_prefix}-hq-worker.log", "wb") as out:
            processes.append(subprocess.Popen(
                [hq, "worker", "start", "--server-dir", str(server), "--cpus", str(cores)],
                stdout=out, stderr=subprocess.STDOUT, env=env, start_new_session=True))
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
def workers_elsewhere(root: str | Path, server: Path, log_prefix: str, others: int,
                      cores: int, env: dict[str, str] | None = None) -> Iterator[None]:
    """An HQ worker on each of the job's `others` other nodes for the `with`
    block, in one srun step, in `env` (srun passes the environment on to the
    step); each logs to `<log_prefix>-hq-worker-<node>.log`. When the block
    ends, stops the HQ server, which ends the workers, and then the step:
    srun answers SIGTERM by killing whatever of it is left."""
    hq = binary(root)
    # --relative=1: the nodes after the first, which runs this link.
    # --cpus-per-task: a step gets one CPU per task unless told otherwise,
    # and the worker and its tasks would be confined to it.
    argv = ["srun", f"--nodes={others}", f"--ntasks={others}", "--ntasks-per-node=1",
            "--relative=1", f"--cpus-per-task={cores}", "--kill-on-bad-exit=0",
            f"--output={log_prefix}-hq-worker-%N.log",
            hq, "worker", "start", "--server-dir", str(server), "--cpus", str(cores)]
    log.info(f"hq workers on {others} more node(s): {shlex.join(argv)}")
    step = subprocess.Popen(argv, env=env)
    try:
        yield
    finally:
        subprocess.run([hq, "server", "stop", "--server-dir", str(server)],
                       capture_output=True)
        step.terminate()
        step.wait()
        log.info(f"hq workers on the other nodes: srun exit {step.returncode}")


class HQBackend:
    """Tasks on the HQ server in `server_dir` (default: $HQ_SERVER_DIR, then
    HQ's own default), all through the `hq` CLI.

    - One open HQ job per backend; each executor task is the task of that
      job whose id is the executor's task id as a number (`--array <tid>`),
      so tids must be numbers. HQ refuses a second task with the same id.
    - Completion comes from one `hq journal stream`, which prints an event
      per line, starting with every event since the server started. If it
      ends while the backend is open, the tasks in flight fail.
    - A task reports its error by writing it to $HQ_ERROR_FILENAME (set with
      `--task-dir`) and exiting non-zero; the text arrives in the
      `task-failed` event, cut to about 2 KiB.
    - Logs of finished tasks are deleted by HQ (`:rm-if-finished`); those of
      failed and cancelled tasks are kept.
    - HQ reruns a task whose worker crashed (HQ's default limit).
    - HQ 0.26.2 cancels whole jobs only, so `close()` cancels every task in
      flight at once, with SIGINT to each task's process group.

    `gpu_resource`: the HQ resource name for Resources.gpus. `time_request`:
    HQ's `--time-request` for every task, e.g. "30m": a worker with less
    time left does not start it (off when None). `hq_bin`: the hq CLI."""

    def __init__(self, server_dir: str | Path | None = None, *,
                 gpu_resource: str = "gpus/nvidia", time_request: str | None = None,
                 hq_bin: str | None = None) -> None:
        hq = hq_bin or shutil.which("hq")
        if hq is None:
            raise HQError("no hq on PATH")
        self.hq = [hq, "--output-mode", "quiet"]
        if server_dir is not None:
            self.hq += ["--server-dir", str(server_dir)]
        self.gpu_resource = gpu_resource
        self.time_request = time_request
        self.job = int(self._run("job", "open"))
        self._live: dict[int, str] = {}  # HQ task id -> tid, until reported ended
        self._ended: queue.SimpleQueue[tuple[int, str | None]] = queue.SimpleQueue()
        self._closing = False
        self._stream = subprocess.Popen([*self.hq, "journal", "stream"], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True)
        self._reader = threading.Thread(target=self._read_events, name="gutsrun-hq-events",
                                        daemon=True)
        self._reader.start()

    def _run(self, *args: str) -> str:
        done = subprocess.run([*self.hq, *args], capture_output=True, text=True)
        if done.returncode != 0:
            command = " ".join(a for a in args[:2] if not a.startswith("-"))
            raise HQError(f"hq {command}: {done.stderr.strip()}")
        return done.stdout.strip()

    def submit(self, tid: str, argv: list[str], res: Resources, stdout: str,
               stderr: str) -> None:
        args = ["submit", "--job", str(self.job), "--array", str(int(tid)),
                "--cpus", str(res.cores), "--task-dir",
                "--stdout", stdout.format(instance="%{INSTANCE_ID}") + ":rm-if-finished",
                "--stderr", stderr.format(instance="%{INSTANCE_ID}") + ":rm-if-finished"]
        if res.gpus:
            args += ["--resource", f"{self.gpu_resource}={res.gpus}"]
        if res.cwd:
            args += ["--cwd", res.cwd]
        for name, value in res.env.items():
            args += ["--env", f"{name}={value}"]
        if self.time_request:
            args += ["--time-request", self.time_request]
        self._live[int(tid)] = tid
        try:
            self._run(*args, "--", *argv)
        except BaseException:
            del self._live[int(tid)]
            raise

    def poll(self) -> list[tuple[str, str | None]]:
        ended = []
        while not self._ended.empty():
            task, error = self._ended.get()
            tid = self._live.pop(task, None)
            if tid is not None:  # None: its end was reported already
                ended.append((tid, error))
        return ended

    def close(self, reason: str = "executor shut down") -> None:
        self._closing = True
        with contextlib.suppress(HQError):  # the server may be gone already
            if self._live:
                self._run("job", "cancel", str(self.job), "--reason", reason)
            self._run("job", "close", str(self.job))
        if self._stream.poll() is None:
            self._stream.terminate()
        self._reader.join(timeout=5)

    def _read_events(self) -> None:
        """Turn the job's task events into (task, None | error) on `_ended`.
        If the stream ends while the backend is open, every task in flight
        fails: the run stops, and its next start redoes them."""
        assert self._stream.stdout is not None
        for line in self._stream.stdout:
            for task, error in self._task_ends(json.loads(line)["event"]):
                self._ended.put((task, error))
        self._stream.wait()
        if not self._closing:
            stderr = self._stream.stderr.read().strip() if self._stream.stderr else ""
            for task in list(self._live):
                self._ended.put((task, f"hq journal stream ended: {stderr}"))

    def _task_ends(self, event: dict[str, Any]) -> list[tuple[int, str | None]]:
        """(task, None) for a finished task of this job, (task, error) for a
        failed or cancelled one; [] for any other event."""
        kind = event.get("type")
        if kind in ("task-finished", "task-failed") and event["job"] == self.job:
            return [(event["task"], event.get("error", "failed") if kind == "task-failed"
                     else None)]
        if kind == "task-canceled":
            return [(t["job_task_id"], "cancelled") for t in event["tasks"]
                    if t["job_id"] == self.job]
        return []

