"""Everything HyperQueue, through its `hq` CLI: a server and worker on the
link's node, workers on the job's other nodes, and `HQBackend`, which runs
tasks on a server.
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
from collections.abc import Generator
from pathlib import Path
from typing import Any, Literal, NotRequired, TypedDict, cast

from gutsrun.executor import Resources

VERSION = "0.26.2"  # keep in sync with flake.nix and scripts/get-hq.sh

log = logging.getLogger("gutsrun.hq")

SERVER_START_TIMEOUT = 60  # s for the HQ server to write hq-current
SERVER_START_POLL = 0.1  # s between looks for hq-current
PROCESS_STOP_TIMEOUT = 10  # s for a stopped server or worker to exit before it is killed
EVENT_READER_STOP_TIMEOUT = 5  # s for the event-stream thread to end at close()


class HQError(Exception):
    """No suitable hq binary, the server did not start, or hq refused a call."""


# --- HQ's JSON, as HQ 0.26.2 prints it: only the keys read here ----------------------


class WorkerResource(TypedDict):
    """One resource of a worker in `hq worker list`."""

    name: str  # e.g. "cpus", "gpus/nvidia", "mem"
    kind: str  # "range", "list" or "sum"
    values: NotRequired[list[str]]  # a "list" resource's items, e.g. GPU indices


class WorkerResources(TypedDict):
    resources: list[WorkerResource]


class WorkerConfiguration(TypedDict):
    resources: WorkerResources


class Worker(TypedDict):
    """One entry of `hq --output-mode json worker list`."""

    id: int
    configuration: WorkerConfiguration


class TaskFinishedEvent(TypedDict):
    type: Literal["task-finished"]
    job: int
    task: int


class TaskFailedEvent(TypedDict):
    type: Literal["task-failed"]
    job: int
    task: int
    error: NotRequired[str]  # what the task wrote to $HQ_ERROR_FILENAME, or HQ's reason


class CanceledTask(TypedDict):
    job_id: int
    job_task_id: int


class TaskCanceledEvent(TypedDict):
    """One event for all tasks a `hq job cancel` cancelled."""

    type: Literal["task-canceled"]
    tasks: list[CanceledTask]


# The events that end a task. `hq journal stream` prints one JSON object
# per line, {"time": ..., "event": {...}}; most events are of other types,
# and one (after a job cancel) has no type at all.
TaskEndEvent = TaskFinishedEvent | TaskFailedEvent | TaskCanceledEvent
TASK_END_TYPES = ("task-finished", "task-failed", "task-canceled")


def find_hq_binary(root: str | Path) -> str:
    """The hq binary: bin/hq in the checkout, else the one on PATH, checked
    to be VERSION. The other functions here take the binary it returns."""
    bundled = Path(root) / "bin" / "hq"
    hq = str(bundled) if bundled.exists() else shutil.which("hq")
    if hq is None:
        raise HQError("no hq in bin/ or on PATH; run scripts/get-hq.sh")
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


def worker_gpus(hq: str, server_dir: str | Path, gpu_resource: str = "gpus/nvidia") -> list[int]:
    """How many GPUs (`gpu_resource`) each connected worker of the server
    has. HQ detects a node's NVIDIA GPUs when the worker starts."""
    done = subprocess.run([hq, "--server-dir", str(server_dir), "--output-mode", "json",
                           "worker", "list"], capture_output=True, text=True)
    if done.returncode != 0:
        raise HQError(f"hq worker list: {done.stderr.strip()}")
    workers: list[Worker] = json.loads(done.stdout)
    return [gpu_count(worker, gpu_resource) for worker in workers]


def gpu_count(worker: Worker, gpu_resource: str) -> int:
    """How many GPUs (`gpu_resource`) one worker of `hq worker list` has.
    HQ lists them as a resource whose `values` are the GPUs' indices."""
    count = 0
    for resource in worker["configuration"]["resources"]["resources"]:
        if resource["name"] == gpu_resource:
            count += len(resource.get("values", []))
    return count


@contextlib.contextmanager
def running_server_and_worker(hq: str, server: Path, log_prefix: str, cores: int,
                              env: dict[str, str] | None = None) -> Generator[None]:
    """Run an HQ server and one worker on this node for the `with` block.
    The worker, and so its tasks, runs in `env`. Stops both and removes the
    server directory when the block ends, also on errors.

    The access file (`<server>/access.json`: host, ports, keys) is written
    before the server starts, so workers on other nodes can start at any
    time: they find it, and retry every 2 s until the server listens.

    Both run in a session of their own: a Ctrl-C in the terminal goes to
    the caller, which stops its tasks before the `with` block stops HQ."""
    shutil.rmtree(server, ignore_errors=True)
    server.mkdir(parents=True)
    access = server / "access.json"
    done = subprocess.run([hq, "server", "generate-access", str(access),
                           "--host", socket.gethostname(),
                           "--client-port", str(free_port()), "--worker-port", str(free_port())],
                          capture_output=True, text=True)
    if done.returncode != 0:
        raise HQError(f"hq server generate-access: {done.stderr.strip()}")
    processes: list[subprocess.Popen[bytes]] = []
    try:
        with open(f"{log_prefix}-hq-server.log", "wb") as out:
            processes.append(subprocess.Popen(
                [hq, "server", "start", "--server-dir", str(server), "--access-file", str(access)],
                stdout=out, stderr=subprocess.STDOUT, start_new_session=True))
        # The server writes hq-current on this node, so no NFS cache hides it.
        deadline = time.monotonic() + SERVER_START_TIMEOUT
        while not (server / "hq-current").exists():
            if processes[0].poll() is not None or time.monotonic() > deadline:
                raise HQError(f"hq server did not start; see {log_prefix}-hq-server.log")
            time.sleep(SERVER_START_POLL)
        with open(f"{log_prefix}-hq-worker.log", "wb") as out:
            processes.append(subprocess.Popen(
                [hq, "worker", "start", "--server-dir", str(server), "--cpus", str(cores)],
                stdout=out, stderr=subprocess.STDOUT, env=env, start_new_session=True))
        log.info(f"hq server and worker started in {server}")
        yield
    finally:
        subprocess.run([hq, "server", "stop", "--server-dir", str(server)],
                       capture_output=True)
        for process in processes:
            try:
                process.wait(timeout=PROCESS_STOP_TIMEOUT)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        shutil.rmtree(server, ignore_errors=True)


@contextlib.contextmanager
def running_workers_on_other_nodes(hq: str, server: Path, log_prefix: str, others: int,
                                   cores: int, env: dict[str, str] | None = None
                                   ) -> Generator[None]:
    """An HQ worker on each of the job's `others` other nodes for the `with`
    block, in one srun step, in `env` (srun passes the environment on to the
    step); each logs to `<log_prefix>-hq-worker-<node>.log`.

    A worker that dies during the block ends neither the block nor the
    other workers (`--kill-on-bad-exit=0`): HQ reruns its tasks elsewhere.

    When the block ends, this stops the HQ server, which tells every worker
    to stop, and then terminates the step: srun answers SIGTERM by killing
    whatever of it is left. The caller enters this block inside
    `running_server_and_worker`'s, so this one ends first, while the server
    still runs; that server's own stop then finds nothing left to do."""
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
    HQ's own default), all through the `hq` CLI, the binary `hq`.

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
    - HQ reruns a task whose worker was lost while it ran. After 5 lost
      workers (HQ's default `--crash-limit`), the task fails instead.
    - HQ 0.26.2 cancels whole jobs only, so `close()` cancels every task in
      flight at once, with SIGINT to each task's process group.

    `gpu_resource`: the HQ resource name for Resources.gpus. `time_request`:
    HQ's `--time-request` for every task, e.g. "30m": a worker with less
    time left does not start it (off when None)."""

    def __init__(self, hq: str, server_dir: str | Path | None = None, *,
                 gpu_resource: str = "gpus/nvidia", time_request: str | None = None) -> None:
        # The start of every hq command line.
        self.hq_argv = [hq, "--output-mode", "quiet"]
        if server_dir is not None:
            self.hq_argv += ["--server-dir", str(server_dir)]
        self.gpu_resource = gpu_resource
        self.time_request = time_request
        self.job = int(self._run("job", "open"))
        self._live: dict[int, str] = {}  # HQ task id -> tid, until reported ended
        self._ended: queue.SimpleQueue[tuple[int, str | None]] = queue.SimpleQueue()
        self._closing = False
        self._stream = subprocess.Popen([*self.hq_argv, "journal", "stream"],
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self._reader = threading.Thread(target=self._read_events, name="gutsrun-hq-events",
                                        daemon=True)
        self._reader.start()

    def _run(self, *args: str) -> str:
        done = subprocess.run([*self.hq_argv, *args], capture_output=True, text=True)
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
        """The tasks that ended since the last call, as (tid, None) or
        (tid, error); see Backend.poll. The reader thread puts every end it
        sees on `_ended`; this takes them off and turns HQ's task id back
        into the executor's tid. A task's end is reported once: a second
        one (e.g. a lost event stream after a task already failed) finds the
        task gone from `_live` and is dropped."""
        ended: list[tuple[str, str | None]] = []
        while not self._ended.empty():
            task, error = self._ended.get()
            tid = self._live.pop(task, None)
            if tid is not None:
                ended.append((tid, error))
        return ended

    def close(self, reason: str = "executor shut down") -> None:
        """Cancel the tasks in flight, close the job, stop reading events.
        A failed hq call is logged, not raised: at shutdown the server may
        be gone already, and the rest of the shutdown still has to run."""
        self._closing = True
        if self._live:
            try:
                self._run("job", "cancel", str(self.job), "--reason", reason)
            except HQError as e:
                log.warning(f"could not cancel HQ job {self.job}: {e}")
        try:
            self._run("job", "close", str(self.job))
        except HQError as e:
            log.warning(f"could not close HQ job {self.job}: {e}")
        if self._stream.poll() is None:
            self._stream.terminate()
        self._reader.join(timeout=EVENT_READER_STOP_TIMEOUT)

    def _read_events(self) -> None:
        """Turn the job's task events into (task, None | error) on `_ended`.
        If the stream ends while the backend is open, or has a line this
        cannot read, every task in flight fails: an end it missed would
        leave its task waiting forever. The run stops, and its next start
        redoes them."""
        assert self._stream.stdout is not None
        line = ""
        try:
            for line in self._stream.stdout:
                event: dict[str, Any] = json.loads(line)["event"]
                if event.get("type") in TASK_END_TYPES:
                    for task, error in self._task_ends(cast(TaskEndEvent, event)):
                        self._ended.put((task, error))
        except Exception as e:  # anything: this thread must not end silently
            log.exception(f"cannot read this hq journal stream line: {line!r}")
            failure = f"hq journal stream unreadable ({type(e).__name__}: {e})"
            self._stream.terminate()
            self._stream.wait()
        else:
            self._stream.wait()
            stderr = self._stream.stderr.read().strip() if self._stream.stderr else ""
            failure = f"hq journal stream ended: {stderr}"
        if not self._closing:
            for task in list(self._live):
                self._ended.put((task, failure))

    def _task_ends(self, event: TaskEndEvent) -> list[tuple[int, str | None]]:
        """The tasks of this job that `event` ends: (task, None) for a
        finished one, (task, error) for a failed or cancelled one."""
        if event["type"] == "task-canceled":
            return [(task["job_task_id"], "cancelled") for task in event["tasks"]
                    if task["job_id"] == self.job]
        if event["job"] != self.job:
            return []
        if event["type"] == "task-failed":
            return [(event["task"], event.get("error", "failed"))]
        return [(event["task"], None)]

