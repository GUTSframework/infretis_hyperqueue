"""HyperQueue backend: one HQ job with one program task per executor task.

One job per task, so a finished task can be forgotten on its own
(`Client.forget` works per job) and the server's memory stays flat.

- Never waits with `Client.wait_for_jobs`: in 0.26.2 it polls the server
  once per second. The executor learns of finished tasks from result files
  and calls `poll()` at a low rate for failures only.
- `poll()` asks about all tasks in flight in one FFI call.
- The 0.26.2 Python client cannot cancel a job, so `cancel()` runs the
  `hq` CLI.
- HQ reruns a task whose worker crashed, up to `crash_limit` times; the
  rerun gets a new HQ_INSTANCE_ID and so a new result file name.
"""

import shutil
import subprocess

from hyperqueue import Client, Job
from hyperqueue.ffi.protocol import ResourceRequest

from gutsrun.backends import Resources


class HQBackend:
    def __init__(self, server_dir: str | None = None, *, crash_limit: int = 2,
                 gpu_resource: str = "gpus/nvidia", hq_bin: str | None = None):
        # server_dir must be a str: the FFI rejects pathlib.Path.
        self.server_dir = None if server_dir is None else str(server_dir)
        self.client = Client(self.server_dir)
        self.crash_limit = crash_limit
        self.gpu_resource = gpu_resource
        self.hq_bin = hq_bin or shutil.which("hq")
        self._jobs: dict[str, int] = {}  # tid -> HQ job id, until forgotten
        self._to_forget: set[str] = set()

    def submit(self, tid, argv, res: Resources, stdout, stderr):
        job = Job()
        request = ResourceRequest(
            cpus=res.cores,
            resources={self.gpu_resource: res.gpus} if res.gpus else None)
        job.program(
            argv, env=res.env or None, cwd=res.cwd,
            stdout=stdout.format(instance="%{INSTANCE_ID}"),
            stderr=stderr.format(instance="%{INSTANCE_ID}"),
            resources=request, crash_limit=self.crash_limit)
        self._jobs[tid] = self.client.submit(job).id

    def poll(self):
        self._forget_pending()
        live = {jid: tid for tid, jid in self._jobs.items() if tid not in self._to_forget}
        if not live:
            return []
        ended = []
        failed = self.client.connection.get_failed_tasks(list(live))
        for jid, tasks in failed.items():
            for ctx in tasks.values():
                tid = live[jid]
                ended.append((tid, ctx.error))
                self._to_forget.add(tid)
        return ended

    def release(self, tid):
        if tid in self._jobs:
            self._to_forget.add(tid)
            self._forget_pending()

    def _forget_pending(self):
        for tid in list(self._to_forget):
            try:
                self.client.forget(self._jobs[tid])
            except Exception:  # still running (exiting after its result); retry later
                continue
            del self._jobs[tid]
            self._to_forget.discard(tid)

    def cancel(self, tid):
        jid = self._jobs.get(tid)
        if jid is None or self.hq_bin is None:
            return
        cmd = [self.hq_bin, "job", "cancel", str(jid)]
        if self.server_dir:
            cmd += ["--server-dir", self.server_dir]
        subprocess.run(cmd, capture_output=True, check=False)
        self._to_forget.add(tid)

    def close(self):
        for tid in list(self._jobs):
            if tid not in self._to_forget:
                self.cancel(tid)
        self._forget_pending()
