"""A coordinator that submits slow tasks and then hangs, to be killed.

    python tests/coordinator.py <spool> <started-marker> <seconds> <n>
"""

import sys
import time

import helpers
from gutsrun import TaskExecutor
from gutsrun.backends.local import LocalBackend

spool_dir, started, seconds, n = sys.argv[1], sys.argv[2], float(sys.argv[3]), int(sys.argv[4])
ex = TaskExecutor(LocalBackend(max_workers=n), spool_dir)
for i in range(n):
    ex.submit(helpers.slow, f"{started}{i}", seconds, f"old-{i}")
print(ex.epoch, flush=True)
time.sleep(3600)
