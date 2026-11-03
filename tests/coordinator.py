"""A coordinator that submits slow tasks and then hangs, to be killed.

    python tests/coordinator.py <hq-server-dir> <spool> <started-marker> <seconds> <n>
"""

import sys
import time

import helpers

from gutsrun import TaskExecutor
from gutsrun.hq import HQBackend

server, spool_dir, started = sys.argv[1], sys.argv[2], sys.argv[3]
seconds, n = float(sys.argv[4]), int(sys.argv[5])
ex = TaskExecutor(HQBackend(server), spool_dir)
for i in range(n):
    ex.submit(helpers.slow, f"{started}{i}", seconds, f"old-{i}")
print(ex.dir.name, flush=True)
time.sleep(3600)
