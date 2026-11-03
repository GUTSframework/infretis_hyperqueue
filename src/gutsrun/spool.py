"""The spool directory shared by the executor and its tasks.

Layout (one directory per executor instance; a new executor deletes the
others, so tasks of an older one find neither their input nor out/):

    <spool>/<name>/in/<tid>.pkl    task input (sys.path, then the call)
    <spool>/<name>/out/<tid>.pkl   task result (only of a call that returned)
    <spool>/<name>/log/<tid>.<instance>.{out,err}   kept only for failed tasks

The instance goes up when HQ reruns a task after its worker crashed.

Every file is written to a dot-prefixed temporary name and renamed into
place, so a reader never sees half a file.
"""

import os
import pickle
import time
from pathlib import Path


def new_name() -> str:
    """A directory name that sorts by start time and does not collide."""
    import secrets  # not needed in task processes

    return time.strftime("%Y%m%dT%H%M%S") + "-" + secrets.token_hex(4)


def write_atomic(path: Path, data: bytes) -> None:
    """Write `data` to `path` via a dot-prefixed temporary file and a rename."""
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def dump(*objects: object) -> bytes:
    """The objects pickled one after the other, to read with pickle.load in turn."""
    return b"".join(pickle.dumps(o, protocol=pickle.HIGHEST_PROTOCOL) for o in objects)

