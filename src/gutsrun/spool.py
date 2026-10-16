"""The spool directory shared by the executor and its tasks.

Layout (one directory per executor instance, the "epoch"):

    <spool>/CURRENT                 name of the live epoch
    <spool>/<epoch>/in/<tid>.pkl    task input (sys.path, then the call)
    <spool>/<epoch>/out/<tid>.<instance>.pkl   task result
    <spool>/<epoch>/log/<tid>.<instance>.{out,err}

Every file is written to a dot-prefixed temporary name and renamed into
place, so a reader never sees half a file. Readers skip dot files.
"""

import os
import pickle
import time
from pathlib import Path

CURRENT = "CURRENT"


def new_epoch() -> str:
    """A name that sorts by start time and does not collide."""
    import secrets  # not needed in task processes

    return time.strftime("%Y%m%dT%H%M%S") + "-" + secrets.token_hex(4)


def write_atomic(path: Path, data: bytes) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def read_current(spool: Path) -> str | None:
    try:
        return (spool / CURRENT).read_text().strip()
    except FileNotFoundError:
        return None


def is_current(epoch_dir: Path) -> bool:
    return read_current(epoch_dir.parent) == epoch_dir.name


def dump(*objects) -> bytes:
    return b"".join(pickle.dumps(o, protocol=pickle.HIGHEST_PROTOCOL) for o in objects)


def result_name(tid: str, instance: str) -> str:
    return f"{tid}.{instance}.pkl"


def parse_result_name(name: str) -> tuple[str, str] | None:
    """`<tid>.<instance>.pkl` -> (tid, instance); None for anything else."""
    if name.startswith(".") or not name.endswith(".pkl"):
        return None
    parts = name[: -len(".pkl")].split(".")
    if len(parts) != 2:
        return None
    return parts[0], parts[1]
