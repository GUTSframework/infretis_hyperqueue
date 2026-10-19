"""Task functions for the tests. They live in a module (not a test file's
`__main__`) so tasks can unpickle them by reference."""

import os
import time
from pathlib import Path
from typing import Any

STATE: Any = None


def square(x: int) -> int:
    return x * x


def boom(msg: str) -> None:
    raise ValueError(msg)


def hard_exit(code: int) -> None:
    os._exit(code)


def env(name: str) -> str | None:
    return os.environ.get(name)


def cwd() -> str:
    return os.getcwd()


def init_state(value: Any) -> None:
    global STATE
    STATE = value


def read_state() -> Any:
    return STATE


def instance() -> str | None:
    return os.environ.get("HQ_INSTANCE_ID") or os.environ.get("GUTSRUN_INSTANCE")


def slow(started: str, seconds: float, value: Any = None) -> Any:
    """Mark the start (with the instance), sleep, return value."""
    Path(started).with_name(Path(started).name + f".{instance()}").touch()
    time.sleep(seconds)
    return value if value is not None else instance()


def touch(path: str) -> str:
    Path(path).touch()
    return path
