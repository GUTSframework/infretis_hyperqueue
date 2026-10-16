"""Task functions for the tests. They live in a module (not a test file's
`__main__`) so tasks can unpickle them by reference."""

import os
import time
from pathlib import Path

STATE = None


def square(x):
    return x * x


def boom(msg):
    raise ValueError(msg)


def hard_exit(code):
    os._exit(code)


def env(name):
    return os.environ.get(name)


def cwd():
    return os.getcwd()


def init_state(value):
    global STATE
    STATE = value


def read_state():
    return STATE


def instance():
    return os.environ.get("HQ_INSTANCE_ID") or os.environ.get("GUTSRUN_INSTANCE")


def slow(started: str, seconds: float, value=None):
    """Mark the start (with the instance), sleep, return value."""
    Path(started).with_name(Path(started).name + f".{instance()}").touch()
    time.sleep(seconds)
    return value if value is not None else instance()


def touch(path: str):
    Path(path).touch()
    return path
