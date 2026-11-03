"""Backends start task processes; the executor does everything else.

A backend gets a task as an argv plus where to run it, and later reports
which tasks ended, with the error of the failed ones. It never sees the
call or the result: those travel through the spool directory.

`gutsrun.hq.HQBackend` runs tasks on HyperQueue, `local.LocalBackend` as
local processes.
"""

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class Resources:
    """What one task needs. Modelled on executorlib's `resource_dict`."""

    cores: int = 1
    gpus: int = 0
    cwd: str | None = None
    env: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None, default: "Resources") -> "Resources":
        """A copy of `default`, with the keys in `d` replaced (`env` is
        merged). Never `default` itself: callers change what they get."""
        d = d or {}
        unknown = set(d) - {"cores", "gpus", "cwd", "env"}
        if unknown:
            raise ValueError(f"unknown resource keys: {sorted(unknown)}")
        return cls(
            cores=d.get("cores", default.cores),
            gpus=d.get("gpus", default.gpus),
            cwd=d.get("cwd", default.cwd),
            env={**default.env, **d.get("env", {})},
        )


class Backend(Protocol):
    """Starts task processes and reports the ones that ended."""

    def submit(self, tid: str, argv: list[str], res: Resources, stdout: str, stderr: str) -> None:
        """Start (or queue) a task. `stdout`/`stderr` contain `{instance}`.
        The task process finds a file to write its error to in
        $HQ_ERROR_FILENAME. Logs of tasks that finish are deleted."""

    def poll(self) -> list[tuple[str, str | None]]:
        """Tasks that ended since the last call: (tid, None) for a clean
        exit, (tid, message) for a failure, with the error the task wrote
        if it wrote one."""
        ...

    def close(self) -> None:
        """Cancel what is left and free the backend's resources."""
