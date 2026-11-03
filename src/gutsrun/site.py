"""A cluster's settings: sites/<name>.toml (sites/template.toml documents
every key), and the environment its `[environment] setup` lines leave.

Standard library only: a link imports it.
"""

import os
import subprocess
import tomllib
from pathlib import Path


class SiteError(Exception):
    """A site file is missing or wrong, or its setup failed."""


class Site:
    """sites/<name>.toml; sites/template.toml documents every key. The
    environment variable GUTSRUN_CORES overrides `cores_per_node`, for tests
    and for trying a link on a machine without Slurm."""

    KEYS = {"slurm": {"partition", "cores_per_node", "max_walltime_day"},
            "environment": {"setup"}}

    def __init__(self, root: str | Path, name: str) -> None:
        path = Path(root) / "sites" / f"{name}.toml"
        if not path.exists():
            known = sorted(p.stem for p in path.parent.glob("*.toml") if p.stem != "template")
            raise SiteError(f"no site {name!r}; known: {', '.join(known)} "
                            "(a new one starts as a copy of sites/template.toml)")
        data = tomllib.loads(path.read_text())
        for table, keys in data.items():
            if table not in self.KEYS:
                raise SiteError(f"{path}: unknown table [{table}]")
            if unknown := set(keys) - self.KEYS[table]:
                raise SiteError(f"{path}: unknown in [{table}]: {', '.join(sorted(unknown))}")
        slurm = data.get("slurm", {})
        self.partition: str = slurm["partition"]
        self.cores: int = int(os.environ.get("GUTSRUN_CORES") or slurm["cores_per_node"])
        self.max_walltime_day: str | None = slurm.get("max_walltime_day")
        self.setup: str | None = data.get("environment", {}).get("setup")


def setup_env(setup: str, env: dict[str, str]) -> dict[str, str]:
    """The environment after running the site's `setup` lines (e.g. `module
    load`) in bash, starting from `env`. Their output goes to the link's log."""
    script = f"{{\n{setup}\n}} >&2 || exit 1\nenv -0"
    done = subprocess.run(["bash", "-c", script], env=env, stdout=subprocess.PIPE)
    if done.returncode != 0:
        raise SiteError(f"the site's setup failed (exit {done.returncode}): {setup!r}")
    pairs = (entry.partition("=") for entry in done.stdout.decode().split("\0") if entry)
    return {name: value for name, _, value in pairs}
