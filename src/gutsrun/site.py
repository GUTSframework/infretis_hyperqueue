"""A cluster's settings: sites/<name>.toml (sites/template.toml documents
every key), and the environment its `[environment] setup` lines leave.
"""

import os
import subprocess
import tempfile
import tomllib
from pathlib import Path
from typing import Any


class SiteError(Exception):
    """A site file is missing or wrong, or its setup failed."""


# Every table a site file may have, its keys, and the type of each value.
SITE_KEYS: dict[str, dict[str, type]] = {
    "slurm": {"partition": str, "cores_per_node": int, "max_walltime": str},
    "environment": {"setup": str},
}
# The keys every site file sets; the others are optional.
REQUIRED_SITE_KEYS = {"slurm": {"partition", "cores_per_node"}}
# How an error message names a value's type: TOML's names.
TOML_TYPE_NAMES = {str: "a string", int: "an integer"}
# An environment variable that, when set, overrides `cores_per_node`: for
# running a link on a machine without Slurm (DEVELOPMENT.md).
CORES_OVERRIDE_VARIABLE = "GUTSRUN_CORES"


def checked_site_file(path: Path) -> dict[str, dict[str, Any]]:
    """The tables of the site file at `path`. Raises SiteError if it is not
    valid TOML, if a table, a key or a value's type is not in SITE_KEYS, if
    a required key is missing, or if `cores_per_node` is below 1."""
    try:
        data = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as e:
        raise SiteError(f"{path}: not valid TOML: {e}") from None
    for table, values in data.items():
        if table not in SITE_KEYS:
            raise SiteError(f"{path}: unknown table [{table}]")
        if not isinstance(values, dict):
            raise SiteError(f"{path}: {table} is not a table; write [{table}] above its keys")
        if unknown := set(values) - set(SITE_KEYS[table]):
            raise SiteError(f"{path}: unknown in [{table}]: {', '.join(sorted(unknown))}")
        for key, value in values.items():
            expected = SITE_KEYS[table][key]
            # `type() is`, not isinstance(): TOML's true is a bool, and a bool is an int.
            if type(value) is not expected:
                raise SiteError(f"{path}: [{table}] {key} = {value!r} is not "
                                f"{TOML_TYPE_NAMES[expected]}")
    for table, keys in REQUIRED_SITE_KEYS.items():
        if missing := keys - set(data.get(table, {})):
            raise SiteError(f"{path}: missing in [{table}]: {', '.join(sorted(missing))}")
    if data["slurm"]["cores_per_node"] < 1:
        raise SiteError(f"{path}: [slurm] cores_per_node must be at least 1")
    return data


class Site:
    """The settings of the site `name`, read from <root>/sites/<name>.toml.
    `cores` is `cores_per_node`, or the value of CORES_OVERRIDE_VARIABLE if
    that is set."""

    def __init__(self, root: str | Path, name: str) -> None:
        path = Path(root) / "sites" / f"{name}.toml"
        if not path.exists():
            known = sorted(p.stem for p in path.parent.glob("*.toml") if p.stem != "template")
            raise SiteError(f"no site {name!r}; known: {', '.join(known)} "
                            "(a new one starts as a copy of sites/template.toml)")
        data = checked_site_file(path)
        slurm = data["slurm"]
        self.partition: str = slurm["partition"]
        self.cores: int = slurm["cores_per_node"]
        if override := os.environ.get(CORES_OVERRIDE_VARIABLE):
            if not override.isdigit() or int(override) < 1:
                raise SiteError(f"{CORES_OVERRIDE_VARIABLE}={override!r} is not a whole "
                                "number of cores, at least 1")
            self.cores = int(override)
        self.max_walltime: str | None = slurm.get("max_walltime")
        self.setup: str | None = data.get("environment", {}).get("setup")


def env_after_site_setup(setup: str, env: dict[str, str]) -> dict[str, str]:
    """The environment after running the site's `setup` lines (e.g. `module
    load`) in bash, starting from `env`. Their output goes to this process's
    stdout and stderr, which in a link are the link's log. The first command
    that fails ends the setup, as with bash's `set -e`; the log names that
    command. Bash writes the resulting environment to a temporary file,
    given to the script as $1."""
    script = "\n".join([
        "set -e",
        "trap 'echo \"site setup failed at: $BASH_COMMAND\" >&2' ERR",
        setup,
        'env -0 > "$1"',
    ])
    with tempfile.NamedTemporaryFile(prefix="gutsrun-site-env-") as env_file:
        # The script's $0 is the name bash puts in front of its own error messages.
        done = subprocess.run(["bash", "-c", script, "site-setup", env_file.name], env=env)
        if done.returncode != 0:
            raise SiteError(f"the site's setup failed (exit {done.returncode}): {setup!r}")
        entries = Path(env_file.name).read_bytes().decode().split("\0")
    pairs = (entry.partition("=") for entry in entries if entry)
    return {name: value for name, _, value in pairs}
