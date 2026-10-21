"""Task-side code: what each move's task process runs.

Importing this module first also imports `infretis.setup` first, which
works around infretis's import-order bug (importing
`infretis.classes.engines.factory` first fails: it uses `importlib.util`
without importing it). `# isort: split` keeps ruff from sorting it later.
The executor pickles `run_move` before the infretis call, so unpickling
imports this module before anything from infretis.

Parts of this file are adapted from infretis (https://github.com/infretis/infretis,
commit a42e039; see PROVENANCE.md), which carries this licence:

    MIT License

    Copyright (c) 2022 infretis

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
"""

import logging
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Any

import infretis.setup  # noqa: F401  (must come first, see above)

# isort: split
import infretis.core.tis
from infretis.classes.engines.factory import create_engines
from infretis.classes.engines.gromacs import GromacsEngine
from infretis.classes.formatter import get_log_formatter
from infretis.classes.orderparameter import create_orderparameters

logger = logging.getLogger("")


def init(config: dict[str, Any]) -> None:
    """Replaces infretis's `worker_initializer(counter, config)`.

    Same engine and order parameter set-up, without the shared counter
    (a multiprocessing Value cannot travel to an HQ task). Logging moves to
    `run_move`, which knows the pin.
    """
    if "simulation" in config:
        with shared_gromacs_inputs_read_only():
            engines, _ = create_engines(config)
        # infretis annotates `engines` as dict[str, list]; it takes what
        # create_engines returns.
        create_orderparameters(engines, config)  # pyright: ignore[reportArgumentType]
        infretis.core.tis.ENGINES = engines


class SharedInputChanged(RuntimeError):
    """A task's engine settings no longer produce the shared GROMACS mdp
    that infretis wrote at start, which other moves are reading."""


@contextmanager
def shared_gromacs_inputs_read_only() -> Iterator[None]:
    """While a task builds its engines, keep `GromacsEngine.__init__` from
    writing to the shared input directory, which other tasks' moves read at
    the same time.

    `__init__` writes `<input_path>/infretis.mdp` in place (truncate, then
    line by line) and runs grompp there for `topol.tpr`. Every move copies
    that mdp into its own, so a move that reads it mid-write gets an mdp
    without thermostat or constraints: grompp may refuse it, or the move
    runs other physics. infretis's setup has written both in the parent
    before any task starts, and nothing reads `topol.tpr` later. So here
    grompp is skipped, and the mdp is built in local temp space and compared
    with the shared one instead of written. If they differ, the shared mdp
    changed since the start, or two engines with different settings share
    one `input_path` (in infretis, the last one built wins and the others'
    moves run with its settings): `SharedInputChanged` stops the run.
    """
    modify_input = GromacsEngine._modify_input  # EngineBase's staticmethod
    own = {name: GromacsEngine.__dict__.get(name)
           for name in ("_modify_input", "_execute_grompp")}

    def check_unchanged(sourcefile: str | Path, outputfile: str | Path,
                        settings: dict[str, Any], delim: str = "=") -> None:
        shared = Path(outputfile)
        with tempfile.TemporaryDirectory() as tmp:
            mine = Path(tmp) / shared.name
            modify_input(sourcefile, mine, settings, delim)
            if shared.exists() and mine.read_bytes() == shared.read_bytes():
                return
        what = (f"differs from what this task's engine settings make of {sourcefile}"
                if shared.exists() else "is missing")
        raise SharedInputChanged(
            f"{shared} {what}. infretis writes it at start and every move reads "
            "it, so a task does not rewrite it. Either it changed during the "
            "run, or two engines with different settings share its directory "
            "(give each engine its own input_path).")

    def skip_grompp(self: GromacsEngine, mdp_file: str | Path, config: str | Path,
                    deffnm: str) -> dict[str, str]:
        return {"tpr": f"{deffnm}.tpr", "mdout": "mdout.mdp"}

    GromacsEngine._modify_input = staticmethod(check_unchanged)
    GromacsEngine._execute_grompp = skip_grompp
    try:
        yield
    finally:
        for name, attr in own.items():
            if attr is None:
                delattr(GromacsEngine, name)
            else:
                setattr(GromacsEngine, name, attr)


def run_move(call: "partial[Any]") -> Any:
    """Run `call` (infretis's `partial(run_md, md_item)`), logging to
    `worker{pin}.log`, the same per-slot name as the scratch dir `worker{pin}/`."""
    pin = call.args[0]["pin"]
    level = logging.INFO
    handler = logging.FileHandler(f"worker{pin}.log", mode="a")
    handler.setLevel(level)
    handler.setFormatter(get_log_formatter(level))
    logger.setLevel(logging.DEBUG)  # as infretis.asyncrunner does at import
    logger.addHandler(handler)
    try:
        return call()
    finally:
        logger.removeHandler(handler)
        handler.close()
