"""Task-side code: what each move's task process runs.

Importing this module first also imports `infretis.setup` first, which
works around infretis's import-order bug (importing
`infretis.classes.engines.factory` or `infretis.core.tis` first fails).
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

import infretis.setup  # noqa: F401  (must come first, see above)
import infretis.core.tis
from infretis.classes.engines.factory import create_engines
from infretis.classes.formatter import get_log_formatter
from infretis.classes.orderparameter import create_orderparameters

logger = logging.getLogger("")


def init(config):
    """Replaces infretis's `worker_initializer(counter, config)`.

    Same engine and order parameter set-up, without the shared counter
    (a multiprocessing Value cannot travel to an HQ task). Logging moves to
    `run_move`, which knows the pin.
    """
    if "simulation" in config:
        engines, _ = create_engines(config)
        create_orderparameters(engines, config)
        infretis.core.tis.ENGINES = engines


def run_move(call):
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
