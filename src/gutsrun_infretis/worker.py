"""Task-side code: what each move's task process runs.

Importing this module first also imports `infretis.setup` first, which
works around infretis's import-order bug (importing
`infretis.classes.engines.factory` or `infretis.core.tis` first fails).
The executor pickles `run_move` before the infretis call, so unpickling
imports this module before anything from infretis.
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
