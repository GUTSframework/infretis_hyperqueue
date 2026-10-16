"""gutsrun: run Python calls as cluster tasks behind a standard Executor.

Generic: knows nothing about MD or infretis (that is the adapter's job).

The exports load lazily: every task process imports this package (for
`gutsrun.task`), and should not pay for the executor's imports.
"""

_EXPORTS = {"TaskExecutor", "TaskFailed", "RemoteTraceback"}


def __getattr__(name):
    if name in _EXPORTS:
        from gutsrun import executor
        return getattr(executor, name)
    raise AttributeError(f"module 'gutsrun' has no attribute {name!r}")
