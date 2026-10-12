"""Install the test network guard in a subprocess an offline test starts.

Python imports the first `sitecustomize` on `sys.path` at startup, and
`network_guard.guarded_environment` puts this directory at the front of the child's `PYTHONPATH`:
only the script's own directory (`sys.path[0]`) is searched before it. This module therefore
shadows any `sitecustomize` the interpreter would otherwise run, so it runs that one too, after
the guard is installed.

The guard is installed only when the environment asks for it, so a child that inherits the path
without the variable runs unguarded. A child that is asked for the guard and cannot install it --
it cannot import the guard, or installing it fails -- writes why to stderr and exits
`network_guard.UNGUARDED_EXIT` at once, rather than run on unguarded.
"""

from __future__ import annotations

import importlib.machinery
import importlib.util
import os
import sys
from pathlib import Path

# `network_guard.UNGUARDED_EXIT`, written out because the child may be unable to import it.
_UNGUARDED_EXIT = 87
_HERE = Path(__file__).resolve().parent


def _install_guard() -> None:
    try:
        from tests.helpers import network_guard

        network_guard.install_in_subprocess()
    except BaseException as exc:  # any failure must stop the child, never leave it unguarded
        sys.stderr.write(f"the test network guard could not be installed: {type(exc).__name__}: {exc}\n")
        sys.stderr.flush()
        os._exit(_UNGUARDED_EXIT)


def _is_here(entry: str) -> bool:
    try:
        return Path(entry or os.getcwd()).resolve() == _HERE
    except OSError:
        return False


def _run_shadowed() -> None:
    """Run the `sitecustomize` later on `sys.path` that this module shadows, if there is one."""
    spec = importlib.machinery.PathFinder.find_spec("sitecustomize", [p for p in sys.path if not _is_here(p)])
    if spec is None or spec.loader is None:
        return
    module = importlib.util.module_from_spec(spec)
    sys.modules["sitecustomize"] = module
    spec.loader.exec_module(module)


if os.environ.get("SST_TEST_NETWORK_GUARD") == "1":
    _install_guard()
_run_shadowed()
