"""The environment a test, or a golden writer, runs SST in: none of the shell's SST settings.

A variable from the developer's shell -- a target, a profiles directory, a coding agent's marker
that switches the output to the agent format -- would change what SST prints. The test suite
clears every one of them, so a golden writer clears the same ones before it renders, or it
would write goldens no test produces.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator

from snowflake_semantic_tools.cli.render.terminal import AGENT_MARKERS, FORCE_COLOR_VARIABLES

SST_ENVIRONMENT = (
    "SST_PROJECT_DIR",
    "SST_CONFIG",
    "SST_PROFILES_DIR",
    "SST_TARGET",
    "SST_DEFER_TARGET",
    "SST_STATE_DIR",
    "SST_THREADS",
    "SST_OUTPUT",
    "SST_LOG_LEVEL",
    "SST_STRICT",
    "SST_NO_COLOR",
    "SST_COLOR",
    "NO_COLOR",
    "DBT_PROFILES_DIR",
    # What decides colour, width, CI progress lines, and the agent output format.
    *FORCE_COLOR_VARIABLES,
    *AGENT_MARKERS,
    "CI",
    "COLUMNS",
)


@contextlib.contextmanager
def cleared_environment() -> Iterator[None]:
    """Remove every `SST_ENVIRONMENT` variable for the block, then put back what was set."""
    saved = {name: os.environ.pop(name) for name in SST_ENVIRONMENT if name in os.environ}
    try:
        yield
    finally:
        os.environ.update(saved)
