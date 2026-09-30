"""The declared shape of `sst_config.yml`.

`keys` holds the key table: every key, block, and wildcard slot SST reads, which the
generated configuration reference renders row by row. `validate` checks a parsed
configuration tree against that table. This module re-exports both public surfaces,
so importers never reach into the submodules.
"""

from __future__ import annotations

from .keys import (
    CHILDREN,
    CONFIG_FILE,
    CONFIG_KEYS,
    CONFIG_SCHEMA,
    TOP_LEVEL_KEYS,
    ChildPolicy,
    ConfigKey,
    KeyKind,
    KeyStatus,
)
from .validate import Positions, validate_config

__all__ = [
    "CHILDREN",
    "CONFIG_FILE",
    "CONFIG_KEYS",
    "CONFIG_SCHEMA",
    "TOP_LEVEL_KEYS",
    "ChildPolicy",
    "ConfigKey",
    "KeyKind",
    "KeyStatus",
    "Positions",
    "validate_config",
]
