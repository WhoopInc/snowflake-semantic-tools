"""The declared shape of `sst_config.yml`, and how the engine reads values out of it.

`keys` holds the key table: every key, block, and wildcard slot SST reads, which the
generated configuration reference renders row by row, and `values` reads the values the
engine uses out of a parsed tree; `domain.validate.config` checks a tree against the table.
This module re-exports their public surfaces, so importers never reach into the submodules.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.config_schema.keys import (
    CHILDREN,
    CONFIG_FILE,
    CONFIG_KEYS,
    CONFIG_NAMES,
    CONFIG_SCHEMA,
    TOP_LEVEL_KEYS,
    ChildPolicy,
    ConfigKey,
    KeyKind,
    KeyStatus,
)
from snowflake_semantic_tools.domain.model.config_schema.values import (
    DbtSettings,
    EnrichmentConfig,
    config_block,
    config_bool,
    config_int,
    config_text,
    configured_dir,
    dbt_settings,
    enrichment_config,
    skills_configured,
    target_text,
)

__all__ = [
    "CHILDREN",
    "CONFIG_FILE",
    "CONFIG_KEYS",
    "CONFIG_NAMES",
    "CONFIG_SCHEMA",
    "TOP_LEVEL_KEYS",
    "ChildPolicy",
    "ConfigKey",
    "DbtSettings",
    "EnrichmentConfig",
    "KeyKind",
    "KeyStatus",
    "config_block",
    "config_bool",
    "config_int",
    "config_text",
    "configured_dir",
    "dbt_settings",
    "enrichment_config",
    "skills_configured",
    "target_text",
]
