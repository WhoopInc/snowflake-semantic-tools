"""The declared shape of `sst_config.yml`, and how the engine reads values out of it.

`keys` holds the key table: every key, block, and wildcard slot SST reads, which the
generated configuration reference renders row by row; `values` reads the values the engine
uses out of a parsed tree, and `settings` resolves a semantic view's or an agent's settings
through the one precedence chain; `domain.validate.config` checks a tree against the table.
This module re-exports their public surfaces, so importers never reach into the submodules.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.config_schema.keys import (
    CHILDREN,
    CONFIG_FILE,
    CONFIG_KEYS,
    CONFIG_NAMES,
    CONFIG_SCHEMA,
    DEFAULT_THREADS,
    TOP_LEVEL_KEYS,
    ChildPolicy,
    ConfigKey,
    KeyKind,
    KeyStatus,
)
from snowflake_semantic_tools.domain.model.config_schema.settings import (
    Setting,
    block_settings,
    resolve_settings,
    setting_value,
    with_defaults,
)
from snowflake_semantic_tools.domain.model.config_schema.values import (
    DbtSettings,
    EnrichmentConfig,
    config_block,
    config_bool,
    config_int,
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
    "DEFAULT_THREADS",
    "TOP_LEVEL_KEYS",
    "ChildPolicy",
    "ConfigKey",
    "DbtSettings",
    "EnrichmentConfig",
    "KeyKind",
    "KeyStatus",
    "Setting",
    "block_settings",
    "config_block",
    "config_bool",
    "config_int",
    "configured_dir",
    "dbt_settings",
    "enrichment_config",
    "resolve_settings",
    "setting_value",
    "skills_configured",
    "target_text",
    "with_defaults",
]
