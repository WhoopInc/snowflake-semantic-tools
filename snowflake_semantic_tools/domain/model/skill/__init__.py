"""Skills and plugins: authored folders, path references, and flattened bundles.

A skill is authored nested, the way CoCo Desktop reads it, and flattened only for
the catalog channel, because a Cortex Agent reads supporting files beside
`SKILL.md` and never descends into a subdirectory. Flattening is three operations
in a fixed order: detect collisions in authored terms, join path components with a
double underscore, then rewrite every Markdown reference to the flattened name and
check the rewritten text again.

`model` holds the records and the identities derived from them, `references` finds the
paths a file names, `flatten` publishes a folder flat, `bundle` builds the file set an
extension version is made from, and `validate` checks the catalog. This module only
re-exports their public names.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.skill.bundle import (
    BUNDLE_BUDGET_BYTES,
    PLUGIN_MANIFEST,
    SCAN_MAX_FILE_BYTES,
    SCAN_MAX_FILES,
    SCAN_MAX_TOTAL_BYTES,
    SKILL_MD_BUDGET_BYTES,
    build_plugin_bundle,
    build_skill_bundle,
    plugin_manifest_json,
)
from snowflake_semantic_tools.domain.model.skill.flatten import CONVENTIONAL_DIRS, flatten_skill, flattened_name
from snowflake_semantic_tools.domain.model.skill.model import (
    ALIAS_HEX_CHARACTERS,
    DEFAULT_VERSION_PREFIX,
    MARKDOWN_SUFFIXES,
    SKILL_FILE,
    BundleEntry,
    Plugin,
    Skill,
    SkillBundle,
    SkillCatalog,
    SkillFile,
    bundle_digest,
    extension_identifier,
)
from snowflake_semantic_tools.domain.model.skill.references import IGNORE_MARKER, PathReference, scan_references
from snowflake_semantic_tools.domain.model.skill.validate import validate_skill_catalog

__all__ = [
    "ALIAS_HEX_CHARACTERS",
    "BUNDLE_BUDGET_BYTES",
    "CONVENTIONAL_DIRS",
    "DEFAULT_VERSION_PREFIX",
    "IGNORE_MARKER",
    "MARKDOWN_SUFFIXES",
    "PLUGIN_MANIFEST",
    "SCAN_MAX_FILES",
    "SCAN_MAX_FILE_BYTES",
    "SCAN_MAX_TOTAL_BYTES",
    "SKILL_FILE",
    "SKILL_MD_BUDGET_BYTES",
    "BundleEntry",
    "PathReference",
    "Plugin",
    "Skill",
    "SkillBundle",
    "SkillCatalog",
    "SkillFile",
    "build_plugin_bundle",
    "build_skill_bundle",
    "bundle_digest",
    "extension_identifier",
    "flatten_skill",
    "flattened_name",
    "plugin_manifest_json",
    "scan_references",
    "validate_skill_catalog",
]
