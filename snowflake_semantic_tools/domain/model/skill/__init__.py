"""Skills and plugins: authored folders, plugin manifests, and the bundles they publish as.

A skill is authored nested, the way CoCo Desktop reads it, and flattened only for
the catalog channel, because a Cortex Agent reads supporting files beside
`SKILL.md` and never descends into a subdirectory.

`model` holds the records and the identities derived from them. Finding the paths a file
names is `domain.parse.skill_references`, flattening and bundling are
`domain.render.skill_flatten` and `domain.render.skill_bundle`, and checking the catalog is
`domain.validate.skill`. This module only re-exports `model`'s public names.
"""

from __future__ import annotations

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

__all__ = [
    "ALIAS_HEX_CHARACTERS",
    "DEFAULT_VERSION_PREFIX",
    "MARKDOWN_SUFFIXES",
    "SKILL_FILE",
    "BundleEntry",
    "Plugin",
    "Skill",
    "SkillBundle",
    "SkillCatalog",
    "SkillFile",
    "bundle_digest",
    "extension_identifier",
]
