"""Skills and plugins: authored folders, plugin manifests, and the bundles they publish as.

A skill is its folder: `SKILL.md` and every file and folder beside it, nested skills
included. Every channel publishes the folder as authored, so a relative path that resolves
in the repository resolves where an agent reads the skill.

`model` holds the records and the identities derived from them. Finding the paths a file
names is `domain.parse.skill_references`, checking a skill's files and bundling are
`domain.validate.skill_files` and `domain.render.skill_bundle`, and checking the catalog is
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
