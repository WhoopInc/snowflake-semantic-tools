"""CoCo Desktop profiles: content-addressed stage trees and one registry row each.

A profile publishes as trees below the fixed `by_type` prefixes Desktop reads --
`skills/`, `prompts/`, `mcp/`, `hooks/`, `commands/`, `plugins/` -- each named by
the digest of its own content, then one registry row whose pointers name those
trees. Uploading a new tree never touches a tree a live row points at, so the row
write is the atomic switch and the previous trees remain for rollback.

`model` holds the records, `validate` checks what profiles name, and `build` renders one
profile's release. This module only re-exports their public names.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.profile.build import assemble_prompt, build_profile
from snowflake_semantic_tools.domain.model.profile.model import (
    COMMAND_FRONTMATTER_KEYS,
    DESKTOP_REGISTRY,
    REJECTED_PROFILE_KEYS,
    SHARED_PROFILE,
    TREE_HASH_CHARACTERS,
    CommandFile,
    DesktopProfile,
    HookDefinition,
    McpConfig,
    ProfileCatalog,
    ProfileRelease,
    SharedProfile,
    StageTree,
)
from snowflake_semantic_tools.domain.model.profile.validate import unreached_skills, validate_profile_catalog

__all__ = [
    "COMMAND_FRONTMATTER_KEYS",
    "DESKTOP_REGISTRY",
    "REJECTED_PROFILE_KEYS",
    "SHARED_PROFILE",
    "TREE_HASH_CHARACTERS",
    "CommandFile",
    "DesktopProfile",
    "HookDefinition",
    "McpConfig",
    "ProfileCatalog",
    "ProfileRelease",
    "SharedProfile",
    "StageTree",
    "assemble_prompt",
    "build_profile",
    "unreached_skills",
    "validate_profile_catalog",
]
