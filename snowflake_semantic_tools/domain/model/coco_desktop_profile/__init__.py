"""CoCo Desktop profiles: content-addressed stage trees and one registry row each.

A profile publishes as trees below the fixed `by_type` prefixes Desktop reads --
`skills/`, `prompts/`, `mcp/`, `hooks/`, `commands/`, `plugins/` -- each named by
the digest of its own content, then one registry row whose pointers name those
trees. Uploading a new tree never touches a tree a live row points at, so the row
write is the atomic switch and the previous trees remain for rollback.

`model` holds the records; `domain.render.coco_desktop_profile` renders one profile's release and
`domain.validate.coco_desktop_profile` checks what profiles name. This module only re-exports `model`'s
public names.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.coco_desktop_profile.model import (
    COMMAND_FRONTMATTER_KEYS,
    DESKTOP_REGISTRY,
    NAME_SUFFIX_PATTERN,
    REJECTED_COCO_DESKTOP_PROFILE_KEYS,
    SHARED_PROFILE,
    TREE_HASH_CHARACTERS,
    CocoDesktopProfile,
    CocoDesktopProfileCatalog,
    CocoDesktopProfileRelease,
    CommandFile,
    HookDefinition,
    McpConfig,
    SharedCocoDesktopProfile,
    StageTree,
    is_counter,
    next_version,
    same_version,
)

__all__ = [
    "COMMAND_FRONTMATTER_KEYS",
    "DESKTOP_REGISTRY",
    "NAME_SUFFIX_PATTERN",
    "REJECTED_COCO_DESKTOP_PROFILE_KEYS",
    "SHARED_PROFILE",
    "TREE_HASH_CHARACTERS",
    "CommandFile",
    "CocoDesktopProfile",
    "HookDefinition",
    "McpConfig",
    "CocoDesktopProfileCatalog",
    "CocoDesktopProfileRelease",
    "SharedCocoDesktopProfile",
    "StageTree",
    "is_counter",
    "next_version",
    "same_version",
]
