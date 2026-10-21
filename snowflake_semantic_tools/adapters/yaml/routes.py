"""Check a routed block's folder routes against the directories they name.

Every artifact type the registry gives a `routed_block` routes by folder: an unprefixed key of
that block names a directory below the type's route root. What a route resolves to is
`domain.model.config_schema.resolve_settings`; whether its directory exists is a question for
the filesystem, so it is answered here.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.config_schema import CHILDREN, route_root
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY


def routed_diagnostics(
    config: Mapping[str, Any], project_dir: Path, *types: str, also_under: tuple[str, ...] = ()
) -> tuple[Diagnostic, ...]:
    """Report each folder route of each named type's routed block that names no directory.

    Each type's block and route root are what the artifact registry declares for it, so a type
    is checked as every other is. A type the registry gives no routed block is skipped.

    Args:
        types: Artifact type names, such as `skill`, checked in the order given.
        also_under: Artifact types whose route roots hold folders the routes may name too, as
            an eval's routes name a CoWork profile's folder as well as an agent's; a route is
            reported only when it names a directory under none of the roots.

    Diagnostics:
        SST-CFG041: as `folder_route_diagnostics` reports it.
    """
    diagnostics: list[Diagnostic] = []
    for name in types:
        artifact = ARTIFACT_REGISTRY.artifacts[name]
        if artifact.routed_block:
            root = project_dir / route_root(config, artifact)
            more = tuple(project_dir / route_root(config, ARTIFACT_REGISTRY.artifacts[other]) for other in also_under)
            diagnostics.extend(folder_route_diagnostics(dict(config), artifact.routed_block, root, *more))
    return tuple(diagnostics)


def folder_route_diagnostics(config: dict[str, Any], block: str, root: Path, *more: Path) -> tuple[Diagnostic, ...]:
    """Report each folder route under `block:` that names no directory under `root` or `more`.

    A route is a key whose value is a mapping and which the block does not declare; a `+` key is a
    setting, not a route. Routes nest as folders do and are walked depth first, and none below a
    directory missing under every root is checked. Nothing is reported when the block is not a
    mapping.

    Diagnostics:
        SST-CFG041: when a route names no directory; the subject is `config_route:<dotted route>`.
    """
    node = config.get(block) or {}
    if not isinstance(node, dict):
        return ()
    diagnostics: list[Diagnostic] = []
    declared = CHILDREN.get(block, {})

    def walk(level: dict[str, Any], directories: tuple[Path, ...], route: str) -> None:
        for key, value in level.items():
            key_text = str(key)
            if key_text.startswith("+") or key_text in declared or not isinstance(value, dict):
                continue
            children = tuple(child for directory in directories if (child := directory / key_text).is_dir())
            child_route = f"{route}.{key_text}" if route else key_text
            if not children:
                diagnostics.append(
                    D(
                        "SST-CFG041",
                        key=key_text,
                        block=block,
                        root=str(root),
                        subject=f"config_route:{child_route}",
                    )
                )
                continue
            walk(value, children, child_route)

    walk(node, (root, *more), "")
    return tuple(diagnostics)
