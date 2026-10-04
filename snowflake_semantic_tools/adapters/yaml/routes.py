"""Check a routed block's folder routes against the directories they name.

`semantic_views:` and `agents:` route by folder: an unprefixed key names a directory below the
block's artifact root. What a route resolves to is `domain.model.config_schema.routed_block`;
whether its directory exists is a question for the filesystem, so it is answered here.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic


def folder_route_diagnostics(config: dict[str, Any], block: str, root: Path) -> tuple[Diagnostic, ...]:
    """Report each folder route under `block:` that names no directory under `root`.

    A route is a key whose value is a mapping; a `+` key is a setting, not a route. Routes nest
    as folders do and are walked depth first, and none below a missing directory is checked.
    Nothing is reported when the block is not a mapping.

    Diagnostics:
        SST-CFG041: when a route names no directory; the subject is `config_route:<dotted route>`.
    """
    node = config.get(block) or {}
    if not isinstance(node, dict):
        return ()
    diagnostics: list[Diagnostic] = []

    def walk(level: dict[str, Any], directory: Path, route: str) -> None:
        for key, value in level.items():
            key_text = str(key)
            if key_text.startswith("+") or not isinstance(value, dict):
                continue
            child = directory / key_text
            child_route = f"{route}.{key_text}" if route else key_text
            if not child.is_dir():
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
            walk(value, child, child_route)

    walk(node, root, "")
    return tuple(diagnostics)
