"""Where a view publishes: the `semantic_views:` folder routes and the target values they render."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ....domain.model.dbt import DbtTarget
from ....domain.model.diagnostic import D, Diagnostic, Origin
from ..documents import RawDocuments
from .nodes import _node_root


def _render_target_value(value: object, target: DbtTarget) -> str:
    return str(value).replace("{{ target.database }}", target.database).replace("{{ target.schema }}", target.schema)


def _semantic_view_defaults(config: dict[str, Any], path: Path, views_dir: Path) -> dict[str, object]:
    """Merge `semantic_views` `+` keys along the folder routes above one file."""
    block = config.get("semantic_views") or {}
    if not isinstance(block, dict):
        return {}
    resolved: dict[str, object] = {key[1:]: value for key, value in block.items() if str(key).startswith("+")}
    relative_parent = path.resolve().relative_to(views_dir.resolve()).parent
    cursor: object = block
    for part in relative_parent.parts:
        if not isinstance(cursor, dict):
            break
        child = cursor.get(part)
        if not isinstance(child, dict):
            break
        resolved.update({key[1:]: value for key, value in child.items() if str(key).startswith("+")})
        cursor = child
    return resolved


def _semantic_view_target(config: dict[str, Any], path: Path, views_dir: Path, target: DbtTarget) -> DbtTarget:
    resolved = _semantic_view_defaults(config, path, views_dir)
    database = _render_target_value(resolved.get("database", target.database), target)
    schema = _render_target_value(resolved.get("schema", target.schema), target)
    return DbtTarget(database=database, schema=schema)


def _folder_route_diagnostics(config: dict[str, Any], views_dir: Path) -> tuple[Diagnostic, ...]:
    """Report each folder route under `semantic_views:` that names no directory under `views_dir`.

    A route is a key whose value is a mapping; a `+` key is a setting, not a route. Routes nest
    as folders do and are walked depth first, and none below a missing directory is checked.
    Nothing is reported when the block is not a mapping.

    Diagnostics:
        SST-CFG041: when a route names no directory; the subject is `config_route:<dotted route>`.
    """
    block = config.get("semantic_views") or {}
    if not isinstance(block, dict):
        return ()
    diagnostics: list[Diagnostic] = []

    def walk(node: dict[str, Any], directory: Path, route: str) -> None:
        for key, value in node.items():
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
                        block="semantic_views",
                        root=str(views_dir),
                        subject=f"config_route:{child_route}",
                    )
                )
                continue
            walk(value, child, child_route)

    walk(block, views_dir, "")
    return tuple(diagnostics)


def _stray_view_diagnostics(documents: RawDocuments, views_dir: Path) -> tuple[Diagnostic, ...]:
    """Report each view list outside `views_dir` as an unread key (SST-PRS004).

    Views are validated from every file but built only from files under `views_dir`, so
    such a list would otherwise be dropped without a word.
    """
    root_key = _node_root("semantic_view")
    built = {document.path for document in documents.under(views_dir, root_key)}
    diagnostics: list[Diagnostic] = []
    for document in documents.documents:
        if document.path in built or not isinstance(document.tree.get(root_key), list):
            continue
        position = document.position((root_key,))
        origin = Origin(document.path, position.line if position else None, position.col if position else None)
        diagnostics.append(D("SST-PRS004", origin=origin, artifact=document.path, field=root_key))
    return tuple(diagnostics)
