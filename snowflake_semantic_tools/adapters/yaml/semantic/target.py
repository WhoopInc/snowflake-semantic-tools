"""Where a view publishes: the `semantic_views:` folder routes and the target values they render."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments
from snowflake_semantic_tools.adapters.yaml.routes import folder_route_diagnostics
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.config_schema import routed_block
from snowflake_semantic_tools.domain.model.dbt import DbtTarget
from snowflake_semantic_tools.domain.validate.semantic.nodes import node_root


def _render_target_value(value: object, target: DbtTarget) -> str:
    return str(value).replace("{{ target.database }}", target.database).replace("{{ target.schema }}", target.schema)


def _semantic_view_defaults(config: dict[str, Any], path: Path, views_dir: Path) -> dict[str, object]:
    """Merge `semantic_views` `+` keys along the folder routes above one file, without their prefix."""
    folder = path.resolve().relative_to(views_dir.resolve()).parent.parts
    return {key[1:]: value for key, value in routed_block(config.get("semantic_views"), folder).items()}


def _semantic_view_target(config: dict[str, Any], path: Path, views_dir: Path, target: DbtTarget) -> DbtTarget:
    resolved = _semantic_view_defaults(config, path, views_dir)
    database = _render_target_value(resolved.get("database", target.database), target)
    schema = _render_target_value(resolved.get("schema", target.schema), target)
    return DbtTarget(database=database, schema=schema)


def _folder_route_diagnostics(config: dict[str, Any], views_dir: Path) -> tuple[Diagnostic, ...]:
    """Report each folder route under `semantic_views:` that names no directory under `views_dir`.

    Diagnostics:
        SST-CFG041: as `folder_route_diagnostics` reports it.
    """
    return folder_route_diagnostics(config, "semantic_views", views_dir)


def _stray_view_diagnostics(documents: RawDocuments, views_dir: Path) -> tuple[Diagnostic, ...]:
    """Report each view list outside `views_dir` as an unread key (SST-PRS004).

    Views are validated from every file but built only from files under `views_dir`, so
    such a list would otherwise be dropped without a word.
    """
    root_key = node_root("semantic_view")
    built = {document.path for document in documents.under(views_dir, root_key)}
    diagnostics: list[Diagnostic] = []
    for document in documents.documents:
        if document.path in built or not isinstance(document.tree.get(root_key), list):
            continue
        position = document.position((root_key,))
        origin = Origin(document.path, position.line if position else None, position.col if position else None)
        diagnostics.append(D("SST-PRS004", origin=origin, artifact=document.path, field=root_key))
    return tuple(diagnostics)
