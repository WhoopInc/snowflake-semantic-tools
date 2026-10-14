"""A view's settings, resolved through its `config:` and the folder routes, and where it publishes."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments
from snowflake_semantic_tools.adapters.yaml.routes import folder_route_diagnostics
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.config_schema import (
    Setting,
    resolve_settings,
    setting_value,
    target_text,
    with_defaults,
)
from snowflake_semantic_tools.domain.model.dbt import DbtTarget
from snowflake_semantic_tools.domain.validate.semantic.nodes import node_root


def _view_settings(
    config: Mapping[str, Any], path: Path, views_dir: Path, node: Mapping[str, Any] | None = None
) -> Mapping[str, Setting]:
    """Resolve a view's settings: its own `config:`, then the folder routes over its file, then the root.

    The folder is the view file's directory below `views_dir`.
    """
    folder = path.resolve().relative_to(views_dir.resolve()).parent.parts
    return resolve_settings(config.get("semantic_views"), folder, (node or {}).get("config"))


def _semantic_view_target(
    config: Mapping[str, Any], path: Path, views_dir: Path, target: DbtTarget, node: Mapping[str, Any] | None = None
) -> DbtTarget:
    """Return where a view publishes: its resolved `database` and `schema`, else the target's own.

    A setting that renders empty -- a target conditional whose chosen arm is `''` -- is unset,
    so the target's own value applies, as it does for every other block's location.
    """
    return _view_location(_view_settings(config, path, views_dir, node), target)


def _view_location(settings: Mapping[str, Setting], target: DbtTarget) -> DbtTarget:
    return DbtTarget(
        database=target_text(setting_value(settings, "database"), target, None) or target.database,
        schema=target_text(setting_value(settings, "schema"), target, None) or target.schema,
    )


def _view_provenance(settings: Mapping[str, Setting], location: DbtTarget) -> Mapping[str, Setting]:
    """Return a view's settings as it reports them: the location rendered, and each default it took."""
    rendered = {
        key: Setting(key, value, settings[key].source)
        for key, value in (("database", location.database), ("schema", location.schema))
        if key in settings
    }
    filled = with_defaults(
        {**settings, **rendered}, {"database": location.database, "schema": location.schema, "enabled": True}
    )
    return MappingProxyType({item.key: item for item in filled})


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
