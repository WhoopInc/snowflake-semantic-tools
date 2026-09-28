"""Read dbt's manifest.json into the narrow catalog SST consumes."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ...domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from ...domain.model.diagnostic import D
from ..project import ProjectError

SUPPORTED_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"


def _mapping(value: object, *, path: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ProjectError(f"dbt manifest {path} must be an object")
    return value


def _strings(value: object, *, path: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ProjectError(f"dbt manifest {path} must be a list")
    return tuple(_string_value(item) for item in value)


def _string_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _unique_keys(value: object, *, path: str) -> tuple[tuple[str, ...], ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ProjectError(f"dbt manifest {path} must be a list of column lists")
    keys: list[tuple[str, ...]] = []
    for index, key in enumerate(value):
        keys.append(_strings(key, path=f"{path}[{index}]"))
    return tuple(keys)


def _sst_meta(value: object, *, path: str) -> Mapping[str, Any]:
    node = _mapping(value, path=path)
    meta = node.get("meta")
    if meta is None:
        config = node.get("config")
        if isinstance(config, dict):
            meta = config.get("meta")
    if meta is None:
        return {}
    meta_map = _mapping(meta, path=f"{path}.meta")
    sst = meta_map.get("sst")
    return {} if sst is None else _mapping(sst, path=f"{path}.meta.sst")


def _column(name: str, value: object, *, node_path: str) -> DbtColumn:
    path = f"{node_path}.columns.{name}"
    node = _mapping(value, path=path)
    meta = _sst_meta(node, path=path)
    description = str(node.get("description") or "").strip() or None
    data_type = str(node.get("data_type") or meta.get("data_type") or "").strip() or None
    column_type = str(meta.get("column_type") or "").strip() or None
    return DbtColumn(
        name=str(node.get("name") or name),
        description=description,
        data_type=data_type,
        column_type=column_type,
        synonyms=_strings(meta.get("synonyms"), path=f"{path}.meta.sst.synonyms"),
        sample_values=_strings(meta.get("sample_values"), path=f"{path}.meta.sst.sample_values"),
        is_enum=bool(meta.get("is_enum", False)),
        excluded=bool(meta.get("exclude", False)),
    )


def _relation_name(node: Mapping[str, Any], *, path: str) -> str:
    relation = str(node.get("relation_name") or "").strip()
    if not relation:
        raise ProjectError(f"dbt manifest {path}.relation_name is required for an SST model")
    return relation.upper()


def catalog_from_document(document: object) -> DbtCatalog:
    """Project a decoded manifest document into immutable domain values."""
    root = _mapping(document, path="root")
    metadata = _mapping(root.get("metadata"), path="metadata")
    schema_version = str(metadata.get("dbt_schema_version") or "")
    if schema_version != SUPPORTED_SCHEMA:
        diagnostic = D("SST-PRT007", found=schema_version, expected=SUPPORTED_SCHEMA)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))

    nodes = _mapping(root.get("nodes"), path="nodes")
    models: list[DbtModel] = []
    for unique_id, raw_node in sorted(nodes.items()):
        path = f"nodes.{unique_id}"
        node = _mapping(raw_node, path=path)
        if node.get("resource_type") != "model":
            continue
        name = str(node.get("name") or "").strip()
        if not name:
            raise ProjectError(f"dbt manifest {path}.name is required")
        meta = _sst_meta(node, path=path)
        raw_columns = _mapping(node.get("columns") or {}, path=f"{path}.columns")
        columns = tuple(_column(str(column_name), value, node_path=path) for column_name, value in raw_columns.items())
        models.append(
            DbtModel(
                unique_id=str(unique_id),
                name=name,
                relation_name=_relation_name(node, path=path),
                primary_key=_strings(meta.get("primary_key"), path=f"{path}.config.meta.sst.primary_key"),
                unique_keys=_unique_keys(meta.get("unique_keys"), path=f"{path}.config.meta.sst.unique_keys"),
                columns=columns,
                original_file_path=str(node.get("original_file_path") or "").strip() or None,
                patch_path=str(node.get("patch_path") or "").strip() or None,
                forbidden_location_keys=tuple(key for key in ("database", "schema") if key in meta),
                description=str(node.get("description") or "").strip() or None,
            )
        )

    return DbtCatalog(
        schema_version=schema_version,
        dbt_version=str(metadata.get("dbt_version") or "").strip() or None,
        project_name=str(metadata.get("project_name") or "").strip() or None,
        models=tuple(models),
    )


def load_manifest_catalog(path: Path) -> DbtCatalog:
    """Read and decode one dbt manifest without consulting dbt model YAML."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ProjectError(f"cannot read dbt manifest {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ProjectError(f"dbt manifest {path} is not valid JSON: {exc}") from exc
    return catalog_from_document(document)
