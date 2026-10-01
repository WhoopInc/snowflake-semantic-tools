"""Read dbt's manifest.json into the narrow catalog SST consumes."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.diagnostic import D

SUPPORTED_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"

# The `meta.sst` keys SST reads. `database` and `schema` are read only to be
# refused (SST-DBT030); anything else is reported where a view uses the model.
MODEL_META_KEYS = frozenset(("primary_key", "unique_keys", "database", "schema"))
COLUMN_META_KEYS = frozenset(("column_type", "data_type", "synonyms", "sample_values", "is_enum", "exclude"))


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


def _primary_key(value: object, *, path: str) -> tuple[tuple[str, ...], bool]:
    """The key columns, and whether they were written in the 0.3 string form.

    A 0.3 string (`id`, or `a, b`) is not read: guessing its columns would
    decide which joins render. It is reported where a view uses the model.
    """
    if isinstance(value, str):
        return (), bool(value.strip())
    return _strings(value, path=path), False


def _unique_keys(value: object, *, path: str) -> tuple[tuple[tuple[str, ...], ...], bool]:
    """The unique keys, and whether they were written in a 0.3 form.

    1.0 reads a list of column lists. The 0.3 forms -- a comma-separated string,
    a flat list of names, or a bare name inside the list -- are not read, and are
    reported like `primary_key`.
    """
    if value is None:
        return (), False
    if isinstance(value, str):
        return (), bool(value.strip())
    if not isinstance(value, list):
        raise ProjectError(f"dbt manifest {path} must be a list of column lists")
    if any(not isinstance(key, list) for key in value):
        return (), True
    return tuple(_strings(key, path=f"{path}[{index}]") for index, key in enumerate(value)), False


def _sst_meta(value: object, *, path: str) -> Mapping[str, Any]:
    node = _mapping(value, path=path)
    config = node.get("config")
    # dbt can write `meta` both bare and under `config`, one of them without `sst` (often
    # `meta: {}`), so a place without it must not hide the other. The bare `sst` wins over config's.
    for meta in (node.get("meta"), config.get("meta") if isinstance(config, dict) else None):
        if meta is None:
            continue
        sst = _mapping(meta, path=f"{path}.meta").get("sst")
        if sst is not None:
            return _mapping(sst, path=f"{path}.meta.sst")
    return {}


def _column(name: str, value: object, *, node_path: str) -> DbtColumn:
    """Project one column node; dbt's own `data_type` wins over `meta.sst.data_type`.

    A `meta.sst.data_type` that disagrees with dbt's, compared without case or whitespace, is
    kept as `declared_data_type` for the validator to report.
    """
    path = f"{node_path}.columns.{name}"
    node = _mapping(value, path=path)
    meta = _sst_meta(node, path=path)
    description = _text(node.get("description"))
    native_type = _text(node.get("data_type"))
    declared_type = _text(meta.get("data_type"))
    data_type = native_type or declared_type
    column_type = _text(meta.get("column_type"))
    # dbt's own data_type wins; a meta.sst.data_type that says otherwise is reported.
    disagrees = (
        native_type is not None and declared_type is not None and _type_key(native_type) != _type_key(declared_type)
    )
    return DbtColumn(
        name=str(node.get("name") or name),
        description=description,
        data_type=data_type,
        column_type=column_type,
        synonyms=_strings(meta.get("synonyms"), path=f"{path}.meta.sst.synonyms"),
        sample_values=_strings(meta.get("sample_values"), path=f"{path}.meta.sst.sample_values"),
        is_enum=bool(meta.get("is_enum", False)),
        excluded=bool(meta.get("exclude", False)),
        unknown_meta_keys=tuple(sorted(str(key) for key in meta if key not in COLUMN_META_KEYS)),
        declared_data_type=declared_type if disagrees else None,
    )


def _type_key(data_type: str) -> str:
    return "".join(data_type.upper().split())


def _text(value: object) -> str | None:
    """Return a manifest field as stripped text, or None when it is falsy or only whitespace."""
    return str(value or "").strip() or None


def _relation_name(node: Mapping[str, Any], *, path: str) -> str:
    relation = str(node.get("relation_name") or "").strip()
    if not relation:
        raise ProjectError(f"dbt manifest {path}.relation_name is required for an SST model")
    return relation.upper()


def _model(unique_id: object, raw_node: object) -> DbtModel | None:
    """Project one manifest node, or return None for a node SST does not read.

    Every node must be a mapping; only a model is read, and a model with neither a relation
    nor SST metadata is left out.

    Raises:
        ProjectError: The node is not a mapping, or the model has no name or a malformed part.
    """
    path = f"nodes.{unique_id}"
    node = _mapping(raw_node, path=path)
    if node.get("resource_type") != "model":
        return None
    name = str(node.get("name") or "").strip()
    if not name:
        raise ProjectError(f"dbt manifest {path}.name is required")
    meta = _sst_meta(node, path=path)
    if not meta and not str(node.get("relation_name") or "").strip():
        # An ephemeral model has no relation to query and, without SST
        # metadata, nothing to validate; a view that names it gets SST-MEM003.
        return None
    return _build_model(unique_id, name, node, meta, path=path)


def _build_model(
    unique_id: object, name: str, node: Mapping[str, Any], meta: Mapping[str, Any], *, path: str
) -> DbtModel:
    """Read a model's columns, keys and relation, in that order, into its domain value.

    The order decides which problem a malformed model reports: its first bad column, then its
    keys, then a missing relation.

    Raises:
        ProjectError: A column or key has the wrong shape, or the model has no relation name.
    """
    raw_columns = _mapping(node.get("columns") or {}, path=f"{path}.columns")
    columns = tuple(_column(str(column_name), value, node_path=path) for column_name, value in raw_columns.items())
    primary_key, legacy_primary_key = _primary_key(meta.get("primary_key"), path=f"{path}.config.meta.sst.primary_key")
    unique_keys, legacy_unique_keys = _unique_keys(meta.get("unique_keys"), path=f"{path}.config.meta.sst.unique_keys")
    return DbtModel(
        unique_id=str(unique_id),
        name=name,
        relation_name=_relation_name(node, path=path),
        primary_key=primary_key,
        unique_keys=unique_keys,
        columns=columns,
        original_file_path=_text(node.get("original_file_path")),
        patch_path=_text(node.get("patch_path")),
        forbidden_location_keys=tuple(key for key in ("database", "schema") if key in meta),
        description=_text(node.get("description")),
        legacy_key_fields=tuple(
            field
            for field, legacy in (("primary_key", legacy_primary_key), ("unique_keys", legacy_unique_keys))
            if legacy
        ),
        unknown_meta_keys=tuple(sorted(str(key) for key in meta if key not in MODEL_META_KEYS)),
    )


def catalog_from_document(document: object) -> DbtCatalog:
    """Project a decoded manifest document into immutable domain values.

    Nodes are read in sorted unique-id order, and the catalog keeps their models in that order.

    Raises:
        ProjectError: The schema version is not `SUPPORTED_SCHEMA`, or a part SST reads has the
            wrong shape, such as a model without a name or a relation.

    Diagnostics:
        SST-PRT007: the manifest's `dbt_schema_version` is not `SUPPORTED_SCHEMA`; raised.
    """
    root = _mapping(document, path="root")
    metadata = _mapping(root.get("metadata"), path="metadata")
    schema_version = str(metadata.get("dbt_schema_version") or "")
    if schema_version != SUPPORTED_SCHEMA:
        diagnostic = D("SST-PRT007", found=schema_version, expected=SUPPORTED_SCHEMA)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))

    nodes = _mapping(root.get("nodes"), path="nodes")
    models: list[DbtModel] = []
    for unique_id, raw_node in sorted(nodes.items()):
        model = _model(unique_id, raw_node)
        if model is not None:
            models.append(model)

    return DbtCatalog(
        schema_version=schema_version,
        dbt_version=_text(metadata.get("dbt_version")),
        project_name=_text(metadata.get("project_name")),
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
