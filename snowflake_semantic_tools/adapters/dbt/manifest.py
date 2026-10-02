"""Read dbt's manifest.json into the narrow catalog SST consumes."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel

SUPPORTED_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"

# The `meta.sst` keys SST reads. `database` and `schema` are read only to be
# refused (SST-DBT030); anything else is reported where a view uses the model.
MODEL_META_KEYS = frozenset(("primary_key", "unique_keys", "database", "schema"))
COLUMN_META_KEYS = frozenset(("column_type", "data_type", "synonyms", "sample_values", "is_enum", "exclude"))

# Tests whose columns hold keys. A numeric column one of them names is an identifier, so
# enrich derives it as a dimension, not as a fact to sum.
KEY_TESTS = frozenset(("unique", "unique_combination_of_columns", "relationships"))


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


def _metas(node: Mapping[str, Any]) -> tuple[object, object]:
    """Return a node's bare `meta` and its `config.meta`, either None when absent."""
    config = node.get("config")
    return node.get("meta"), config.get("meta") if isinstance(config, dict) else None


def _sst_meta(value: object, *, path: str) -> Mapping[str, Any]:
    node = _mapping(value, path=path)
    # dbt can write `meta` both bare and under `config`, one of them without `sst` (often
    # `meta: {}`), so a place without it must not hide the other. The bare `sst` wins over config's.
    for meta in _metas(node):
        if meta is None:
            continue
        sst = _mapping(meta, path=f"{path}.meta").get("sst")
        if sst is not None:
            return _mapping(sst, path=f"{path}.meta.sst")
    return {}


def _pii_tagged(node: Mapping[str, Any]) -> bool:
    """Report whether a column carries `pii_tags` beside `sst`, bare or under config, of any category."""
    return any(isinstance(meta, dict) and bool(meta.get("pii_tags")) for meta in _metas(node))


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
        is_enum=None if meta.get("is_enum") is None else bool(meta.get("is_enum")),
        excluded=bool(meta.get("exclude", False)),
        unknown_meta_keys=tuple(sorted(str(key) for key in meta if key not in COLUMN_META_KEYS)),
        declared_data_type=declared_type if disagrees else None,
        declared_keys=frozenset(str(key) for key in meta),
        native_data_type=native_type,
        pii_tagged=_pii_tagged(node),
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


def _patch_file(patch_path: str | None) -> str | None:
    """Return `patch_path` relative to the project root: dbt writes it as `<package>://<path>`."""
    if patch_path is None:
        return None
    _, separator, path = patch_path.partition("://")
    return path if separator else patch_path


def _test_columns(node: Mapping[str, Any], metadata: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the columns one key test names, casefolded: its column, or its combination of columns."""
    kwargs = metadata.get("kwargs")
    arguments = kwargs if isinstance(kwargs, dict) else {}
    combination = arguments.get("combination_of_columns")
    named = [node.get("column_name"), arguments.get("column_name")]
    named.extend(combination if isinstance(combination, list) else ())
    return tuple(column.strip().casefold() for column in named if isinstance(column, str) and column.strip())


def _key_test_columns(nodes: Mapping[str, Any]) -> dict[str, frozenset[str]]:
    """Return the casefolded columns key tests name, by the unique id of the model each tests.

    A test that is attached to no model, or is not one of `KEY_TESTS`, is ignored; a node that is
    not a mapping is left for the model pass to report.
    """
    found: dict[str, set[str]] = {}
    for node in nodes.values():
        if not isinstance(node, dict) or node.get("resource_type") != "test":
            continue
        metadata = node.get("test_metadata")
        model = node.get("attached_node")
        if not isinstance(metadata, dict) or metadata.get("name") not in KEY_TESTS or not isinstance(model, str):
            continue
        found.setdefault(model, set()).update(_test_columns(node, metadata))
    return {model: frozenset(columns) for model, columns in found.items()}


def _model(unique_id: object, raw_node: object, key_columns: frozenset[str]) -> DbtModel | str | None:
    """Project one manifest node; the name of a model with nothing to read; None for any other node.

    Every node must be a mapping; only a model is read, and a model with neither a relation
    nor SST metadata is left out, its name returned so the catalog can say it has no relation.

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
        return name
    return _build_model(unique_id, name, node, meta, path=path, key_columns=key_columns)


def _build_model(
    unique_id: object,
    name: str,
    node: Mapping[str, Any],
    meta: Mapping[str, Any],
    *,
    path: str,
    key_columns: frozenset[str] = frozenset(),
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
        key_test_columns=key_columns,
        package_name=_text(node.get("package_name")),
        raw_relation_name=_text(node.get("relation_name")),
        patch_file=_patch_file(_text(node.get("patch_path"))),
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
    key_tests = _key_test_columns(nodes)
    models: list[DbtModel] = []
    relationless: list[str] = []
    for unique_id, raw_node in sorted(nodes.items()):
        model = _model(unique_id, raw_node, key_tests.get(str(unique_id), frozenset()))
        if isinstance(model, DbtModel):
            models.append(model)
        elif model is not None:
            relationless.append(model)

    return DbtCatalog(
        schema_version=schema_version,
        dbt_version=_text(metadata.get("dbt_version")),
        project_name=_text(metadata.get("project_name")),
        models=tuple(models),
        relationless_models=tuple(relationless),
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
