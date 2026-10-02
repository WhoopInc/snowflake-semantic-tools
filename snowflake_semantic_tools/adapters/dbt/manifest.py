"""Read dbt's manifest.json into the narrow catalog SST consumes."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel, DbtSource

SUPPORTED_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"
# The manifest schema versions SST reads: an explicit set, never a range, since dbt makes no
# promise that a later version is a superset of an earlier one.
SUPPORTED_SCHEMA_VERSIONS = frozenset((12,))
# Both spellings dbt has written: `.../manifest/v12.json` and `.../manifest/v12/manifest.json`.
_SCHEMA_URL = re.compile(r"^https?://schemas\.getdbt\.com/dbt/manifest/v(\d+)(?:\.json|/manifest\.json)$")

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
        access_modifier=_text(meta.get("access_modifier")),
    )


def _type_key(data_type: str) -> str:
    return "".join(data_type.upper().split())


def _text(value: object) -> str | None:
    """Return a manifest field as stripped text, or None when it is falsy or only whitespace."""
    return str(value or "").strip() or None


def _relation_name(node: Mapping[str, Any]) -> str:
    return str(node.get("relation_name") or "").strip().upper()


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


def _model(
    unique_id: object, raw_node: object, key_columns: frozenset[str], test_count: int = 0
) -> DbtModel | tuple[str, str] | Diagnostic | None:
    """Project one manifest node, or say why the catalog leaves it out.

    Every node must be a mapping; only a model is read. A model with no relation is returned
    as its name and its materialisation, so the catalog can say why it has none.

    Returns:
        The model; `(name, materialisation)` for a model with no relation; the diagnostic of a
        model SST cannot use; None for any other node.

    Raises:
        ProjectError: The node is not a mapping, or the model has a malformed part.

    Diagnostics:
        SST-DBT013: the model has no name; it is skipped.
        SST-DBT014: the model has a relation but an empty `database` or `schema`; it is skipped.
    """
    path = f"nodes.{unique_id}"
    node = _mapping(raw_node, path=path)
    if node.get("resource_type") != "model":
        return None
    name = str(node.get("name") or "").strip()
    if not name:
        return D("SST-DBT013", value=str(unique_id))
    if not str(node.get("relation_name") or "").strip():
        # An ephemeral model has no relation to query: a view that names it gets SST-DBT009.
        config = node.get("config")
        materialized = config.get("materialized") if isinstance(config, dict) else None
        return name, str(materialized or "ephemeral")
    empty = next((key for key in ("database", "schema") if key in node and not _text(node.get(key))), None)
    if empty is not None:
        return D("SST-DBT014", model=name, key=empty, subject=f"dbt_model:{name}")
    meta = _sst_meta(node, path=path)
    return _build_model(unique_id, name, node, meta, path=path, key_columns=key_columns, test_count=test_count)


def _build_model(
    unique_id: object,
    name: str,
    node: Mapping[str, Any],
    meta: Mapping[str, Any],
    *,
    path: str,
    key_columns: frozenset[str] = frozenset(),
    test_count: int = 0,
) -> DbtModel:
    """Read a model's columns, then its keys, into its domain value.

    The order decides which problem a malformed model reports: its first bad column, then its keys.

    Raises:
        ProjectError: A column or key has the wrong shape.
    """
    raw_columns = _mapping(node.get("columns") or {}, path=f"{path}.columns")
    columns = tuple(_column(str(column_name), value, node_path=path) for column_name, value in raw_columns.items())
    primary_key, legacy_primary_key = _primary_key(meta.get("primary_key"), path=f"{path}.config.meta.sst.primary_key")
    unique_keys, legacy_unique_keys = _unique_keys(meta.get("unique_keys"), path=f"{path}.config.meta.sst.unique_keys")
    return DbtModel(
        unique_id=str(unique_id),
        name=name,
        relation_name=_relation_name(node),
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
        checksum=_checksum(node),
        has_contract=_has_contract(node),
        test_count=test_count,
        materialized=_text(config.get("materialized")) if isinstance(config := node.get("config"), Mapping) else None,
    )


def _checksum(node: Mapping[str, Any]) -> str | None:
    """Return dbt's checksum of the model's file, or None when the manifest records none."""
    checksum = node.get("checksum")
    return _text(checksum.get("checksum")) if isinstance(checksum, dict) else None


def _has_contract(node: Mapping[str, Any]) -> bool:
    """Report whether the model enforces a contract, read from the node or its config."""
    config = node.get("config")
    places = (node.get("contract"), config.get("contract") if isinstance(config, dict) else None)
    return any(isinstance(contract, dict) and contract.get("enforced") is True for contract in places)


def _test_counts(nodes: Mapping[str, Any]) -> dict[str, int]:
    """Count the test nodes attached to each model, by the model's unique id."""
    counts: dict[str, int] = {}
    for node in nodes.values():
        if not isinstance(node, dict) or node.get("resource_type") != "test":
            continue
        attached = node.get("attached_node")
        depends_on = node.get("depends_on")
        targets = (
            {attached}
            if isinstance(attached, str)
            else {item for item in (depends_on.get("nodes") or []) if isinstance(item, str)}
            if isinstance(depends_on, dict)
            else set()
        )
        for target in targets:
            counts[target] = counts.get(target, 0) + 1
    return counts


def schema_version_number(value: object) -> int | None:
    """Return the version a manifest's `dbt_schema_version` URL names, or None when it names none."""
    match = _SCHEMA_URL.fullmatch(value) if isinstance(value, str) else None
    return int(match.group(1)) if match else None


def check_schema_version(metadata: Mapping[str, Any], *, allow_unsupported: bool = False) -> None:
    """Refuse a manifest whose schema version SST does not read, unless told to accept the risk.

    Raises:
        ProjectError: As the diagnostics say, unless `allow_unsupported`.

    Diagnostics:
        SST-DBT018: `dbt_schema_version` is absent or names no version; raised.
        SST-DBT017: it names a version outside `SUPPORTED_SCHEMA_VERSIONS`; raised.
    """
    if allow_unsupported:
        return
    value = metadata.get("dbt_schema_version")
    number = schema_version_number(value)
    if number is None:
        diagnostic = D("SST-DBT018", found="absent" if value in (None, "") else repr(value))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    if number not in SUPPORTED_SCHEMA_VERSIONS:
        supported = ", ".join(f"v{version}" for version in sorted(SUPPORTED_SCHEMA_VERSIONS))
        diagnostic = D("SST-DBT017", found=str(value), expected=supported)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def _disabled_models(root: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the names of the models dbt moved to the `disabled` map, sorted."""
    disabled = root.get("disabled")
    names = {
        str(node.get("name"))
        for entries in (disabled.values() if isinstance(disabled, dict) else ())
        for node in (entries if isinstance(entries, list) else ())
        if isinstance(node, dict) and node.get("resource_type") == "model" and node.get("name")
    }
    return tuple(sorted(names))


def _sources(root: Mapping[str, Any]) -> tuple[tuple[DbtSource, ...], tuple[Diagnostic, ...]]:
    """Read the declared sources, in sorted unique-id order, and each `source.table` pair that repeats.

    Diagnostics:
        SST-DBT012: a `source.table` pair is declared more than once, once per pair.
    """
    raw = root.get("sources")
    sources = tuple(
        DbtSource(
            unique_id=str(unique_id),
            source_name=str(node.get("source_name") or ""),
            name=str(node.get("name") or ""),
            relation_name=_text(node.get("relation_name")),
        )
        for unique_id, node in sorted((raw if isinstance(raw, dict) else {}).items())
        if isinstance(node, dict) and node.get("source_name") and node.get("name")
    )
    counts: dict[str, int] = {}
    for source in sources:
        pair = f"{source.source_name}.{source.name}".casefold()
        counts[pair] = counts.get(pair, 0) + 1
    return sources, tuple(D("SST-DBT012", value=pair) for pair, count in sorted(counts.items()) if count > 1)


def catalog_from_document(document: object, *, allow_unsupported_schema: bool = False) -> DbtCatalog:
    """Project a decoded manifest document into immutable domain values.

    Nodes are read in sorted unique-id order, and the catalog keeps their models in that order.
    A model SST cannot use is left out with a diagnostic in `DbtCatalog.diagnostics`.

    Args:
        allow_unsupported_schema: Read a manifest whose schema version is unsupported or unreadable.

    Args:
        allow_unsupported_schema: Read a manifest of another schema version for this one run,
            as `--allow-unsupported-manifest-schema` asks, instead of refusing it.

    Raises:
        ProjectError: The schema version is refused, or a part SST reads has the wrong shape.

    Diagnostics:
        SST-DBT018, SST-DBT017: as `check_schema_version` raises them.
        SST-DBT013, SST-DBT014: a model has no name, or no database or schema; it is skipped.
        SST-DBT012: a source pair is declared more than once.
    """
    root = _mapping(document, path="root")
    metadata = _mapping(root.get("metadata"), path="metadata")
    check_schema_version(metadata, allow_unsupported=allow_unsupported_schema)
    nodes = _mapping(root.get("nodes"), path="nodes")
    key_tests = _key_test_columns(nodes)
    test_counts = _test_counts(nodes)
    models: list[DbtModel] = []
    relationless: list[str] = []
    unreadable: dict[str, str] = {}
    diagnostics: list[Diagnostic] = []
    for unique_id, raw_node in sorted(nodes.items()):
        model = _model(unique_id, raw_node, key_tests.get(str(unique_id), frozenset()), test_counts.get(unique_id, 0))
        if isinstance(model, DbtModel):
            models.append(model)
        elif isinstance(model, Diagnostic):
            diagnostics.append(model)
        elif model is not None:
            relationless.append(model[0])
            unreadable[model[0].casefold()] = model[1]
    unreadable.update({name.casefold(): "disabled" for name in _disabled_models(root)})
    sources, duplicate_sources = _sources(root)
    return DbtCatalog(
        schema_version=str(metadata.get("dbt_schema_version") or ""),
        dbt_version=_text(metadata.get("dbt_version")),
        project_name=_text(metadata.get("project_name")),
        models=tuple(models),
        relationless_models=tuple(relationless),
        unreadable_models=MappingProxyType(unreadable),
        sources=sources,
        diagnostics=(*diagnostics, *duplicate_sources),
    )


def load_manifest_catalog(path: Path, *, allow_unsupported_schema: bool = False) -> DbtCatalog:
    """Read and decode one dbt manifest without consulting dbt model YAML.

    Args:
        allow_unsupported_schema: As `catalog_from_document` takes it.

    Raises:
        ProjectError: the manifest is absent (SST-PRT006), cannot be read (SST-PRT009), is not
            JSON, or is refused as `catalog_from_document` says.

    Diagnostics:
        SST-PRT006: no manifest exists at the path; raised.
        SST-PRT009: the manifest exists and cannot be read; raised.
    """
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        diagnostic = D("SST-PRT006", path=str(path))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
    except OSError as exc:
        diagnostic = D("SST-PRT009", path=str(path), detail=str(exc))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
    except json.JSONDecodeError as exc:
        raise ProjectError(f"dbt manifest {path} is not valid JSON: {exc}") from exc
    return catalog_from_document(document, allow_unsupported_schema=allow_unsupported_schema)
