"""The typed manifest decode reads every manifest as the plain decode does, or steps aside.

`manifest_schema` keeps only the keys SST reads, so it is right exactly when the catalog built
from what it keeps equals the one built from the whole document. These compare the two on every
manifest at hand, and check that a document the structs would read differently is left to the
plain decode, so its diagnostics are the ones it reports today.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.manifest import catalog_from_document, load_manifest_catalog
from snowflake_semantic_tools.adapters.dbt.manifest_schema import decode_manifest
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.json_files import MAX_JSON_DEPTH, JsonFileError, parse_json, read_bounded
from tests.helpers.reference_project import DBT_MANIFEST, REPO_ROOT

# Other projects' manifests, compared when they are on this machine: the reference manifest is
# small, and these are the large, real ones the typed decode is for.
_LOCAL = (
    REPO_ROOT.parent / "analytics-dbt" / "target" / "manifest.json",
    REPO_ROOT.parent / "sst-jaffle-shop" / "target" / "manifest.json",
)
_MANIFESTS = [pytest.param(DBT_MANIFEST, id="reference")] + [
    pytest.param(path, id=path.parents[1].name, marks=pytest.mark.skipif(not path.is_file(), reason="not here"))
    for path in _LOCAL
]
_V12 = "https://schemas.getdbt.com/dbt/manifest/v12.json"


def _both(raw: bytes) -> tuple[object, object]:
    """The catalog, or the codes refusing it, built from the typed decode and from the plain one."""

    def build(document: object) -> object:
        try:
            return catalog_from_document(document, allow_unsupported_schema=True)
        except ProjectError as exc:
            return [(item.code, item.message) for item in exc.diagnostics]

    kept = decode_manifest(raw)
    assert kept is not None, "the typed decode stepped aside for a manifest it should read"
    return build(kept), build(parse_json(raw))


@pytest.mark.parametrize("path", _MANIFESTS)
def test_the_typed_decode_builds_the_catalog_the_plain_decode_builds(path: Path) -> None:
    fast, plain = _both(read_bounded(path))
    assert fast == plain


def _manifest(**parts: object) -> bytes:
    return json.dumps({"metadata": {"dbt_schema_version": _V12}, "nodes": {}, **parts}).encode()


_MODEL = {
    "resource_type": "model",
    "name": "orders",
    "relation_name": '"DB"."S"."ORDERS"',
    "database": "DB",
    "schema": "S",
    "compiled_code": "select 1",
    "columns": {"id": {"name": "id", "data_type": "number", "meta": {"sst": {"synonyms": ["key"], "x": 1}}}},
}


@pytest.mark.parametrize(
    "raw",
    [
        # Values SST reads in every shape it tells apart: missing, null, wrong-typed, int or float.
        pytest.param(_manifest(nodes={"model.p.orders": _MODEL}), id="a-model"),
        pytest.param(_manifest(nodes={"model.p.orders": {**_MODEL, "database": None}}), id="null-database"),
        pytest.param(
            _manifest(nodes={"model.p.orders": {k: v for k, v in _MODEL.items() if k != "schema"}}),
            id="missing-schema",
        ),
        pytest.param(_manifest(nodes={"model.p.orders": {**_MODEL, "meta": "text"}}), id="meta-not-a-mapping"),
        pytest.param(
            _manifest(nodes={"model.p.orders": {**_MODEL, "meta": {"sst": {"synonyms": [1, 1.5, None, {}]}}}}),
            id="synonyms-of-every-type",
        ),
        pytest.param(
            _manifest(nodes={"model.p.orders": {**_MODEL, "meta": {"sst": {"distinct_range": None}}}}),
            id="null-distinct-range",
        ),
        pytest.param(_manifest(nodes={"model.p.orders": {**_MODEL, "columns": None}}), id="null-columns"),
        pytest.param(_manifest(nodes={"model.p.orders": {**_MODEL, "config": None}}), id="null-config"),
        pytest.param(
            _manifest(nodes={"model.p.orders": {**_MODEL, "config": {"meta": {"sst": "text"}}}}),
            id="config-meta-sst-not-a-mapping",
        ),
        pytest.param(_manifest(nodes={}, metadata={"dbt_schema_version": 12, "dbt_version": None}), id="odd-metadata"),
        pytest.param(_manifest(sources={"source.p.s.t": {"source_name": "s", "name": "t"}}), id="a-source"),
        pytest.param(_manifest(disabled={"model.p.old": [{"name": "old", "resource_type": "model"}]}), id="disabled"),
        pytest.param(json.dumps({"metadata": {"dbt_schema_version": _V12}}).encode(), id="no-nodes"),
    ],
)
def test_the_typed_decode_keeps_every_distinction_the_catalog_draws(raw: bytes) -> None:
    fast, plain = _both(raw)
    assert fast == plain


@pytest.mark.parametrize(
    "raw",
    [
        # A part the structs type, in another shape: the plain decode reports it with its path.
        pytest.param(_manifest(nodes={"model.p.orders": "text"}), id="node-not-a-mapping"),
        pytest.param(_manifest(nodes=[]), id="nodes-a-list"),
        pytest.param(_manifest(nodes={"model.p.orders": {**_MODEL, "columns": ["id"]}}), id="columns-a-list"),
        pytest.param(_manifest(nodes={"model.p.orders": {**_MODEL, "columns": {"id": 3}}}), id="column-a-number"),
        pytest.param(_manifest(nodes={"model.p.orders": {**_MODEL, "config": []}}), id="config-a-list"),
        pytest.param(_manifest(metadata=[]), id="metadata-a-list"),
        pytest.param(b"[]", id="root-a-list"),
        pytest.param(b'{"metadata": {"dbt_schema_version": NaN}, "nodes": {}}', id="nan-json-accepts"),
        pytest.param(b'{"nodes": {,}}', id="not-json"),
    ],
)
def test_a_document_the_structs_would_read_differently_is_left_to_the_plain_decode(raw: bytes) -> None:
    assert decode_manifest(raw) is None


def test_a_misshapen_manifest_reports_through_the_loader_what_the_plain_read_reports(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_bytes(_manifest(nodes={"model.p.orders": {**_MODEL, "columns": {"id": 3}}}))
    with pytest.raises(ProjectError) as raised:
        load_manifest_catalog(path)
    with pytest.raises(ProjectError) as plain:
        catalog_from_document(parse_json(path.read_bytes()))
    assert [(item.code, item.message) for item in raised.value.diagnostics] == [
        (item.code, item.message) for item in plain.value.diagnostics
    ]
    assert raised.value.diagnostics[0].code == "SST-DBT033"


@pytest.mark.parametrize("where", ["meta", "columns.id.meta", "config.meta", "depends_on"])
@pytest.mark.parametrize("extra", [0, 1])
def test_the_typed_decode_refuses_exactly_the_nesting_the_plain_decode_refuses(where: str, extra: int) -> None:
    # A value at each kept level, as deep as the bound allows and one level more.
    shell = {"metadata": {"dbt_schema_version": _V12}, "nodes": {"model.p.orders": copy.deepcopy(_MODEL)}}
    holder: dict[str, object] = shell["nodes"]["model.p.orders"]  # type: ignore[index]
    *parents, leaf = where.split(".")
    for key in parents:
        holder = holder.setdefault(key, {})  # type: ignore[assignment]
    marker = "__deep__"
    holder[leaf] = marker
    enclosing = 3 + len(parents)  # the root, nodes, the node, then each parent
    depth = MAX_JSON_DEPTH - enclosing + extra
    raw = json.dumps(shell).replace(f'"{marker}"', "[" * depth + "]" * depth).encode()
    try:
        parse_json(raw)
        plain_refuses = False
    except JsonFileError:
        plain_refuses = True
    assert plain_refuses is bool(extra)
    assert (decode_manifest(raw) is None) is plain_refuses
