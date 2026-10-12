"""The dbt manifest contract: what SST reads is present in every manifest a supported dbt writes.

`tests/fixtures/dbt_manifests/v<schema>/dbt-<release>/manifest.json` holds one manifest of the
reference project per manifest schema version SST accepts and per dbt release it is checked
with, written by a real `dbt parse` and scrubbed by `tests.helpers.manifest_capture`, which says
how to capture one. Every fixture, and every manifest given with `--dbt-manifest` (the CI job
that runs dbt passes the one it just wrote), is checked for:

- a schema version SST accepts, filed under that version and its dbt release;
- each path of the read-surface table in docs/design/dbt-compatibility.md, with the JSON type SST
  expects, on the reference project's models, tests and sources;
- model checksums SST's staleness check recognises as fresh against the code dbt hashed;
- the semantic views the reference project renders, which must be the goldens' views.

The committed fixtures must also read as one catalog, apart from what names the release.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable, Iterator, Mapping
from functools import cache
from pathlib import Path
from typing import Any

import pytest

from snowflake_semantic_tools.adapters.dbt.manifest import (
    SUPPORTED_SCHEMA_VERSIONS,
    catalog_from_document,
    load_manifest_catalog,
    schema_version_number,
)
from snowflake_semantic_tools.adapters.dbt.project import stale_models
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.render.semantic_view import render
from tests.helpers.manifest_capture import FIXTURES, fixture_path
from tests.helpers.projects import load_views
from tests.helpers.reference_project import DBT_MANIFEST, FIXTURE, project_copy

COMMITTED = tuple(sorted(FIXTURES.glob("v*/dbt-*/manifest.json")))
PROJECT = "sst_reference_impl"

_Kind = type | tuple[type, ...]
_TEXT: _Kind = str
_OPTIONAL_TEXT: _Kind = (str, type(None))
# The read-surface table, one path per row and part, relative to the root, a node or a column.
ROOT_PATHS: dict[str, _Kind] = {
    "metadata.dbt_schema_version": _TEXT,
    "metadata.dbt_version": _TEXT,
    "metadata.project_name": _TEXT,
    "nodes": dict,
    "sources": dict,
    "disabled": dict,
}
MODEL_PATHS: dict[str, _Kind] = {
    "resource_type": _TEXT,
    "name": _TEXT,
    "relation_name": _TEXT,
    "database": _TEXT,
    "schema": _TEXT,
    "config.materialized": _TEXT,
    "meta": dict,
    "config.meta": dict,
    "columns": dict,
    "description": _TEXT,
    "checksum.checksum": _TEXT,
    "contract.enforced": bool,
    "config.contract.enforced": bool,
    "original_file_path": _TEXT,
    "patch_path": _TEXT,
    "package_name": _TEXT,
}
COLUMN_PATHS: dict[str, _Kind] = {
    "name": _TEXT,
    "description": _TEXT,
    "data_type": _OPTIONAL_TEXT,
    "meta": dict,
    "config.meta": dict,
}
TEST_PATHS: dict[str, _Kind] = {
    "resource_type": _TEXT,
    "attached_node": _OPTIONAL_TEXT,
    "depends_on.nodes": list,
    "test_metadata.name": _TEXT,
    "test_metadata.kwargs": dict,
    "column_name": _OPTIONAL_TEXT,
}
SOURCE_PATHS: dict[str, _Kind] = {"source_name": _TEXT, "name": _TEXT, "relation_name": _TEXT}
_ABSENT = object()


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "manifest_path" in metafunc.fixturenames:
        given = tuple(Path(path).resolve() for path in metafunc.config.getoption("--dbt-manifest"))
        paths = (*COMMITTED, *given)
        metafunc.parametrize("manifest_path", paths, ids=[_label(path) for path in paths])


def _label(path: Path) -> str:
    return f"{path.parent.parent.name}-{path.parent.name}" if path in COMMITTED else f"given:{path}"


@cache
def _document(path: Path) -> Mapping[str, Any]:
    document: Mapping[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return document


def _at(value: object, dotted: str) -> object:
    for key in dotted.split("."):
        if not isinstance(value, dict) or key not in value:
            return _ABSENT
        value = value[key]
    return value


def _check(value: object, where: str, paths: Mapping[str, _Kind]) -> Iterator[str]:
    for dotted, kind in paths.items():
        found = _at(value, dotted)
        if found is _ABSENT:
            yield f"{where}.{dotted}: absent"
        elif not isinstance(found, kind) or (kind is not bool and isinstance(found, bool)):
            expected = " or ".join(item.__name__ for item in (kind if isinstance(kind, tuple) else (kind,)))
            yield f"{where}.{dotted}: {type(found).__name__}, not {expected}"


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def read_surface_gaps(document: Mapping[str, Any]) -> list[str]:
    """Each read-surface path absent from the reference project's nodes, or of another type."""
    gaps = list(_check(document, "", ROOT_PATHS))
    nodes = _mapping(document.get("nodes"))
    models = {key: node for key, node in nodes.items() if key.startswith(f"model.{PROJECT}.")}
    tests = {key: node for key, node in nodes.items() if key.startswith(f"test.{PROJECT}.")}
    sources = _mapping(document.get("sources"))
    for kind, found in (("models", models), ("tests", tests), ("sources", sources)):
        if not found:
            gaps.append(f"nodes: no reference-project {kind}")
    for key, node in sorted(models.items()):
        gaps.extend(_check(node, f"nodes.{key}", MODEL_PATHS))
        columns = _mapping(node.get("columns"))
        for name, column in sorted(columns.items()):
            gaps.extend(_check(column, f"nodes.{key}.columns.{name}", COLUMN_PATHS))
    for key, node in sorted(tests.items()):
        gaps.extend(_check(node, f"nodes.{key}", TEST_PATHS))
    for key, node in sorted(sources.items()):
        gaps.extend(_check(node, f"sources.{key}", SOURCE_PATHS))
    # `meta.sst`, else `config.meta.sst`: one of them carries SST's keys wherever a model has any.
    if not any(
        isinstance(_at(node, "meta.sst"), dict) or isinstance(_at(node, "config.meta.sst"), dict)
        for node in models.values()
    ):
        gaps.append("nodes.<model>.meta.sst, nodes.<model>.config.meta.sst: on no model")
    return [gap.lstrip(".") for gap in gaps]


def _hashed_code_project(root: Path, document: Mapping[str, Any]) -> Path:
    """The reference project with each model's file replaced by the code the manifest says dbt read."""
    project = project_copy(root)
    for node in document["nodes"].values():
        if node.get("resource_type") == "model" and node.get("package_name") == PROJECT:
            (project / node["original_file_path"]).write_text(node["raw_code"], encoding="utf-8")
    return project


def _without_release(catalog: DbtCatalog) -> DbtCatalog:
    """The catalog without what names the dbt release: its version, and how it hashed each model."""
    models = tuple(dataclasses.replace(model, checksum=None) for model in catalog.models)
    return dataclasses.replace(catalog, dbt_version=None, models=models)


def test_every_supported_schema_version_has_a_fixture_and_every_fixture_a_supported_version() -> None:
    filed = {int(path.parent.parent.name.removeprefix("v")) for path in COMMITTED}
    assert filed == set(SUPPORTED_SCHEMA_VERSIONS), (
        f"fixtures under {FIXTURES} cover schema versions {sorted(filed)}, and SST accepts "
        f"{sorted(SUPPORTED_SCHEMA_VERSIONS)}; capture one as tests/helpers/manifest_capture.py says"
    )


def test_the_manifest_is_of_a_schema_sst_accepts(manifest_path: Path) -> None:
    document = _document(manifest_path)
    assert schema_version_number(document["metadata"]["dbt_schema_version"]) in SUPPORTED_SCHEMA_VERSIONS
    catalog = load_manifest_catalog(manifest_path)
    assert catalog.diagnostics == ()
    if manifest_path in COMMITTED:
        assert fixture_path(document) == manifest_path


def test_the_manifest_has_every_path_sst_reads(manifest_path: Path) -> None:
    assert read_surface_gaps(_document(manifest_path)) == []


def test_sst_recognises_each_model_checksum_the_release_wrote(manifest_path: Path, tmp_path: Path) -> None:
    document = _document(manifest_path)
    project = _hashed_code_project(tmp_path, document)
    catalog = catalog_from_document(document)
    assert stale_models(project, catalog, ("models",)) == ()


@pytest.mark.parametrize("committed", COMMITTED, ids=lambda path: path.parent.name)
def test_a_whitespace_only_edit_is_stale_only_for_a_release_that_hashes_whitespace(
    committed: Path, tmp_path: Path
) -> None:
    # dbt 1.12 collapses whitespace before hashing, and 1.11 only strips the ends, so doubling a
    # space inside a model is fresh to the first and an edit to the second.
    document = _document(committed)
    project = _hashed_code_project(tmp_path, document)
    catalog = catalog_from_document(document)
    edited = next(model for model in catalog.models if model.package_name == PROJECT and model.original_file_path)
    path = project / str(edited.original_file_path)
    path.write_text(path.read_text(encoding="utf-8").replace(" ", "  ", 1), encoding="utf-8")
    stale = [item.context["value"] for item in stale_models(project, catalog, ("models",))]
    hashes_whitespace = str(document["metadata"]["dbt_version"]).startswith("1.11.")
    assert stale == ([edited.original_file_path] if hashes_whitespace else [])


@cache
def _golden_views() -> dict[str, str]:
    """The reference project's views as the DDL goldens pin them: rendered from the reduced manifest."""
    return {view.fqn: str(render(view)) for view in load_views(FIXTURE, manifest_path=DBT_MANIFEST)}


def test_the_reference_project_renders_the_goldens_views_from_the_manifest(manifest_path: Path, tmp_path: Path) -> None:
    project = _hashed_code_project(tmp_path, _document(manifest_path))
    rendered = {view.fqn: str(render(view)) for view in load_views(project, manifest_path=manifest_path)}
    assert rendered == _golden_views()


def test_every_committed_fixture_reads_as_one_catalog_apart_from_its_release() -> None:
    first, *others = (_without_release(load_manifest_catalog(path)) for path in COMMITTED)
    assert others, "the contract compares releases; it needs a fixture for more than one"
    for path, catalog in zip(COMMITTED[1:], others, strict=True):
        assert catalog == first, f"{_label(path)} reads differently from {_label(COMMITTED[0])}"


@pytest.mark.parametrize(
    ("edit", "named"),
    [
        (
            lambda document: document["nodes"]["model.sst_reference_impl.orders"].pop("relation_name"),
            "relation_name: absent",
        ),
        (
            lambda document: document["nodes"]["model.sst_reference_impl.orders"]["checksum"].pop("checksum"),
            "checksum.checksum: absent",
        ),
        (
            lambda document: document["sources"]["source.sst_reference_impl.jaffle_service.orders_raw"].update(name=1),
            "name: int, not str",
        ),
        (lambda document: document["metadata"].pop("project_name"), "metadata.project_name: absent"),
        (
            lambda document: document["nodes"]["model.sst_reference_impl.orders"]["contract"].update(enforced="yes"),
            "contract.enforced: str, not bool",
        ),
    ],
)
def test_a_missing_or_retyped_path_is_named(edit: Callable[[dict[str, Any]], object], named: str) -> None:
    document = json.loads(COMMITTED[0].read_text(encoding="utf-8"))
    edit(document)
    gaps = read_surface_gaps(document)
    assert len(gaps) == 1 and gaps[0].endswith(named), gaps
