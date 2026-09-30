"""Contract tests for the narrow dbt manifest projection."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.manifest import (
    SUPPORTED_SCHEMA,
    catalog_from_document,
    load_manifest_catalog,
)
from snowflake_semantic_tools.adapters.project import ProjectError


def manifest_document() -> dict[str, object]:
    return {
        "metadata": {
            "dbt_schema_version": SUPPORTED_SCHEMA,
            "dbt_version": "1.11.2",
            "project_name": "fixture",
        },
        "nodes": {
            "seed.fixture.products": {
                "resource_type": "seed",
                "name": "products_raw",
            },
            "model.fixture.products": {
                "resource_type": "model",
                "name": "products",
                "description": "Products exposed to semantic views.",
                "relation_name": "db.sch.product_catalog",
                "original_file_path": "models/products.sql",
                "patch_path": "fixture://models/products.yml",
                "config": {
                    "meta": {
                        "sst": {
                            "primary_key": ["product_id"],
                            "unique_keys": [["product_name"]],
                        }
                    }
                },
                "columns": {
                    "product_id": {
                        "name": "product_id",
                        "description": "Product key.",
                        "data_type": "VARCHAR",
                        "meta": {
                            "sst": {
                                "column_type": "dimension",
                                "synonyms": ["item key"],
                                "sample_values": [1, 2],
                                "is_enum": False,
                            }
                        },
                    }
                },
            },
        },
    }


def test_projects_resolved_model_metadata_and_relation() -> None:
    catalog = catalog_from_document(manifest_document())
    assert catalog.schema_version == SUPPORTED_SCHEMA
    assert len(catalog.models) == 1
    model = catalog.model("PRODUCTS")
    assert model is not None
    assert model.relation_name == "DB.SCH.PRODUCT_CATALOG"
    assert model.primary_key == ("product_id",)
    assert model.unique_keys == (("product_name",),)
    assert model.description == "Products exposed to semantic views."
    assert model.column("PRODUCT_ID") is not None
    assert model.column("product_id").sample_values == ("1", "2")  # type: ignore[union-attr]


def test_column_data_type_falls_back_to_sst_metadata() -> None:
    document = manifest_document()
    nodes = document["nodes"]
    assert isinstance(nodes, dict)
    model = nodes["model.fixture.products"]
    assert isinstance(model, dict)
    columns = model["columns"]
    assert isinstance(columns, dict)
    column = columns["product_id"]
    assert isinstance(column, dict)
    column["data_type"] = None
    meta = column["meta"]
    assert isinstance(meta, dict)
    sst = meta["sst"]
    assert isinstance(sst, dict)
    sst["data_type"] = "NUMBER(38,0)"
    projected = catalog_from_document(document).model("products")
    assert projected is not None
    assert projected.column("product_id").data_type == "NUMBER(38,0)"  # type: ignore[union-attr]


def test_rejects_an_unsupported_manifest_schema() -> None:
    document = manifest_document()
    metadata = document["metadata"]
    assert isinstance(metadata, dict)
    metadata["dbt_schema_version"] = "https://schemas.getdbt.com/dbt/manifest/v13.json"
    with pytest.raises(ProjectError, match="manifest schema") as exc_info:
        catalog_from_document(document)
    assert [diagnostic.code for diagnostic in exc_info.value.diagnostics] == ["SST-PRT007"]


def test_rejects_a_model_without_a_physical_relation() -> None:
    document = manifest_document()
    nodes = document["nodes"]
    assert isinstance(nodes, dict)
    model = nodes["model.fixture.products"]
    assert isinstance(model, dict)
    model["relation_name"] = None
    with pytest.raises(ProjectError, match="relation_name is required"):
        catalog_from_document(document)


def test_loads_a_manifest_from_disk(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest_document()), encoding="utf-8")
    assert load_manifest_catalog(path).model("products") is not None


def test_invalid_json_is_a_project_error(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text("{", encoding="utf-8")
    with pytest.raises(ProjectError, match="not valid JSON"):
        load_manifest_catalog(path)


def test_forbidden_model_location_keys_are_retained_for_validation() -> None:
    document = manifest_document()
    nodes = document["nodes"]
    assert isinstance(nodes, dict)
    model = nodes["model.fixture.products"]
    assert isinstance(model, dict)
    config = model["config"]
    assert isinstance(config, dict)
    meta = config["meta"]
    assert isinstance(meta, dict)
    sst = meta["sst"]
    assert isinstance(sst, dict)
    sst.update({"database": "DB", "schema": "SCH"})
    projected = catalog_from_document(document).model("products")
    assert projected is not None
    assert projected.forbidden_location_keys == ("database", "schema")
    assert projected.unknown_meta_keys == ()


def test_unknown_meta_sst_keys_are_retained_for_validation() -> None:
    document = manifest_document()
    model = document["nodes"]["model.fixture.products"]  # type: ignore[index]
    model["config"]["meta"]["sst"].update({"synonyms": ["catalogue"], "cortex_searchable": True})
    model["columns"]["product_id"]["meta"]["sst"]["privacy_category"] = "none"
    projected = catalog_from_document(document).model("products")
    assert projected is not None
    assert projected.unknown_meta_keys == ("cortex_searchable", "synonyms")
    column = projected.column("product_id")
    assert column is not None and column.unknown_meta_keys == ("privacy_category",)


def test_a_meta_sst_data_type_is_recorded_only_when_it_disagrees_with_dbt() -> None:
    document = manifest_document()
    column = document["nodes"]["model.fixture.products"]["columns"]["product_id"]  # type: ignore[index]
    column["meta"]["sst"]["data_type"] = " varchar "
    projected = catalog_from_document(document).model("products")
    assert projected is not None and projected.columns[0].declared_data_type is None
    column["meta"]["sst"]["data_type"] = "NUMBER"
    projected = catalog_from_document(document).model("products")
    assert projected is not None
    assert (projected.columns[0].data_type, projected.columns[0].declared_data_type) == ("VARCHAR", "NUMBER")


@pytest.mark.parametrize(
    ("value", "expected", "legacy"),
    [
        (["product_id"], ("product_id",), False),
        (None, (), False),
        ("product_id", (), True),
        ("calendar_date, user_id", (), True),
        ("", (), False),
        ("  ", (), False),
    ],
)
def test_primary_key_03_string_forms_are_reported_not_read(
    value: object, expected: tuple[str, ...], legacy: bool
) -> None:
    document = manifest_document()
    sst = document["nodes"]["model.fixture.products"]["config"]["meta"]["sst"]  # type: ignore[index]
    sst["primary_key"] = value
    projected = catalog_from_document(document).model("products")
    assert projected is not None
    assert (projected.primary_key, projected.legacy_key_fields) == (expected, ("primary_key",) if legacy else ())


@pytest.mark.parametrize(
    ("value", "expected", "legacy"),
    [
        ([["a", "b"], ["c"]], (("a", "b"), ("c",)), False),
        ([], (), False),
        (["a", "b"], (), True),
        ([["a", "b"], "c"], (), True),
        ("a, b", (), True),
        ("", (), False),
    ],
)
def test_unique_keys_03_forms_are_reported_not_read(
    value: object, expected: tuple[tuple[str, ...], ...], legacy: bool
) -> None:
    document = manifest_document()
    sst = document["nodes"]["model.fixture.products"]["config"]["meta"]["sst"]  # type: ignore[index]
    sst["unique_keys"] = value
    projected = catalog_from_document(document).model("products")
    assert projected is not None
    assert (projected.unique_keys, projected.legacy_key_fields) == (expected, ("unique_keys",) if legacy else ())


def test_primary_key_of_another_type_is_still_a_project_error() -> None:
    document = manifest_document()
    sst = document["nodes"]["model.fixture.products"]["config"]["meta"]["sst"]  # type: ignore[index]
    sst["primary_key"] = {"column": "product_id"}
    with pytest.raises(ProjectError, match="primary_key must be a list"):
        catalog_from_document(document)


def test_a_relationless_model_without_sst_metadata_is_skipped() -> None:
    document = manifest_document()
    nodes = document["nodes"]
    assert isinstance(nodes, dict)
    nodes["model.fixture.ephemeral"] = {"resource_type": "model", "name": "ephemeral", "relation_name": None}
    catalog = catalog_from_document(document)
    assert catalog.model("ephemeral") is None
    assert catalog.model("products") is not None
