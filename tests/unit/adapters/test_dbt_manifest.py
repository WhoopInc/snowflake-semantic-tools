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
