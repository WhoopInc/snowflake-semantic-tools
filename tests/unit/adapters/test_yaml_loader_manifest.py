"""The YAML adapter must consume dbt metadata from the manifest, not dbt YAML."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.manifest import SUPPORTED_SCHEMA
from snowflake_semantic_tools.adapters.project import ProjectError
from snowflake_semantic_tools.adapters.yaml.documents import discover_yaml, load_documents
from snowflake_semantic_tools.adapters.yaml.loader import (
    _folder_route_diagnostics,
    _legacy_reference_diagnostics,
    _metric_parse_diagnostics,
    _parse_yaml_bytes,
    _relationship_parse_diagnostics,
    _resolve_refs,
    _verified_query_diagnostics,
    load_semantic_views,
    load_semantic_views_result,
)


def write_project(root: Path) -> Path:
    (root / "semantic_models" / "semantic_views").mkdir(parents=True)
    (root / "semantic_models" / "metrics").mkdir(parents=True)
    (root / "models").mkdir()
    (root / "dbt_project.yml").write_text(
        "name: fixture\nprofile: fixture\nmodel-paths: [models]\ntarget-path: target\n",
        encoding="utf-8",
    )
    (root / "profiles.yml").write_text(
        "fixture:\n  target: dev\n  outputs:\n    dev:\n      type: snowflake\n      database: DB\n      schema: SCH\n",
        encoding="utf-8",
    )
    (root / "sst_config.yml").write_text(
        "project:\n  semantic_models_dir: semantic_models\n",
        encoding="utf-8",
    )
    (root / "semantic_models" / "semantic_views" / "views.yml").write_text(
        "semantic_views:\n  - name: catalog\n    description: Product catalog.\n"
        "    tables:\n      - \"{{ ref('products') }}\"\n",
        encoding="utf-8",
    )
    # Deliberately wrong metadata. If this file is re-read, the assertions below fail.
    (root / "models" / "products.yml").write_text(
        "version: 2\nmodels:\n  - name: products\n    config:\n      meta:\n        sst:\n          primary_key: [wrong_id]\n",
        encoding="utf-8",
    )
    manifest = {
        "metadata": {
            "dbt_schema_version": SUPPORTED_SCHEMA,
            "dbt_version": "1.11.2",
            "project_name": "fixture",
        },
        "nodes": {
            "model.fixture.products": {
                "resource_type": "model",
                "name": "products",
                "description": "Products available to semantic views.",
                "relation_name": "physical_db.physical_schema.product_catalog",
                "config": {"meta": {"sst": {"primary_key": ["product_id"]}}},
                "columns": {
                    "product_id": {
                        "name": "product_id",
                        "description": "Manifest description.",
                        "data_type": "VARCHAR",
                        "meta": {"sst": {"column_type": "dimension"}},
                    }
                },
            }
        },
    }
    path = root / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def test_uses_relation_grain_and_columns_from_manifest(tmp_path: Path) -> None:
    manifest = write_project(tmp_path)
    view = load_semantic_views(tmp_path, manifest_path=manifest, invoke_dbt=False)[0]
    assert view.fqn == "DB.SCH.CATALOG"
    assert view.tables[0].fqn == "PHYSICAL_DB.PHYSICAL_SCHEMA.PRODUCT_CATALOG"
    assert view.tables[0].primary_key == ("PRODUCT_ID",)
    assert view.dimensions[0].comment == "Manifest description."


def test_malformed_expression_carries_a_structured_load_diagnostic() -> None:
    with pytest.raises(ProjectError) as exc_info:
        _resolve_refs("{{ ref('products')", {"products": "PRODUCTS"})
    diagnostic = exc_info.value.diagnostics[0]
    assert diagnostic.code == "SST-LOD004"
    assert diagnostic.context["line"] == 1
    assert "unterminated template expression" in diagnostic.message


def test_unknown_view_model_isolated_from_healthy_views(tmp_path: Path) -> None:
    manifest = write_project(tmp_path)
    views_path = tmp_path / "semantic_models" / "semantic_views" / "views.yml"
    views_path.write_text(
        "semantic_views:\n"
        "  - name: healthy\n"
        "    description: Healthy view.\n"
        "    tables: [\"{{ ref('products') }}\"]\n"
        "  - name: poisoned\n"
        "    description: Poisoned view.\n"
        "    tables: [\"{{ ref('missing') }}\"]\n",
        encoding="utf-8",
    )
    project = load_semantic_views_result(tmp_path, manifest_path=manifest, invoke_dbt=False)
    assert [view.fqn for view in project.views] == ["DB.SCH.HEALTHY"]
    assert [diagnostic.code for diagnostic in project.diagnostics] == ["SST-REF001"]
    assert project.diagnostics[0].subject == "semantic_view:poisoned"


def test_duplicate_view_names_are_diagnosed_and_not_manifest_candidates(tmp_path: Path) -> None:
    manifest = write_project(tmp_path)
    views_path = tmp_path / "semantic_models" / "semantic_views" / "views.yml"
    views_path.write_text(
        "semantic_views:\n"
        "  - name: duplicate\n    description: Duplicate one.\n    tables: [\"{{ ref('products') }}\"]\n"
        "  - name: DUPLICATE\n    description: Duplicate two.\n    tables: [\"{{ ref('products') }}\"]\n",
        encoding="utf-8",
    )
    project = load_semantic_views_result(tmp_path, manifest_path=manifest, invoke_dbt=False)
    assert project.views == ()
    assert [diagnostic.code for diagnostic in project.diagnostics] == ["SST-VAL001"]


def test_vq_exclusivity_and_legacy_globals_are_structured_diagnostics(tmp_path: Path) -> None:
    write_project(tmp_path)
    root = tmp_path / "semantic_models"
    verified = root / "verified_queries"
    verified.mkdir()
    (verified / "bad.yml").write_text(
        "snowflake_verified_queries:\n"
        "  - name: both\n"
        "    question: q\n"
        "    tables: [orders]\n"
        "    sql: SELECT 1\n"
        "    sql_file: q.sql\n",
        encoding="utf-8",
    )
    (root / "metrics" / "legacy.yml").write_text(
        "snowflake_metrics:\n"
        "  - name: legacy\n"
        "    tables: [\"{{ table('orders') }}\"]\n"
        "    expr: \"SUM({{ column('orders', 'total') }})\"\n",
        encoding="utf-8",
    )
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), _parse_yaml_bytes)
    assert [diagnostic.code for diagnostic in _verified_query_diagnostics(documents, tmp_path, "semantic_models")] == [
        "SST-VAL412"
    ]
    assert [diagnostic.code for diagnostic in _legacy_reference_diagnostics(documents)] == [
        "SST-REF034",
        "SST-REF035",
    ]


def test_vq_sources_and_relationship_conditions_fail_with_registered_codes(tmp_path: Path) -> None:
    write_project(tmp_path)
    root = tmp_path / "semantic_models"
    verified = root / "verified_queries"
    verified.mkdir()
    (verified / "invalid.yml").write_text(
        "snowflake_verified_queries:\n"
        "  - name: absent_source\n"
        "    question: q1\n"
        "    tables: [products]\n"
        "  - name: missing_file\n"
        "    question: q2\n"
        "    tables: [products]\n"
        "    sql_file: missing.sql\n"
        "  - name: empty_file\n"
        "    question: q3\n"
        "    tables: [products]\n"
        "    sql_file: empty.sql\n",
        encoding="utf-8",
    )
    (verified / "empty.sql").write_text("", encoding="utf-8")
    relationships = root / "relationships"
    relationships.mkdir()
    (relationships / "invalid.yml").write_text(
        "snowflake_relationships:\n"
        "  - name: no_conditions\n"
        "    left_table: products\n"
        "    right_table: products\n"
        "    relationship_conditions: []\n",
        encoding="utf-8",
    )
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), _parse_yaml_bytes)

    assert [diagnostic.code for diagnostic in _verified_query_diagnostics(documents, tmp_path, "semantic_models")] == [
        "SST-VAL412",
        "SST-LOD018",
        "SST-LOD019",
    ]
    assert [
        diagnostic.code for diagnostic in _relationship_parse_diagnostics(documents, tmp_path, "semantic_models")
    ] == ["SST-VAL201"]


def test_missing_folder_route_is_a_config_diagnostic(tmp_path: Path) -> None:
    views_dir = tmp_path / "semantic_models" / "semantic_views"
    views_dir.mkdir(parents=True)
    config = {"semantic_views": {"finance": {"+schema": "FINANCE"}}}
    diagnostics = _folder_route_diagnostics(config, views_dir)
    assert [diagnostic.code for diagnostic in diagnostics] == ["SST-CFG041"]


def test_metric_parse_diagnostics_preserve_missing_empty_and_wrong_types(tmp_path: Path) -> None:
    write_project(tmp_path)
    metrics = tmp_path / "semantic_models" / "metrics" / "invalid.yml"
    metrics.write_text(
        "snowflake_metrics:\n"
        "  - name: missing_expr\n"
        "    tables: [products]\n"
        "  - name: wrong_expr\n"
        "    expr: [COUNT, 1]\n"
        "    tables: products\n"
        "    using_relationships: rel\n"
        "    access_modifier: hidden\n"
        "    visibility: private\n"
        "    synonyms: item\n",
        encoding="utf-8",
    )
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), _parse_yaml_bytes)
    diagnostics = _metric_parse_diagnostics(documents, tmp_path, "semantic_models")
    assert [diagnostic.code for diagnostic in diagnostics] == [
        "SST-PRS002",
        "SST-PRS113",
        "SST-PRS003",
        "SST-PRS003",
        "SST-PRS013",
        "SST-VAL122",
        "SST-PRS029",
    ]
