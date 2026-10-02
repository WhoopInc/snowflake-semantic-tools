"""The YAML adapter must consume dbt metadata from the manifest, not dbt YAML."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.dbt.manifest import SUPPORTED_SCHEMA
from snowflake_semantic_tools.adapters.yaml.documents import discover_yaml, load_documents
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.adapters.yaml.semantic.checks.authored_keys import (
    _authored_key_diagnostics,
    _legacy_reference_diagnostics,
    _member_name_diagnostics,
)
from snowflake_semantic_tools.adapters.yaml.semantic.checks.shape import (
    _metric_parse_diagnostics,
    _verified_query_diagnostics,
)
from snowflake_semantic_tools.adapters.yaml.semantic.defs import _frame
from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _relationship_parse_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.target import _folder_route_diagnostics
from snowflake_semantic_tools.domain.model.diagnostic import ERROR_REGISTRY
from tests.helpers.projects import load_project, load_views


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
        "version: 2\nmodels:\n  - name: products\n    config:\n"
        "      meta:\n        sst:\n          primary_key: [wrong_id]\n",
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
    view = load_views(tmp_path, manifest_path=manifest)[0]
    assert view.fqn == "DB.SCH.CATALOG"
    assert view.tables[0].fqn == "PHYSICAL_DB.PHYSICAL_SCHEMA.PRODUCT_CATALOG"
    assert view.tables[0].primary_key == ("PRODUCT_ID",)
    assert view.dimensions[0].comment == "Manifest description."


def test_semantic_views_enabled_default_follows_folder_routes(tmp_path: Path) -> None:
    manifest = write_project(tmp_path)
    views_dir = tmp_path / "semantic_models" / "semantic_views"
    (views_dir / "archive").mkdir()
    (views_dir / "archive" / "old.yml").write_text(
        "semantic_views:\n  - name: retired\n    description: Retired.\n    tables: [\"{{ ref('products') }}\"]\n"
        "  - name: kept\n    enabled: true\n    description: Kept.\n    tables: [\"{{ ref('products') }}\"]\n",
        encoding="utf-8",
    )
    (tmp_path / "sst_config.yml").write_text(
        "project:\n  semantic_models_dir: semantic_models\nsemantic_views:\n  archive:\n    +enabled: false\n",
        encoding="utf-8",
    )
    names = [view.fqn.rsplit(".", 1)[-1] for view in load_views(tmp_path, manifest_path=manifest)]
    assert sorted(names) == ["CATALOG", "KEPT"]


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
    project = load_project(tmp_path, manifest_path=manifest)
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
    project = load_project(tmp_path, manifest_path=manifest)
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
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), parse_yaml_bytes)
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
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), parse_yaml_bytes)

    assert [diagnostic.code for diagnostic in _verified_query_diagnostics(documents, tmp_path, "semantic_models")] == [
        "SST-VAL412",
        "SST-LOD018",
        "SST-LOD019",
    ]
    assert [
        diagnostic.code for diagnostic in _relationship_parse_diagnostics(documents, tmp_path, "semantic_models")
    ] == ["SST-VAL201"]


def test_a_vq_sql_file_that_is_not_utf8_is_reported_instead_of_raising(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = write_project(tmp_path)
    verified = tmp_path / "semantic_models" / "verified_queries"
    verified.mkdir()
    (verified / "queries.yml").write_text(
        "snowflake_verified_queries:\n"
        "  - name: latin1\n"
        "    question: q\n"
        "    tables: [products]\n"
        "    sql_file: latin1.sql\n"
        "  - name: plain\n"
        "    question: q2\n"
        "    tables: [products]\n"
        "    sql_file: sql/plain.sql\n",
        encoding="utf-8",
    )
    (verified / "latin1.sql").write_bytes(b"SELECT 'caf\xe9'\n")
    (verified / "sql").mkdir()
    (verified / "sql" / "plain.sql").write_text("SELECT 1\n", encoding="utf-8")
    # From inside the project, as `sst` runs, the project directory is relative.
    monkeypatch.chdir(tmp_path)

    project = load_project(Path("."), manifest_path=manifest)

    assert [(item.subject, dict(item.context)) for item in project.diagnostics if item.code == "SST-PRS122"] == [
        ("verified_query:latin1", {"file": "semantic_models/verified_queries/latin1.sql", "offset": 11})
    ]


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
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), parse_yaml_bytes)
    diagnostics = _metric_parse_diagnostics(documents, tmp_path, "semantic_models")
    assert [diagnostic.code for diagnostic in diagnostics] == [
        "SST-PRS002",
        "SST-PRS113",
        "SST-PRS003",
        "SST-PRS003",
        "SST-PRS013",
        "SST-PRS029",
    ]


def test_every_unread_key_is_reported_and_0_3_spellings_are_errors(tmp_path: Path) -> None:
    write_project(tmp_path)
    root = tmp_path / "semantic_models"
    (root / "semantic_views" / "views.yml").write_text(
        "semantic_views:\n"
        "  - name: catalog\n"
        "    description: Product catalog.\n"
        "    owner: data-team\n"
        "    tables: [\"{{ ref('products') }}\"]\n"
        "    table_config:\n      products:\n        synonyms: [items]\n        alias: goods\n"
        "    variables:\n      - name: floor\n        data_type: NUMBER\n"
        "        default_value: 1\n        unit: cents\n"
        "    tags:\n      - name: \"{{ tag('tier') }}\"\n        value: gold\n        note: x\n",
        encoding="utf-8",
    )
    (root / "metrics" / "metrics.yml").write_text(
        "snowflake_metrics:\n"
        "  - name: product_count\n"
        "    tables: [\"{{ ref('products') }}\"]\n"
        "    expr: COUNT(*)\n"
        "    visibility: private\n"
        "    non_additive_by: [product_id]\n"
        "    non_additive_dimensions:\n      - dimension: product_id\n        grain: day\n"
        "        order: desc\n        nulls: first\n"
        "    default_aggregation: sum\n",
        encoding="utf-8",
    )
    for folder, text in {
        "custom_instructions": (
            "snowflake_custom_instructions:\n  - name: tone\n    description: Reader note.\n"
            "    sql_generation: Round.\n    question_categorization: Decline.\n    consumer: analyst\n"
        ),
        "filters": (
            "snowflake_filters:\n  - name: cheap\n    tables: [\"{{ ref('products') }}\"]\n"
            "    expr: \"{{ ref('products', 'product_id') }} < 5\"\n    synonyms: [budget]\n"
        ),
        "verified_queries": (
            "snowflake_verified_queries:\n  - name: how_many\n    description: Reader note.\n"
            "    question: How many?\n    sql: SELECT 1\n    tables: [\"{{ ref('products') }}\"]\n    tags: [x]\n"
        ),
        "relationships": (
            "snowflake_relationships:\n  - name: self\n    description: Reader note.\n"
            "    left_table: products\n    right_table: products\n    join_type: left_outer\n"
            "    relationship_columns:\n      - left_column: product_id\n        right_column: product_id\n"
        ),
    }.items():
        (root / folder).mkdir(exist_ok=True)
        (root / folder / f"{folder}.yml").write_text(text, encoding="utf-8")
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), parse_yaml_bytes)
    found = [
        (item.code, item.severity.name, item.subject, item.context["field"], item.context.get("expected"))
        for item in _authored_key_diagnostics(documents)
    ]
    assert sorted(found) == sorted(
        [
            ("SST-PRS004", "WARNING", "semantic_view:catalog", "owner", None),
            ("SST-PRS004", "WARNING", "semantic_view:catalog", "table_config.products.alias", None),
            ("SST-PRS004", "WARNING", "semantic_view:catalog", "variables[0].unit", None),
            ("SST-PRS004", "WARNING", "semantic_view:catalog", "tags[0].note", None),
            ("SST-PRS020", "ERROR", "metric:product_count", "visibility", "access_modifier"),
            ("SST-PRS020", "ERROR", "metric:product_count", "non_additive_by", "non_additive_dimensions"),
            ("SST-PRS004", "WARNING", "metric:product_count", "non_additive_dimensions[0].grain", None),
            (
                "SST-PRS020",
                "ERROR",
                "metric:product_count",
                "non_additive_dimensions[0].order",
                "sort_direction",
            ),
            ("SST-PRS020", "ERROR", "metric:product_count", "non_additive_dimensions[0].nulls", "null_order"),
            ("SST-PRS004", "WARNING", "metric:product_count", "default_aggregation", None),
            ("SST-PRS020", "ERROR", "custom_instruction:tone", "sql_generation", "ai_sql_generation"),
            (
                "SST-PRS020",
                "ERROR",
                "custom_instruction:tone",
                "question_categorization",
                "ai_question_categorization",
            ),
            ("SST-PRS004", "WARNING", "custom_instruction:tone", "consumer", None),
            ("SST-PRS004", "WARNING", "filter:cheap", "synonyms", None),
            ("SST-PRS004", "WARNING", "verified_query:how_many", "tags", None),
            ("SST-PRS004", "WARNING", "relationship:self", "join_type", None),
            ("SST-PRS020", "ERROR", "relationship:self", "relationship_columns", "relationship_conditions"),
        ]
    )
    visibility = next(item for item in _authored_key_diagnostics(documents) if item.context["field"] == "visibility")
    assert visibility.message == "metric:product_count: 'visibility' was renamed in 1.0; use 'access_modifier'"
    assert visibility.origin is not None and visibility.origin.line == 5
    # The renamed relationship shape is not also reported as having no conditions.
    assert _relationship_parse_diagnostics(documents, tmp_path, "semantic_models") == ()


def test_nameless_and_duplicate_members_are_reported(tmp_path: Path) -> None:
    write_project(tmp_path)
    root = tmp_path / "semantic_models"
    for folder, text in {
        "filters": (
            "snowflake_filters:\n  - expr: 'TRUE'\n  - just a string\n"
            "  - name: cheap\n    expr: 'TRUE'\n    tables: [products]\n"
        ),
        "custom_instructions": "snowflake_custom_instructions:\n  - name: Tone\n    ai_sql_generation: Round.\n",
        "more_instructions": "snowflake_custom_instructions:\n  - name: tone\n    ai_sql_generation: Truncate.\n",
    }.items():
        (root / folder).mkdir()
        (root / folder / f"{folder}.yml").write_text(text, encoding="utf-8")
    (root / "metrics" / "metrics.yml").write_text(
        "snowflake_metrics:\n  - expr: COUNT(*)\n    tables: [products]\n", encoding="utf-8"
    )
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), parse_yaml_bytes)
    found = sorted(
        (item.code, item.subject, item.context.get("index", item.context.get("name")))
        for item in _member_name_diagnostics(documents)
    )
    assert found == [
        ("SST-PRS106", "custom_instruction:tone", "tone"),
        ("SST-PRS107", "filter:0", 0),
        ("SST-PRS107", "filter:1", 1),
        ("SST-PRS107", "metric:0", 0),
    ]


def test_view_tags_must_be_a_list_and_an_empty_file_only_warns(tmp_path: Path) -> None:
    manifest = write_project(tmp_path)
    views = tmp_path / "semantic_models" / "semantic_views"
    (views / "views.yml").write_text(
        "semantic_views:\n  - name: catalog\n    description: Product catalog.\n"
        "    tables: [\"{{ ref('products') }}\"]\n    tags: {tier: gold}\n",
        encoding="utf-8",
    )
    (views / "empty.yml").write_text("# nothing here yet\n", encoding="utf-8")
    project = load_project(tmp_path, manifest_path=manifest)
    assert project.views == ()
    assert [(item.code, item.severity.name) for item in project.diagnostics] == [
        ("SST-LOD003", "WARNING"),
        ("SST-PRS027", "ERROR"),
    ]


def test_a_meta_sst_data_type_that_disagrees_with_dbt_warns(tmp_path: Path) -> None:
    manifest_path = write_project(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    column = manifest["nodes"]["model.fixture.products"]["columns"]["product_id"]
    column["meta"]["sst"]["data_type"] = "number(38, 0)"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    project = load_project(tmp_path, manifest_path=manifest_path)
    assert [(item.code, item.message) for item in project.diagnostics] == [
        (
            "SST-DBT004",
            "model 'products': column 'product_id' is VARCHAR in dbt and number(38, 0) in the semantic layer",
        )
    ]


def test_non_additive_entries_are_shape_checked(tmp_path: Path) -> None:
    write_project(tmp_path)
    (tmp_path / "semantic_models" / "metrics" / "non_additive.yml").write_text(
        "snowflake_metrics:\n"
        "  - name: not_a_list\n    tables: [products]\n    expr: COUNT(*)\n"
        "    non_additive_dimensions: product_id\n"
        "  - name: bad_entries\n    tables: [products]\n    expr: COUNT(*)\n    non_additive_dimensions:\n"
        "      - product_id\n"
        "      - table: products\n"
        "      - dimension: product_id\n"
        "        table: \"{{ ref('products') }}\"\n"
        "        sort_direction: desc\n"
        "        null_order: [first]\n",
        encoding="utf-8",
    )
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), parse_yaml_bytes)
    assert [
        (item.code, item.subject, item.context["field"])
        for item in _metric_parse_diagnostics(documents, tmp_path, "semantic_models")
    ] == [
        ("SST-PRS003", "metric:not_a_list", "non_additive_dimensions"),
        ("SST-PRS003", "metric:bad_entries", "non_additive_dimensions[0]"),
        ("SST-PRS002", "metric:bad_entries", "non_additive_dimensions[1].dimension"),
        ("SST-PRS003", "metric:bad_entries", "non_additive_dimensions[2].table"),
        ("SST-PRS013", "metric:bad_entries", "non_additive_dimensions[2].sort_direction"),
        ("SST-PRS013", "metric:bad_entries", "non_additive_dimensions[2].null_order"),
    ]


def test_window_blocks_are_shape_checked(tmp_path: Path) -> None:
    write_project(tmp_path)
    head = "    tables: [products]\n    expr: SUM(x)\n"
    (tmp_path / "semantic_models" / "metrics" / "windows.yml").write_text(
        "snowflake_metrics:\n"
        f"  - name: not_a_mapping\n{head}    window: [product_id]\n"
        f"  - name: exclusive\n{head}    using_relationships: [a_to_b]\n"
        "    non_additive_dimensions: [{dimension: product_id}]\n"
        "    window:\n"
        "      partition_by: product_id\n"
        "      partition_by_excluding: [\"{{ ref('products', 'product_id') }}\"]\n"
        f"  - name: bad_order\n{head}    window:\n"
        "      order_by: [1, {sort_direction: up, null_order: middle}, \"{{ ref('products', 'product_id') }}\"]\n"
        "      frame: ROWS 2 PRECEDING\n"
        f"  - name: order_not_a_list\n{head}    window:\n      order_by: product_id\n      frame: 7\n",
        encoding="utf-8",
    )
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), parse_yaml_bytes)
    found = [
        (item.code, item.subject, item.context.get("field") or item.context.get("value"))
        for item in _metric_parse_diagnostics(documents, tmp_path, "semantic_models")
    ]
    assert found == [
        ("SST-PRS003", "metric:not_a_mapping", "window"),
        ("SST-PRS014", "metric:exclusive", "window"),
        ("SST-PRS014", "metric:exclusive", "window"),
        ("SST-PRS003", "metric:exclusive", "window.partition_by"),
        ("SST-PRS014", "metric:exclusive", "window.partition_by"),
        ("SST-PRS003", "metric:bad_order", "window.order_by[0]"),
        ("SST-PRS002", "metric:bad_order", "window.order_by[1].ref"),
        ("SST-PRS013", "metric:bad_order", "window.order_by[1].sort_direction"),
        ("SST-PRS013", "metric:bad_order", "window.order_by[1].null_order"),
        ("SST-PRS124", "metric:bad_order", "ROWS 2 PRECEDING"),
        ("SST-PRS003", "metric:order_not_a_list", "window.order_by"),
        ("SST-PRS124", "metric:order_not_a_list", 7),
    ]
    others = [item.context["other"] for item in _metric_parse_diagnostics(documents, tmp_path, "semantic_models")[1:3]]
    assert others == ["using_relationships", "non_additive_dimensions"]


@pytest.mark.parametrize(
    ("authored", "canonical"),
    [
        (" rows  between 2 preceding and current row ", "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"),
        (
            "range between interval '6 days' preceding and current row",
            "RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW",
        ),
        (
            "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
        ),
        ("ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING", "ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING"),
        ("ROWS UNBOUNDED PRECEDING", None),
        ("ROWS BETWEEN x PRECEDING AND CURRENT ROW", None),
        ("ROWS BETWEEN 1 PRECEDING AND CURRENT ROW; DROP TABLE t", None),
        (["ROWS"], None),
    ],
)
def test_a_frame_is_snowflakes_frame_grammar_and_nothing_else(authored: object, canonical: str | None) -> None:
    assert _frame(authored) == canonical


def test_window_keys_are_checked_and_0_3_order_spellings_are_errors(tmp_path: Path) -> None:
    write_project(tmp_path)
    (tmp_path / "semantic_models" / "metrics" / "windows.yml").write_text(
        "snowflake_metrics:\n"
        "  - name: running\n"
        "    tables: [products]\n"
        "    expr: SUM(x)\n"
        "    window:\n"
        "      partiton_by: [a]\n"
        "      order_by:\n"
        "        - column: \"{{ ref('products', 'product_id') }}\"\n"
        "          direction: desc\n"
        "          nulls: first\n",
        encoding="utf-8",
    )
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), parse_yaml_bytes)
    assert [
        (item.code, item.context["field"], item.context.get("expected"))
        for item in _authored_key_diagnostics(documents)
    ] == [
        ("SST-PRS004", "window.partiton_by", None),
        ("SST-PRS020", "window.order_by[0].column", "ref"),
        ("SST-PRS020", "window.order_by[0].direction", "sort_direction"),
        ("SST-PRS004", "window.order_by[0].nulls", None),
    ]


def test_a_window_dimension_the_metric_cannot_reach_fails_the_view(tmp_path: Path) -> None:
    manifest_path = write_project(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["nodes"]["model.fixture.calendar"] = {
        "resource_type": "model",
        "name": "calendar",
        "description": "Calendar months.",
        "relation_name": "db.sch.calendar",
        "config": {"meta": {"sst": {"primary_key": ["month"]}}},
        "columns": {
            "month": {
                "name": "month",
                "description": "Month.",
                "data_type": "DATE",
                "meta": {"sst": {"column_type": "time_dimension"}},
            }
        },
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (tmp_path / "semantic_models" / "semantic_views" / "views.yml").write_text(
        "semantic_views:\n  - name: catalog\n    description: Product catalog.\n"
        "    tables:\n      - \"{{ ref('products') }}\"\n      - \"{{ ref('calendar') }}\"\n",
        encoding="utf-8",
    )
    (tmp_path / "semantic_models" / "metrics" / "metrics.yml").write_text(
        "snowflake_metrics:\n"
        "  - name: product_count\n    description: Products.\n    tables: [\"{{ ref('products') }}\"]\n"
        "    expr: \"COUNT({{ ref('products', 'product_id') }})\"\n"
        "  - name: running_product_count\n    description: Running.\n    tables: [\"{{ ref('products') }}\"]\n"
        "    expr: \"SUM({{ metric('product_count') }})\"\n"
        "    window:\n      order_by: [\"{{ ref('calendar', 'month') }}\"]\n",
        encoding="utf-8",
    )
    project = load_project(tmp_path, manifest_path=manifest_path)
    assert project.views == ()
    assert [(item.code, item.subject, item.context["field"]) for item in project.diagnostics] == [
        ("SST-VAL125", "semantic_view:catalog", "order_by[0]")
    ]
    assert "a dimension PRODUCTS reaches in this view" in project.diagnostics[0].message


def test_a_non_additive_table_outside_the_view_fails_the_view(tmp_path: Path) -> None:
    manifest_path = write_project(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["nodes"]["model.fixture.calendar"] = {
        "resource_type": "model",
        "name": "calendar",
        "description": "Calendar months.",
        "relation_name": "db.sch.calendar",
        "config": {"meta": {"sst": {"primary_key": ["month"]}}},
        "columns": {
            "month": {
                "name": "month",
                "description": "Month.",
                "data_type": "DATE",
                "meta": {"sst": {"column_type": "time_dimension"}},
            }
        },
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (tmp_path / "semantic_models" / "metrics" / "metrics.yml").write_text(
        "snowflake_metrics:\n"
        "  - name: product_count\n"
        "    description: Products.\n"
        "    tables: [\"{{ ref('products') }}\"]\n"
        "    expr: \"COUNT({{ ref('products', 'product_id') }})\"\n"
        "    non_additive_dimensions:\n"
        "      - table: calendar\n"
        "        dimension: month\n",
        encoding="utf-8",
    )
    project = load_project(tmp_path, manifest_path=manifest_path)
    assert project.views == ()
    assert [(item.code, item.subject, item.context["value"]) for item in project.diagnostics] == [
        ("SST-VAL118", "semantic_view:catalog", "calendar.month")
    ]


def test_a_ref_relationship_endpoint_is_an_error_naming_the_codemod(tmp_path: Path) -> None:
    manifest = write_project(tmp_path)
    relationships = tmp_path / "semantic_models" / "relationships"
    relationships.mkdir()
    (relationships / "relationships.yml").write_text(
        "snowflake_relationships:\n"
        "  - name: self\n"
        "    left_table: \"{{ ref('products') }}\"\n"
        "    right_table: products\n"
        "    relationship_conditions:\n"
        "      - \"{{ ref('products', 'product_id') }} = {{ ref('products', 'product_id') }}\"\n",
        encoding="utf-8",
    )
    project = load_project(tmp_path, manifest_path=manifest)
    endpoint = [item for item in project.diagnostics if item.code == "SST-REF045"]
    assert [(item.subject, item.context["field"]) for item in endpoint] == [("relationship:self", "left_table")]
    assert endpoint[0].severity.name == "ERROR" and "sst migrate refs" in str(ERROR_REGISTRY["SST-REF045"].suggestion)


def test_0_3_key_forms_and_unknown_meta_keys_are_reported_where_a_view_uses_the_model(tmp_path: Path) -> None:
    manifest_path = write_project(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    node = manifest["nodes"]["model.fixture.products"]
    node["config"]["meta"]["sst"] = {"primary_key": "product_id", "cortex_searchable": True}
    node["columns"]["product_id"]["meta"]["sst"]["privacy_category"] = "none"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    project = load_project(tmp_path, manifest_path=manifest_path)
    assert [(item.code, item.severity.name, item.subject) for item in project.diagnostics] == [
        ("SST-DBT005", "ERROR", "dbt_model:products"),
        ("SST-PRS004", "WARNING", "dbt_model:products"),
        ("SST-PRS004", "WARNING", "dbt_column:products.product_id"),
    ]
    assert project.diagnostics[1].context["field"] == "meta.sst.cortex_searchable"
    assert project.diagnostics[2].context["field"] == "meta.sst.privacy_category"


def test_filter_labels_must_be_a_list_of_strings(tmp_path: Path) -> None:
    manifest = write_project(tmp_path)
    (tmp_path / "semantic_models" / "filters").mkdir()
    (tmp_path / "semantic_models" / "filters" / "filters.yml").write_text(
        "snowflake_filters:\n"
        "  - name: scalar_label\n    tables: [\"{{ ref('products') }}\"]\n"
        "    expr: \"{{ ref('products', 'product_id') }} < 5\"\n    labels: filter\n"
        "  - name: listed_label\n    tables: [\"{{ ref('products') }}\"]\n"
        "    expr: \"{{ ref('products', 'product_id') }} < 5\"\n    labels: [filter]\n",
        encoding="utf-8",
    )
    project = load_project(tmp_path, manifest_path=manifest)
    shape = [item for item in project.diagnostics if item.code == "SST-PRS003"]
    assert [(item.context["artifact"], item.context["field"]) for item in shape] == [("filter:scalar_label", "labels")]
    assert shape[0].origin is not None and shape[0].origin.file == "semantic_models/filters/filters.yml"


@pytest.mark.parametrize("relative", ["semantic_models/metrics/stray.yml", "semantic_models/stray.yml"])
def test_a_view_list_outside_the_views_folder_is_reported_not_dropped_silently(tmp_path: Path, relative: str) -> None:
    manifest = write_project(tmp_path)
    (tmp_path / relative).write_text(
        "semantic_views:\n  - name: stray\n    description: Stray view.\n    tables: [\"{{ ref('products') }}\"]\n",
        encoding="utf-8",
    )
    project = load_project(tmp_path, manifest_path=manifest)
    assert [view.fqn for view in project.views] == ["DB.SCH.CATALOG"]
    assert [(item.code, item.context["artifact"], item.context["field"]) for item in project.diagnostics] == [
        ("SST-PRS004", relative, "semantic_views")
    ]
    origin = project.diagnostics[0].origin
    assert origin is not None and (origin.file, origin.line) == (relative, 2)
