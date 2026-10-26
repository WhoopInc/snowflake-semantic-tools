"""Mapping a live semantic view: relationships, window metrics, verified queries, settings, and model keys."""

from __future__ import annotations

import dataclasses
from typing import Any, cast

from snowflake_semantic_tools.domain.importing.placement import Destination
from snowflake_semantic_tools.domain.importing.semantic_view import ViewPlacement, import_semantic_view
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.import_fakes import dbt_column, dbt_model, index, view_document

_VIEW = QualifiedName.parse("DB.SV.ORDERS_VIEW")
_PLACE = ViewPlacement("semantic_models", Destination(("sales",)))
_MODELS = {("DB", "MARTS", "ORDERS"): "orders", ("DB", "MARTS", "CUSTOMERS"): "customers"}


def _project(*columns: Any, model_fields: dict[str, Any] | None = None, **fields: Any) -> Any:
    model = dataclasses.replace(dbt_model("orders", "DB.MARTS.ORDERS", *columns), **(model_fields or {}))
    return index(models=_MODELS, dbt_models={"orders": model}, **fields)


def _files(imported: Any) -> dict[str, Any]:
    return {file.path: file.document for file in imported.files}


def _body(imported: Any) -> dict[str, Any]:
    return cast(dict[str, Any], imported.files[0].document["semantic_view"])


def _details(imported: Any) -> list[str]:
    return [item.message for item in imported.diagnostics if item.code == "SST-IMP013"]


def _customers() -> dict[str, Any]:
    return {"name": "CUSTOMERS", "base_table": {"database": "DB", "schema": "MARTS", "table": "CUSTOMERS"}}


def test_a_relationship_writes_equality_asof_and_range_conditions_and_one_to_an_unknown_table_is_left_out() -> None:
    document = view_document()
    document["tables"].append(_customers())
    document["relationships"] = [
        {
            "name": "ORDERS_TO_CUSTOMERS",
            "left_table": "ORDERS",
            "right_table": "CUSTOMERS",
            "relationship_columns": [
                {"left_column": "CUSTOMER_ID", "right_column": "ID"},
                {"left_column": "ORDERED_AT", "right_column": "SINCE", "type": "asof"},
                {
                    "left_column": "ORDERED_AT",
                    "type": "range",
                    "right_range": {"start_column": "VALID_FROM", "end_column": "VALID_TO"},
                },
            ],
        },
        {"name": "TO_NOWHERE", "left_table": "ORDERS", "right_table": "GHOST"},
    ]
    project = _project(view_models={"orders_view": frozenset({"orders"}), "other_view": frozenset({"customers"})})
    imported = import_semantic_view(_VIEW, document, project, _PLACE)
    assert _body(imported)["tables"] == ["{{ ref('orders') }}", "{{ ref('customers') }}"]
    assert _files(imported)["semantic_models/sales/relationships/orders_to_customers.yml"] == {
        "relationship": {
            "name": "orders_to_customers",
            "left_table": "{{ ref('orders') }}",
            "right_table": "{{ ref('customers') }}",
            "relationship_conditions": [
                "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'id') }}",
                "{{ ref('orders', 'ordered_at') }} >= {{ ref('customers', 'since') }}",
                "{{ ref('orders', 'ordered_at') }} BETWEEN {{ ref('customers', 'valid_from') }}"
                " AND {{ ref('customers', 'valid_to') }}",
            ],
        }
    }
    details = _details(imported)
    assert any("relationship to_nowhere joins a table that is no dbt model" in detail for detail in details)
    assert any("semantic_view:other_view publish them too" in detail for detail in details)
    assert not any("semantic_view:orders_view publish" in detail for detail in details)


def test_window_metrics_become_window_blocks_and_ones_sst_cannot_write_are_left_out() -> None:
    document = view_document()
    document["tables"][0]["metrics"] = [
        {
            "name": "RUNNING",
            "expr": "SUM(ORDERS.AMOUNT) OVER (PARTITION BY EXCLUDING ORDERS.REGION "
            "ORDER BY ORDERS.DAY DESC NULLS LAST ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
        },
        {"name": "BY_REGION", "expr": "SUM(ORDERS.AMOUNT) OVER (PARTITION BY ORDERS.REGION)"},
        {"name": "BAD_WINDOW", "expr": "SUM(ORDERS.AMOUNT) OVER (ORDER BY 1)"},
        {"name": "BAD_TEXT", "expr": "SUM('open"},
    ]
    imported = import_semantic_view(_VIEW, document, _project(), _PLACE)
    files = _files(imported)
    assert files["semantic_models/sales/metrics/running.yml"] == {
        "metric": {
            "name": "running",
            "expr": "SUM({{ ref('orders', 'amount') }})",
            "window": {
                "partition_by_excluding": ["{{ ref('orders', 'region') }}"],
                "order_by": [
                    {"ref": "{{ ref('orders', 'day') }}", "sort_direction": "descending", "null_order": "last"}
                ],
                "frame": "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
            },
        }
    }
    assert files["semantic_models/sales/metrics/by_region.yml"]["metric"]["window"] == {
        "partition_by": ["{{ ref('orders', 'region') }}"]
    }
    assert "semantic_models/sales/metrics/bad_window.yml" not in files
    assert "semantic_models/sales/metrics/bad_text.yml" not in files
    details = _details(imported)
    assert any("metric bad_window's window, OVER (ORDER BY 1), is not one SST can write" in d for d in details)
    assert any("metric bad_text's expression does not lex" in detail for detail in details)


def test_a_metric_keeps_its_synonyms_relationships_non_additive_dimensions_and_private_access() -> None:
    document = view_document()
    document["metrics"] = [
        {
            "name": "AVG_SALE",
            "expr": "DIV0(ORDERS.AMOUNT, 2)",
            "synonyms": ["mean sale"],
            "using_relationships": ["ORDERS_TO_CUSTOMERS"],
            "non_additive_dimensions": [
                {"table": "ORDERS", "dimension": "DAY", "sort_direction": "DESCENDING", "null_order": "FIRST"},
                {"table": "ELSEWHERE", "dimension": "X"},
            ],
            "access_modifier": "private_access",
        }
    ]
    imported = import_semantic_view(_VIEW, document, _project(dbt_column("DAY")), _PLACE)
    assert _files(imported)["semantic_models/sales/metrics/avg_sale.yml"] == {
        "metric": {
            "name": "avg_sale",
            "expr": "DIV0({{ ref('orders', 'amount') }}, 2)",
            "synonyms": ["mean sale"],
            "using_relationships": ["{{ relationship('orders_to_customers') }}"],
            "non_additive_dimensions": [
                {"ref": "{{ ref('orders', 'DAY') }}", "sort_direction": "descending", "null_order": "first"},
                {"ref": "{{ ref('elsewhere', 'x') }}"},
            ],
            "access_modifier": "private_access",
        }
    }


def test_verified_queries_variables_staleness_and_instructions_are_written_as_the_project_spells_them() -> None:
    document = view_document()
    document["description"] = "Sales view."
    document["verified_queries"] = [
        {
            "name": "TOP",
            "question": "Top?",
            "sql": "SELECT * FROM ORDERS",
            "use_as_onboarding_question": True,
            "verified_at": 1700000000,
            "verified_by": "analyst",
        },
        {"name": "PLAIN", "sql": "SELECT 1", "verified_at": "yesterday"},
        {"name": "BROKEN", "sql": "SELECT 'x"},
    ]
    document["variables"] = [
        {"name": "FLAG", "data_type": "BOOLEAN", "default_value": "TRUE", "description": "A flag."},
        {"name": "MAYBE", "data_type": "BOOLEAN", "default_value": "yes"},
        {"name": "N", "data_type": "NUMBER(10,0)", "default_value": "5"},
        {"name": "F", "data_type": "FLOAT", "default_value": "1.5"},
        {"name": "BAD", "data_type": "INTEGER", "default_value": "many"},
        {"name": "S", "data_type": "VARCHAR", "default_value": "x"},
        {"name": "RAW", "data_type": "NUMBER", "default_value": 3},
    ]
    document["max_staleness"] = "300 seconds"
    document["module_custom_instructions"] = {"sql_generation": "Round.", "question_categorization": "Sales only."}
    placement = ViewPlacement("semantic_models", Destination((), overridden=True))
    imported = import_semantic_view(_VIEW, document, _project(), placement)
    body = _body(imported)
    assert body["description"] == "Sales view."
    assert body["variables"] == [
        {"name": "flag", "data_type": "BOOLEAN", "description": "A flag.", "default_value": True},
        {"name": "maybe", "data_type": "BOOLEAN", "default_value": "yes"},
        {"name": "n", "data_type": "NUMBER", "default_value": 5},
        {"name": "f", "data_type": "FLOAT", "default_value": 1.5},
        {"name": "bad", "data_type": "INTEGER", "default_value": "many"},
        {"name": "s", "data_type": "VARCHAR", "default_value": "x"},
        {"name": "raw", "data_type": "NUMBER", "default_value": 3},
    ]
    assert body["config"] == {"database": "DB", "schema": "SV", "max_staleness": 300}
    assert body["custom_instructions"] == ["{{ custom_instructions('orders_view') }}"]
    files = _files(imported)
    assert files["semantic_models/verified_queries/top.yml"] == {
        "verified_query": {
            "name": "top",
            "question": "Top?",
            "sql": "SELECT * FROM {{ ref('orders') }}",
            "use_as_onboarding_question": True,
            "verified_at": 1700000000,
            "verified_by": "analyst",
        }
    }
    assert files["semantic_models/verified_queries/plain.yml"] == {
        "verified_query": {"name": "plain", "question": "", "sql": "SELECT 1"}
    }
    assert "semantic_models/verified_queries/broken.yml" not in files
    assert files["semantic_models/custom_instructions/orders_view.yml"] == {
        "custom_instruction": {
            "name": "orders_view",
            "ai_sql_generation": "Round.",
            "ai_question_categorization": "Sales only.",
        }
    }
    assert any("verified query broken's SQL does not lex" in detail for detail in _details(imported))


def test_instructions_whose_block_the_project_declares_are_reported_and_not_rewritten() -> None:
    document = view_document()
    document["module_custom_instructions"] = {"question_categorization": "Sales only."}
    project = _project(members={"custom_instruction": frozenset({"orders_view"})})
    imported = import_semantic_view(_VIEW, document, project, _PLACE)
    assert _body(imported)["custom_instructions"] == ["{{ custom_instructions('orders_view') }}"]
    assert [file.path for file in imported.files] == ["semantic_models/sales/orders_view.yml"]
    assert any("are the project's block orders_view, which already exists" in d for d in _details(imported))


def test_instructions_with_only_generation_text_write_only_that_text() -> None:
    document = view_document()
    document["module_custom_instructions"] = {"sql_generation": "Round."}
    files = _files(import_semantic_view(_VIEW, document, _project(), _PLACE))
    assert files["semantic_models/sales/custom_instructions/orders_view.yml"] == {
        "custom_instruction": {"name": "orders_view", "ai_sql_generation": "Round."}
    }


def test_staleness_is_written_only_when_it_differs_from_the_folders_and_is_reported_when_unset() -> None:
    def config(staleness: str, **inherited: object) -> tuple[object, list[str]]:
        document = {**view_document(), "max_staleness": staleness}
        placement = ViewPlacement("semantic_models", Destination(("sales",)), inherited)
        imported = import_semantic_view(_VIEW, document, _project(), placement)
        return _body(imported).get("config"), _details(imported)

    assert config("60", max_staleness=60) == (None, [])
    assert config("90", max_staleness=60) == ({"max_staleness": 90}, [])
    assert config("5 minutes") == (None, [])
    assert config("a b c") == (None, [])
    _, details = config("", max_staleness=60)
    assert any("its folder sets max_staleness 60, which the live view does not set" in d for d in details)


def test_model_keys_the_project_sets_otherwise_are_reported_and_equal_ones_are_kept_quietly() -> None:
    document = view_document(description="Live orders.")
    table = document["tables"][0]
    table["name"] = "SALES_ORDERS"
    table["primary_key"] = {"columns": ["ID"]}
    table["unique_keys"] = [{"columns": ["CODE"]}]
    table["synonyms"] = ["Purchases"]
    table["constraints"] = [
        {"name": "NOT_A_RANGE"},
        {"distinct_range": {"start_column": "VALID_FROM", "end_column": "VALID_TO"}},
    ]
    fields = {
        "primary_key": ("order_id",),
        "unique_keys": (("code",),),
        "synonyms": ("purchases",),
        "distinct_range": ("starts", "ends"),
        "description": "Orders.",
    }
    imported = import_semantic_view(_VIEW, document, _project(model_fields=fields), _PLACE)
    mismatched = [item.context["key"] for item in imported.diagnostics if item.code == "SST-IMP011"]
    assert mismatched == ["primary_key", "distinct_range", "description"]
    assert imported.model_edits == ()
    assert any("table SALES_ORDERS is written as its model, orders" in detail for detail in _details(imported))


def test_a_distinct_range_the_model_leaves_unset_is_set_on_it() -> None:
    document = view_document()
    document["tables"][0]["constraints"] = [
        {"distinct_range": {"start_column": "VALID_FROM", "end_column": "VALID_TO"}}
    ]
    [edit] = import_semantic_view(_VIEW, document, _project(), _PLACE).model_edits
    assert edit.meta == (("distinct_range", {"start": "valid_from", "end": "valid_to"}),)


def test_columns_keep_what_the_project_types_and_report_what_it_sets_otherwise() -> None:
    declared = frozenset({"column_type", "is_enum"})
    columns = (
        dbt_column("state", column_type="dimension", declared_keys=frozenset({"column_type"})),
        dbt_column("ordered_at", column_type="time_dimension", declared_keys=frozenset({"column_type"})),
        dbt_column("amount", column_type="dimension", is_enum=False, declared_keys=declared),
        dbt_column("notes"),
        dbt_column("hidden", column_type="fact", excluded=True),
    )
    document = view_document(
        {"name": "STATE", "expr": "ORDERS.STATE"},
        {"name": "ORDERED_AT", "expr": "ORDERS.ORDERED_AT"},
        {"name": "SECRET", "expr": "ORDERS.SECRET", "access_modifier": "private_access"},
        {"name": "IS_OPEN", "expr": "ORDERS.STATE = 'open", "labels": ["filter"]},
    )
    document["tables"][0]["facts"] = [{"name": "AMOUNT", "expr": "ORDERS.AMOUNT", "is_enum": True}]
    imported = import_semantic_view(_VIEW, document, _project(*columns), _PLACE)
    [edit] = imported.model_edits
    assert [(column.column, column.meta) for column in edit.columns] == [("secret", (("column_type", "dimension"),))]
    mismatched = [
        (item.context["column"], item.context["key"]) for item in imported.diagnostics if item.code == "SST-IMP011"
    ]
    assert mismatched == [("amount", "column_type"), ("amount", "is_enum")]
    details = _details(imported)
    assert any("dimension ORDERS.SECRET is private, which SST does not publish" in d for d in details)
    assert any("filter is_open's expression does not lex" in detail for detail in details)
    assert "exclude_columns" not in _body(imported) and "columns" not in _body(imported)


def test_a_table_read_from_a_dbt_source_is_written_as_its_source_with_nothing_set_on_a_model() -> None:
    document = view_document()
    document["tables"][0]["base_table"] = {"database": "DB", "schema": "RAW", "table": "EVENTS"}
    document["tables"][0]["dimensions"] = [{"name": "KIND", "expr": "ORDERS.KIND"}]
    project = index(sources={("DB", "RAW", "EVENTS"): ("raw", "events")})
    imported = import_semantic_view(_VIEW, document, project, _PLACE)
    assert _body(imported)["tables"] == ["{{ source('raw', 'events') }}"]
    assert imported.model_edits == ()
    assert imported.diagnostics == ()


def test_a_table_whose_base_table_is_incomplete_is_no_model_and_refuses_the_view() -> None:
    document = view_document()
    document["tables"][0]["base_table"] = {"database": "DB"}
    imported = import_semantic_view(_VIEW, document, _project(), _PLACE)
    assert imported.files == ()
    assert [item.code for item in imported.diagnostics] == ["SST-IMP010"]
    assert imported.diagnostics[0].context["value"] == "DB.."
    assert imported.refused


def test_a_filter_without_a_description_writes_its_expression_and_label_only() -> None:
    document = view_document({"name": "IS_OPEN", "expr": "ORDERS.STATE = 'open'", "labels": ["FILTER"]})
    files = _files(import_semantic_view(_VIEW, document, _project(), _PLACE))
    assert files["semantic_models/sales/filters/is_open.yml"] == {
        "filter": {"name": "is_open", "expr": "{{ ref('orders', 'state') }} = 'open'", "labels": ["filter"]}
    }


def test_a_primary_key_and_synonyms_the_model_leaves_unset_are_set_on_it() -> None:
    document = view_document()
    document["tables"][0]["primary_key"] = {"columns": ["ID"]}
    document["tables"][0]["synonyms"] = ["Purchases"]
    [edit] = import_semantic_view(_VIEW, document, _project(dbt_column("ID")), _PLACE).model_edits
    assert edit.meta == (("primary_key", ("ID",)), ("synonyms", ("Purchases",)))
