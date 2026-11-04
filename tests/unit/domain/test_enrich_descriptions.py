"""The descriptions `sst enrich --include descriptions` fills from Snowflake comments."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.enrich import (
    ColumnUpdate,
    Component,
    EnrichOptions,
    ModelEnrichment,
    WarehouseColumn,
    description_value,
    enrich_model,
    needs_description,
    resolve_options,
    table_description,
)
from snowflake_semantic_tools.domain.model.config_schema import EnrichmentConfig
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel

C = Component
SETTINGS = EnrichmentConfig(distinct_limit=3, display_limit=2)
W = WarehouseColumn


def _options(*included: Component, forced: tuple[Component, ...] = ()) -> EnrichOptions:
    return resolve_options(frozenset(included), frozenset(forced))


def _column(name: str, *keys: str, **fields: object) -> DbtColumn:
    values: dict[str, object] = {"description": None, "data_type": None, "column_type": None}
    values.update(fields)
    return DbtColumn(name, declared_keys=frozenset(keys), **values)  # type: ignore[arg-type]


def _model(*columns: DbtColumn) -> DbtModel:
    return DbtModel("model.p.orders", "orders", "DB.SCH.ORDERS", (), (), columns)


def test_descriptions_is_a_component_all_names_and_the_default_leaves_out() -> None:
    assert C("descriptions") is C.DESCRIPTIONS
    assert _options(C.DESCRIPTIONS).includes(C.DESCRIPTIONS)
    assert not _options().includes(C.DESCRIPTIONS)
    assert not _options(C.DESCRIPTIONS).reads_data


@pytest.mark.parametrize(
    ("written", "comment", "options", "expected"),
    [
        pytest.param(None, "The order's key", _options(C.DESCRIPTIONS), "The order's key", id="missing"),
        pytest.param("", "  The order's key\n", _options(C.DESCRIPTIONS), "The order's key", id="empty-trimmed"),
        pytest.param("   ", "Key", _options(C.DESCRIPTIONS), "Key", id="blank-written"),
        pytest.param("Hand written", "Key", _options(C.DESCRIPTIONS), None, id="kept"),
        pytest.param("Hand written", "Key", _options(C.DESCRIPTIONS, forced=(C.DESCRIPTIONS,)), "Key", id="forced"),
        pytest.param("Key", " Key ", _options(C.DESCRIPTIONS, forced=(C.DESCRIPTIONS,)), None, id="forced-same"),
        pytest.param(None, None, _options(C.DESCRIPTIONS), None, id="no-comment"),
        pytest.param(None, "  \n", _options(C.DESCRIPTIONS), None, id="blank-comment"),
        pytest.param(None, "Key", _options(C.COLUMN_TYPES), None, id="not-included"),
    ],
)
def test_a_comment_fills_a_missing_description_and_replaces_one_only_when_forced(
    written: str | None, comment: str | None, options: EnrichOptions, expected: str | None
) -> None:
    assert description_value(written, comment, options) == expected


def test_only_a_model_without_a_description_or_a_forced_run_needs_its_comment() -> None:
    assert needs_description(None, _options(C.DESCRIPTIONS))
    assert needs_description(" ", _options(C.DESCRIPTIONS))
    assert not needs_description("One row per order", _options(C.DESCRIPTIONS))
    assert needs_description("One row per order", _options(C.DESCRIPTIONS, forced=(C.DESCRIPTIONS,)))
    assert not needs_description(None, _options(C.SAMPLE_VALUES))


def test_a_table_comment_fills_the_model_description_unless_it_holds_template_syntax() -> None:
    options = _options(C.DESCRIPTIONS)
    assert table_description("orders", None, "One row per order", options) == ("One row per order", ())
    assert table_description("orders", "Written", "One row per order", options) == (None, ())
    description, refused = table_description("orders", None, "Orders {{ run_query('x') }}", options)
    assert description is None
    assert [(item.code, item.subject) for item in refused] == [("SST-SNO031", "dbt_model:orders")]
    assert refused[0].message == (
        "model 'orders': writing the model's description failed: the relation's comment holds "
        "template syntax, which dbt would run; nothing is written for it"
    )


def test_column_comments_fill_missing_descriptions_beside_the_other_components() -> None:
    model = _model(
        _column("order_id", "column_type", description="Written by hand", column_type="dimension"),
        _column("status", "column_type", column_type="dimension"),
        _column("hidden", excluded=True),
    )
    warehouse = [
        W("ORDER_ID", "TEXT", comment="The order's key"),
        W("STATUS", "TEXT", comment="Where the order is"),
        W("HIDDEN", "TEXT", comment="Never read"),
        W("TOTAL", "NUMBER", comment="  Order total in USD  "),
        W("NOTE", "TEXT"),
    ]
    options = _options(C.COLUMN_TYPES, C.DESCRIPTIONS)
    result = enrich_model(model, warehouse, options, SETTINGS, samples={}, synonyms={})
    assert result.updates == (
        ColumnUpdate("status", (), description="Where the order is"),
        ColumnUpdate("total", (("column_type", "fact"),), added=True, description="Order total in USD"),
        ColumnUpdate("note", (("column_type", "dimension"),), added=True),
    )
    assert result.filled(C.DESCRIPTIONS) == 2 and result.filled(C.COLUMN_TYPES) == 2
    assert result.updates[1].keys == ("description", "column_type")
    forced = enrich_model(
        model, warehouse[:1], _options(C.DESCRIPTIONS, forced=(C.DESCRIPTIONS,)), SETTINGS, samples={}, synonyms={}
    )
    assert forced.updates == (ColumnUpdate("order_id", (), description="The order's key"),)


def test_a_column_comment_holding_template_syntax_writes_nothing_for_the_column() -> None:
    warehouse = [W("STATUS", "TEXT", comment="{% if x %}open{% endif %}"), W("OK", "TEXT", comment="Fine")]
    result = enrich_model(
        _model(), warehouse, _options(C.COLUMN_TYPES, C.DESCRIPTIONS), SETTINGS, samples={}, synonyms={}
    )
    assert result.updates == (ColumnUpdate("ok", (("column_type", "dimension"),), added=True, description="Fine"),)
    assert [(item.code, item.subject) for item in result.diagnostics] == [("SST-SNO031", "dbt_column:orders.status")]


def test_an_update_counts_its_description_once() -> None:
    enrichment = ModelEnrichment(
        "orders", (ColumnUpdate("a", (), description="A"), ColumnUpdate("b", (("synonyms", ("x",)),)))
    )
    assert enrichment.filled(C.DESCRIPTIONS) == 1 and enrichment.filled(C.COLUMN_SYNONYMS) == 1
    assert enrichment.updates[0].keys == ("description",)
