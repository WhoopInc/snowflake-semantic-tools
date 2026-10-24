"""A semantic view's two definitions: what SST renders, and what DESCRIBE SEMANTIC VIEW shows.

The DESCRIBE rows here have the shapes a live account returned for a published view: JSON lists for
synonyms, sample values and keys, `[["A","B"]]` for unique keys, `["filter"]` labels, `TRUE` for an
onboarding question, the ownership marker at the end of the view's comment, and no row at all for
what a member does not set.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    Relationship,
    SemanticView,
    SortKey,
    Table,
    Variable,
    VerifiedQuery,
    ViewScope,
    Window,
)
from snowflake_semantic_tools.domain.plan.definitions.semantic_view import desired_view, live_view
from snowflake_semantic_tools.domain.plan.member_diff import ADDED, CHANGED, REMOVED, diff_definitions
from snowflake_semantic_tools.domain.ports.snowflake.catalog import DescribedMember
from tests.helpers.sql_values import authored, authored_query, statement

_MARKER = "[sst:" + "a" * 64 + ":" + "b" * 64 + "]"
_SQL = "SELECT\n    orders.location_id\n  , COUNT(*) AS order_count\nFROM orders AS orders\nGROUP BY ALL"


def _view(**changes: object) -> SemanticView:
    view = SemanticView(
        fqn="SALES_DB.SV.SALES",
        tables=(
            Table(
                "ORDERS", "SALES_DB.MARTS.ORDERS", primary_key=("ORDER_ID",), synonyms=("purchases",), comment="Orders."
            ),
            Table(
                "SUPPLIES",
                "SALES_DB.MARTS.SUPPLIES",
                unique_keys=(("SUPPLY_ID", "PRODUCT_ID"),),
                distinct_range=("VALID_FROM", "VALID_TO"),
            ),
        ),
        relationships=(
            Relationship("ORDERS_TO_SUPPLIES", "ORDERS", ("SUPPLY_ID",), "SUPPLIES", ("SUPPLY_ID",)),
            Relationship(
                "ORDERS_ASOF", "ORDERS", ("SUPPLY_ID", "AT"), "SUPPLIES", ("SUPPLY_ID", "VALID_FROM"), asof_index=1
            ),
            Relationship("HIDDEN", "ORDERS", ("A",), "SUPPLIES", ("A",)),
        ),
        variables=(Variable("THRESHOLD", statement("NUMBER"), statement("10")),),
        columns=(
            Column("ORDERS", "ORDER_TOTAL", ColumnKind.FACT, authored("ORDERS.ORDER_TOTAL"), sample_values=("1.00",)),
            Column(
                "ORDERS",
                "ORDER_STATE",
                ColumnKind.DIMENSION,
                authored("ORDERS.ORDER_STATE"),
                comment="State.",
                synonyms=("status",),
                sample_values=("open", "closed"),
                is_enum=True,
            ),
            Column("ORDERS", "ORDERED_AT", ColumnKind.TIME_DIMENSION, authored("ORDERS.ORDERED_AT")),
            Column("ORDERS", "IS_OPEN", ColumnKind.FILTER, authored("ORDERS.ORDER_STATE = 'open'"), comment="Open."),
            Column("ORDERS", "SECRET", ColumnKind.DIMENSION, authored("ORDERS.SECRET")),
        ),
        metrics=(
            Metric("ORDER_COUNT", authored("COUNT(ORDERS.ORDER_ID)"), "ORDERS", comment="Orders.", synonyms=("n",)),
            Metric(
                "STOCK",
                authored("SUM(SUPPLIES.UNITS)"),
                "SUPPLIES",
                non_additive_by=(SortKey(authored("SUPPLIES.VALID_FROM"), nulls_first=True),),
            ),
            Metric("PER_ORDER", authored("DIV0(A, B)"), access_modifier="private_access"),
            Metric("RUNNING", authored("SUM(ORDERS.ORDER_TOTAL)"), "ORDERS", window=Window(order_by=())),
        ),
        comment="Sales.",
        ai_sql_generation="Round to cents.",
        verified_queries=(
            VerifiedQuery("ORDERS_BY_LOCATION", "Orders by location?", authored_query(_SQL), 1791763200, "me", True),
            VerifiedQuery("BARE", "Bare?", authored_query("SELECT 1")),
        ),
        scope=ViewScope(exclude_columns=("ORDERS.SECRET",), exclude_relationships=("HIDDEN",)),
    )
    return replace(view, **changes)  # type: ignore[arg-type]  # a test's own fields


def _row(kind: str | None, name: str | None, prop: str, value: str, parent: str | None = None) -> DescribedMember:
    return DescribedMember(kind, name, parent, prop, value)


def _described() -> tuple[DescribedMember, ...]:
    """The rows DESCRIBE SEMANTIC VIEW returns for `_view()` once it is published."""
    return (
        _row("TABLE", "ORDERS", "BASE_TABLE_DATABASE_NAME", "SALES_DB"),
        _row("TABLE", "ORDERS", "BASE_TABLE_SCHEMA_NAME", "MARTS"),
        _row("TABLE", "ORDERS", "BASE_TABLE_NAME", "ORDERS"),
        _row("TABLE", "ORDERS", "PRIMARY_KEY", '["ORDER_ID"]'),
        _row("TABLE", "ORDERS", "SYNONYMS", '["purchases"]'),
        _row("TABLE", "ORDERS", "COMMENT", "Orders."),
        _row("TABLE", "SUPPLIES", "BASE_TABLE_DATABASE_NAME", "SALES_DB"),
        _row("TABLE", "SUPPLIES", "BASE_TABLE_SCHEMA_NAME", "MARTS"),
        _row("TABLE", "SUPPLIES", "BASE_TABLE_NAME", "SUPPLIES"),
        _row("TABLE", "SUPPLIES", "UNIQUE_KEY", '[["SUPPLY_ID","PRODUCT_ID"]]'),
        _row("CONSTRAINT", "SUPPLIES_DISTINCT_RANGE", "CONSTRAINT_TYPE", "DISTINCT_RANGE"),
        _row("CONSTRAINT", "SUPPLIES_DISTINCT_RANGE", "START_COLUMN", "VALID_FROM"),
        _row("CONSTRAINT", "SUPPLIES_DISTINCT_RANGE", "END_COLUMN", "VALID_TO"),
        _row("RELATIONSHIP", "ORDERS_TO_SUPPLIES", "TABLE", "ORDERS"),
        _row("RELATIONSHIP", "ORDERS_TO_SUPPLIES", "REF_TABLE", "SUPPLIES"),
        _row("RELATIONSHIP", "ORDERS_TO_SUPPLIES", "FOREIGN_KEY", '["SUPPLY_ID"]'),
        _row("RELATIONSHIP", "ORDERS_TO_SUPPLIES", "REF_KEY", '["SUPPLY_ID"]'),
        _row("RELATIONSHIP", "ORDERS_ASOF", "TABLE", "ORDERS"),
        _row("RELATIONSHIP", "ORDERS_ASOF", "REF_TABLE", "SUPPLIES"),
        _row("RELATIONSHIP", "ORDERS_ASOF", "FOREIGN_KEY", '["SUPPLY_ID","AT"]'),
        _row("RELATIONSHIP", "ORDERS_ASOF", "REF_KEY", '["SUPPLY_ID","VALID_FROM"]'),
        _row("FACT", "ORDER_TOTAL", "TABLE", "ORDERS", "ORDERS"),
        _row("FACT", "ORDER_TOTAL", "EXPRESSION", "ORDERS.ORDER_TOTAL", "ORDERS"),
        _row("FACT", "ORDER_TOTAL", "DATA_TYPE", "NUMBER(10,2)", "ORDERS"),
        _row("FACT", "ORDER_TOTAL", "SAMPLE_VALUES", '["1.00"]', "ORDERS"),
        _row("DIMENSION", "ORDER_STATE", "EXPRESSION", "ORDERS.ORDER_STATE", "ORDERS"),
        _row("DIMENSION", "ORDER_STATE", "COMMENT", "State.", "ORDERS"),
        _row("DIMENSION", "ORDER_STATE", "SYNONYMS", '["status"]', "ORDERS"),
        _row("DIMENSION", "ORDER_STATE", "SAMPLE_VALUES", '["open","closed"]', "ORDERS"),
        _row("DIMENSION", "ORDER_STATE", "IS_ENUM", "true", "ORDERS"),
        _row("DIMENSION", "ORDER_STATE", "ACCESS_MODIFIER", "PUBLIC", "ORDERS"),
        _row("DIMENSION", "ORDERED_AT", "EXPRESSION", "ORDERS.ORDERED_AT", "ORDERS"),
        _row("DIMENSION", "IS_OPEN", "EXPRESSION", "ORDERS.ORDER_STATE = 'open'", "ORDERS"),
        _row("DIMENSION", "IS_OPEN", "LABELS", '["filter"]', "ORDERS"),
        _row("DIMENSION", "IS_OPEN", "COMMENT", "Open.", "ORDERS"),
        _row("METRIC", "ORDER_COUNT", "EXPRESSION", "COUNT(ORDERS.ORDER_ID)", "ORDERS"),
        _row("METRIC", "ORDER_COUNT", "COMMENT", "Orders.", "ORDERS"),
        _row("METRIC", "ORDER_COUNT", "SYNONYMS", '["n"]', "ORDERS"),
        _row("METRIC", "ORDER_COUNT", "ACCESS_MODIFIER", "PUBLIC", "ORDERS"),
        _row("METRIC", "STOCK", "EXPRESSION", "SUM(SUPPLIES.UNITS)", "SUPPLIES"),
        _row(
            "METRIC",
            "STOCK",
            "NON_ADDITIVE_DIMENSIONS",
            '[{"table":"SUPPLIES","dimension":"VALID_FROM","order":"ASCENDING","null_order":"FIRST"}]',
            "SUPPLIES",
        ),
        _row("DERIVED_METRIC", "PER_ORDER", "EXPRESSION", "DIV0(A, B)"),
        _row("DERIVED_METRIC", "PER_ORDER", "ACCESS_MODIFIER", "PRIVATE"),
        _row("METRIC", "RUNNING", "EXPRESSION", "SUM(ORDERS.ORDER_TOTAL) OVER ()", "ORDERS"),
        _row("AI_VERIFIED_QUERY", "ORDERS_BY_LOCATION", "QUESTION", "Orders by location?"),
        _row("AI_VERIFIED_QUERY", "ORDERS_BY_LOCATION", "SQL", _SQL),
        _row("AI_VERIFIED_QUERY", "ORDERS_BY_LOCATION", "VERIFIED_AT", "1791763200"),
        _row("AI_VERIFIED_QUERY", "ORDERS_BY_LOCATION", "VERIFIED_BY", "me"),
        _row("AI_VERIFIED_QUERY", "ORDERS_BY_LOCATION", "ONBOARDING_QUESTION", "TRUE"),
        _row("AI_VERIFIED_QUERY", "BARE", "QUESTION", "Bare?"),
        _row("AI_VERIFIED_QUERY", "BARE", "SQL", "SELECT 1"),
        _row("CUSTOM_INSTRUCTION", None, "AI_SQL_GENERATION", "Round to cents."),
        _row("VARIABLE", "THRESHOLD", "DEFAULT", "10"),
        _row(None, None, "COMMENT", f"Sales. {_MARKER}"),
    )


def test_a_published_view_and_its_render_agree_on_every_member() -> None:
    desired, live = desired_view(_view()), live_view(_described())
    read = [member.key for member in desired.members if live.kinds is None or member.kind in live.kinds]
    assert read == [member.key for member in live.members]
    assert diff_definitions(live, desired) == ()


def test_describe_reads_neither_variables_nor_the_staleness() -> None:
    live = live_view(_described())
    assert live.kinds is not None and not {"variable", "setting"} & live.kinds
    staler = desired_view(_view(variables=(), max_staleness="1 hour"))
    assert diff_definitions(live, staler) == ()


def test_members_are_named_as_a_person_reads_them_in_a_fixed_kind_order() -> None:
    labels = [f"{member.kind} {member.name}" for member in desired_view(_view()).members]
    assert labels == [
        "table orders",
        "table supplies",
        "constraint supplies_distinct_range",
        "relationship orders_asof",
        "relationship orders_to_supplies",
        "fact orders.order_total",
        "dimension orders.order_state",
        "dimension orders.ordered_at",
        "filter orders.is_open",
        "metric orders.order_count",
        "metric orders.running",
        "metric per_order",
        "metric supplies.stock",
        "variable threshold",
        "verified_query bare",
        "verified_query orders_by_location",
        "instruction question_categorization",
        "instruction sql_generation",
        "view comment",
        "setting max_staleness",
    ]


def test_what_describe_has_not_been_seen_to_report_is_left_out_of_both_sides() -> None:
    # A window metric's expression and an ASOF relationship's referenced columns.
    running = next(member for member in desired_view(_view()).members if member.name == "orders.running")
    assert "expression" not in dict(running.properties)
    asof = next(member for member in desired_view(_view()).members if member.name == "orders_asof")
    assert "ref_columns" not in dict(asof.properties)
    view = _view(metrics=(replace(_view().metrics[3], window=Window(order_by=(SortKey(authored("X")),))),))
    assert not [
        item for item in diff_definitions(live_view(_described()), desired_view(view)) if item.name == "orders.running"
    ]


def test_an_edit_shows_as_the_one_member_and_property_it_changes() -> None:
    edited = _view(metrics=(replace(_view().metrics[0], comment="Orders placed."), *_view().metrics[1:]))
    [diff] = diff_definitions(live_view(_described()), desired_view(edited))
    assert (diff.op, diff.label) == (CHANGED, "metric orders.order_count")
    assert [(item.name, item.before, item.after) for item in diff.properties] == [
        ("comment", "Orders.", "Orders placed.")
    ]


def test_a_member_the_scope_now_admits_is_added_and_one_it_drops_is_removed() -> None:
    view = _view(scope=ViewScope(exclude_columns=("ORDERS.ORDER_TOTAL",), exclude_relationships=("HIDDEN",)))
    diffs = diff_definitions(live_view(_described()), desired_view(view))
    assert [(item.op, item.label) for item in diffs] == [
        (ADDED, "dimension orders.secret"),
        (REMOVED, "fact orders.order_total"),
    ]


def test_instructions_and_the_view_comment_compare_their_text() -> None:
    view = _view(ai_question_categorization="Decline names.", comment="Sales, by order.")
    diffs = diff_definitions(live_view(_described()), desired_view(view))
    assert [(item.label, item.properties[0].before, item.properties[0].after) for item in diffs] == [
        ("instruction question_categorization", "", "Decline names."),
        ("view comment", "Sales.", "Sales, by order."),
    ]


def test_a_sort_key_compares_in_full_whatever_was_left_unstated() -> None:
    descending = SortKey(authored("SUPPLIES.VALID_FROM"), descending=True)
    metric = replace(_view().metrics[1], non_additive_by=(descending,))
    rows = tuple(
        replace(row, value='[{"table":"SUPPLIES","dimension":"VALID_FROM","order":"DESCENDING"}]')
        if row.property == "NON_ADDITIVE_DIMENSIONS"
        else row
        for row in _described()
    )
    view = _view(metrics=(_view().metrics[0], metric, *_view().metrics[2:]))
    assert diff_definitions(live_view(rows), desired_view(view)) == ()


def test_a_row_with_no_value_or_unreadable_json_reads_as_empty() -> None:
    rows = (
        _row("TABLE", "T", "BASE_TABLE_NAME", "T"),
        _row("TABLE", "T", "SYNONYMS", "not json"),
        _row("TABLE", "T", "UNIQUE_KEY", '{"a": 1}'),
        DescribedMember("DIMENSION", "D", "T", "SAMPLE_VALUES", None),
        _row("DIMENSION", "D", "UNIQUE_KEY", '["A"]', "T"),
        _row("METRIC", "M", "NON_ADDITIVE_DIMENSIONS", '{"a": 1}', "T"),
    )
    members = {member.key: dict(member.properties) for member in live_view(rows).members}
    assert members["table", "t"]["base_table"] == ""
    assert members["table", "t"]["synonyms"] == ()
    assert members["table", "t"]["unique_keys"] == ()
    assert members["dimension", "t.d"]["sample_values"] == ()
    assert members["metric", "t.m"]["non_additive_by"] == ()
    assert members["view", "comment"] == {"text": ""}


def test_a_unique_key_dbt_lists_flat_still_reads_as_one_key() -> None:
    rows = (_row("TABLE", "T", "UNIQUE_KEY", '["A","B"]'),)
    [table, *_] = live_view(rows).members
    assert dict(table.properties)["unique_keys"] == ("A", "B")
