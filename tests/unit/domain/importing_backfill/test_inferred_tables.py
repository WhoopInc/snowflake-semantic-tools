"""`sst migrate refs` naming a member's listed tables: what it rewrites, and what it leaves for a person."""

from __future__ import annotations

from snowflake_semantic_tools.domain.migrate.inferred_tables import inferred_tables
from tests.helpers.migrate_outline import applied, lines, outline


def _run(text: str) -> tuple[str, list[str], frozenset[int]]:
    changes, kept = inferred_tables(lines(text), outline(text))
    return applied(text, changes), [item.reason for item in changes.untouched], kept


def _metric(tables: str, expr: str) -> str:
    return f"snowflake_metrics:\n  - name: m\n    tables: {tables}\n    expr: {expr}\n"


def _query(tables: str, sql: str) -> str:
    return f"snowflake_verified_queries:\n  - name: q\n    tables: {tables}\n    sql: {sql}\n"


def test_a_tables_list_naming_something_other_than_a_model_is_left_as_it_is() -> None:
    for tables in ("[\"{{ source('raw', 'events') }}\"]", '["{{ ref( }}"]'):
        text = _metric(tables, "SUM(amount)")
        assert _run(text) == (text, [], frozenset())


def test_a_table_listed_twice_counts_once_so_a_bare_column_takes_it() -> None:
    text = _metric("[orders, ORDERS]", "SUM(amount) + o.qty")
    after, reasons, kept = _run(text)
    assert reasons == [] and kept == frozenset()
    assert "expr: \"SUM({{ ref('orders', 'amount') }}) + o.qty\"" in after


def test_a_column_after_as_is_an_alias_and_not_rewritten() -> None:
    after, _, _ = _run(_metric("[orders]", "amount AS total"))
    assert "expr: \"{{ ref('orders', 'amount') }} AS total\"" in after


def test_sql_kept_in_a_file_keeps_its_tables_and_is_reported() -> None:
    text = _query("[orders]", "\"{{ file('q.sql') }}\"")
    assert _run(text) == (
        text,
        ["its SQL is in a file; name orders there with ref(), then delete tables:"],
        frozenset({2}),
    )


def test_an_expression_that_does_not_lex_keeps_its_tables_and_is_reported() -> None:
    text = _metric("[orders]", '"\'open"')
    assert _run(text)[1] == ["its expression does not lex as SQL; name orders with ref() by hand"]


def test_a_plain_scalar_over_several_lines_or_an_escaped_one_is_left_for_a_person() -> None:
    reason = "its expression is a multi-line or escaped scalar; name orders with ref() by hand"
    assert _run(_metric("[orders]", "SUM(a)\n      + b"))[1] == [reason]
    assert _run(_metric("[orders]", '"SUM(a)\\tb"'))[1] == [reason]


def test_a_block_whose_lines_do_not_spell_its_value_in_order_is_left_for_a_person() -> None:
    text = "snowflake_metrics:\n  - name: m\n    tables: [orders]\n    expr: |2\n        a\n      b\n"
    assert _run(text)[1] == ["its expression is a multi-line or escaped scalar; name orders with ref() by hand"]


def test_a_literal_block_with_a_blank_line_is_rewritten_in_place() -> None:
    text = "snowflake_metrics:\n  - name: m\n    tables: [orders]\n    expr: |\n      SUM(a)\n\n      + b\n"
    after, reasons, kept = _run(text)
    assert reasons == [] and kept == frozenset()
    assert after.endswith("expr: |\n      SUM({{ ref('orders', 'a') }})\n\n      + {{ ref('orders', 'b') }}\n")


def test_an_expression_with_a_broken_template_names_no_table_and_is_reported() -> None:
    text = _metric("[orders]", '"{{ broken( }} + amount"')
    assert _run(text)[1] == ["its expression names orders by no column or FROM; write it with ref()"]


def test_a_verified_query_names_each_from_and_join_table_but_not_a_column_or_a_template() -> None:
    sql = '"SELECT customers.id FROM customers JOIN analytics.marts.orders o WHERE {{ customers() }} > 0"'
    after, reasons, kept = _run(_query("[orders, customers]", sql))
    assert reasons == [] and kept == frozenset()
    assert (
        "sql: \"SELECT customers.id FROM {{ ref('customers') }} JOIN {{ ref('orders') }} o"
        ' WHERE {{ customers() }} > 0"'
    ) in after


def test_a_from_name_that_is_qualified_further_is_not_the_table_and_the_count_hint_is_given() -> None:
    reasons = _run(_query("[orders]", '"SELECT * FROM orders.archive"'))[1]
    assert reasons == [
        "its SQL names orders by no column or FROM; write it with COUNT({{ ref('orders', '<primary key>') }})"
    ]
