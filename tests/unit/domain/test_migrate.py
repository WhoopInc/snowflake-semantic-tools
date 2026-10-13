"""The ref codemod rewrites only the legacy globals, and the classifier is shared."""

from __future__ import annotations

from snowflake_semantic_tools.domain.migrate.refs import (
    FilterSite,
    MigrationResult,
    add_filter_labels,
    migrate_refs,
    rewrite_member_keys,
)
from snowflake_semantic_tools.domain.validate.expression import is_boolean_expression, outer_parentheses, root_function

SOURCE = """semantic_views:
  - name: sales  # a view
    tables:
      - {{ table('orders') }}
      - "{{ table('customers')}}"
    other:
      - {{ table('elsewhere') }}
snowflake_relationships:
  - name: orders_to_customers
    left_table: {{ table('orders') }}
    right_table: "{{ table('customers') }}"  # quoted
    relationship_conditions:
      - "{{ column('orders', 'customer_id') }} = {{column('customers','id')}}"
snowflake_metrics:
  - name: revenue
    tables: [{{ table('orders') }}]
    expr: SUM({{ column('orders', 'amount') }}) + {{ ref('orders', 'tax') }} {{ broken(
    tables:
    - {{ table('orders') }}
note: "{{ table('orders') }} inline"
"""


def test_codemod_rewrites_list_items_relation_keys_and_columns() -> None:
    result = migrate_refs(SOURCE)
    text = result.text
    assert "      - {{ ref('orders') }}\n" in text
    assert "      - \"{{ ref('customers')}}\"\n" in text
    assert "    left_table: \"{{ ref('orders') }}\"\n" in text
    assert "    right_table: \"{{ ref('customers') }}\"  # quoted\n" in text
    assert "{{ ref('orders', 'customer_id') }} = {{ref('customers','id')}}" in text
    assert "tables: [{{ ref('orders') }}]" in text
    assert "SUM({{ ref('orders', 'amount') }}) + {{ ref('orders', 'tax') }}" in text
    assert "    - {{ ref('orders') }}\n" in text
    assert "  - name: sales  # a view\n" in text
    assert [(item.kind, item.line) for item in result.rewrites] == [
        ("ref", 4),
        ("ref", 5),
        ("endpoint", 10),
        ("endpoint", 11),
        ("column", 13),
        ("column", 13),
        ("ref", 16),
        ("column", 17),
        ("ref", 19),
    ]
    assert [(item.line, item.text) for item in result.untouched] == [
        (7, "{{ table('elsewhere') }}"),
        (20, "{{ table('orders') }}"),
    ]
    assert result.changed
    again = migrate_refs(text)
    assert again.text == text and not again.changed


def test_codemod_leaves_clean_text_and_orphan_items_untouched() -> None:
    clean = "tables:\n  - {{ ref('orders') }}\n# {{ table('comment') }} is not code\n"
    result = migrate_refs(clean)
    assert result.text == clean.replace("# {{ table('comment') }}", "# {{ table('comment') }}")
    orphan = migrate_refs("  - {{ table('x') }}\n")
    assert [item.reason for item in orphan.untouched] == ["table() outside a tables: list or a relationship table key"]
    windows = migrate_refs("tables:\r\n  - {{ table('x') }}\r\n")
    assert windows.text == "tables:\r\n  - {{ ref('x') }}\r\n"
    nested = migrate_refs("tables:\n  - name: x\n    parts:\n      - a\n  - {{ table('y') }}\n")
    assert "  - {{ ref('y') }}" in nested.text
    spaced = migrate_refs("tables:\n\n  # the fact table\n  - {{ table('z') }}\n  - {{ table( }}\n")
    assert "  - {{ ref('z') }}\n" in spaced.text and "{{ table( }}" in spaced.text


def test_filter_labels_are_added_only_to_unlabelled_boolean_filters() -> None:
    text = (
        "snowflake_filters:\n"
        "  - name: completed\n"
        "    expr: \"{{ ref('orders', 'state') }} = 'completed'\"\n"
        "  - name: threshold\n"
        '    expr: "1000"\n'
        "  - name: labelled\n"
        '    expr: "TRUE"\n'
        "    labels: [filter]\n"
        "  - name: last\n"
        "    expr: NOT flag"
    )
    sites = (
        FilterSite("completed", "{{ ref('orders', 'state') }} = 'completed'", False, 3, 4),
        FilterSite("threshold", "1000", False, 5, 4),
        FilterSite("labelled", "TRUE", True, 8, 4),
        FilterSite("last", "NOT flag", False, 10, 4),
    )
    result = add_filter_labels(MigrationResult(text), sites)
    assert result.text.count("labels:\n      - filter\n") == 2
    assert "    expr: NOT flag\n    labels:\n      - filter\n" in result.text
    assert '- name: threshold\n    expr: "1000"\n  - name: labelled' in result.text
    assert [item.line for item in result.rewrites] == [4, 11]
    assert add_filter_labels(MigrationResult(""), ()).text == ""


def test_expression_classifier_shapes() -> None:
    assert outer_parentheses("(a)") and not outer_parentheses("(a) + (b)") and not outer_parentheses("a")
    assert outer_parentheses("(')')") is not False or True
    assert outer_parentheses("(')')")
    assert not outer_parentheses("(a")
    assert root_function("((COUNT(x)))") == "COUNT"
    assert root_function("COUNT(x) + 1") is None
    assert root_function("COUNT(x") is None
    assert root_function("x + 1") is None
    assert root_function("F(')', (1))") == "F"
    for predicate in (
        "TRUE",
        "(false)",
        "NOT x",
        "EXISTS (SELECT 1)",
        "BOOLAND(a, b)",
        "a >= 1",
        "x IS NOT NULL",
        "a LIKE 'b'",
    ):
        assert is_boolean_expression(predicate), predicate
    for value in ("1000", "SUM(x)", "x + 1"):
        assert not is_boolean_expression(value), value


def test_codemod_turns_a_bare_endpoint_into_a_ref_call() -> None:
    text = (
        "snowflake_relationships:\n"
        "  - name: a_to_b\n"
        "    left_table: a\n"
        "    right_table: \"{{ ref('b') }}\"\n"
        "    relationship_conditions:\n"
        "      - \"{{ ref('a', 'b_id') }} = {{ ref('b', 'id') }}\"\n"
    )
    result = migrate_refs(text)
    assert "    left_table: \"{{ ref('a') }}\"\n" in result.text
    assert "    right_table: \"{{ ref('b') }}\"\n" in result.text
    assert "{{ ref('a', 'b_id') }} = {{ ref('b', 'id') }}" in result.text
    assert [(item.kind, item.before, item.after) for item in result.rewrites] == [
        ("endpoint", "a", "\"{{ ref('a') }}\"")
    ]
    assert migrate_refs(result.text).rewrites == ()
    # A quoted bare name is rewritten too, and its quotes give way to the call's.
    assert "left_table: \"{{ ref('a') }}\"" in migrate_refs("    left_table: 'a'\n").text


def test_the_keys_sst_infers_are_dropped_from_members_only() -> None:
    text = (
        "semantic_views:\n"
        "  - name: v\n"
        "    tables:\n"
        "      - \"{{ ref('orders') }}\"\n"
        "snowflake_metrics:\n"
        "  - name: total\n"
        "    tables:\n"
        "      - \"{{ ref('orders') }}\"\n"
        "\n"
        "      - \"{{ ref('customers') }}\"\n"
        "    expr: \"SUM({{ ref('orders', 'amount') }})\"\n"
        "  - name: ratio\n"
        "    derived: true  # combines metrics\n"
        "    expr: \"{{ metric('total') }} * 2\"\n"
        "  - tables: [orders]\n"
        "    name: first_key\n"
        "snowflake_filters:\n"
        "  - name: f\n"
        "    tables: [\"{{ ref('orders') }}\"]\n"
        "    expr: x\n"
    )
    result = rewrite_member_keys(migrate_refs(text))
    assert result.text == (
        "semantic_views:\n"
        "  - name: v\n"
        "    tables:\n"
        "      - \"{{ ref('orders') }}\"\n"
        "snowflake_metrics:\n"
        "  - name: total\n"
        "    expr: \"SUM({{ ref('orders', 'amount') }})\"\n"
        "  - name: ratio\n"
        "    expr: \"{{ metric('total') }} * 2\"\n"
        "  - tables: [orders]\n"
        "    name: first_key\n"
        "snowflake_filters:\n"
        "  - name: f\n"
        "    expr: x\n"
    )
    assert [(item.kind, item.line) for item in result.rewrites] == [("inferred", 7), ("inferred", 13), ("inferred", 19)]
    unchanged = rewrite_member_keys(migrate_refs(result.text))
    assert unchanged.text == result.text and not unchanged.changed


def test_a_verified_query_sql_file_becomes_a_file_call() -> None:
    text = (
        "snowflake_verified_queries:\n"
        "  - name: q\n"
        "    sql_file: 'sql/q.sql'  # the sidecar\n"
        "snowflake_metrics:\n"
        "  - name: m\n"
        "    sql_file: untouched\n"
    )
    result = rewrite_member_keys(migrate_refs(text))
    assert "    sql: \"{{ file('sql/q.sql') }}\"  # the sidecar\n" in result.text
    assert "    sql_file: untouched\n" in result.text
    assert [(item.kind, item.line) for item in result.rewrites] == [("file", 3)]
    assert not rewrite_member_keys(migrate_refs(result.text)).changed
