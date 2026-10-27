"""The migrate use cases rewrite and split files in memory and report what they changed."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.outline import outline
from snowflake_semantic_tools.app.migrate_refs import KINDS, MigrateRefs, RefsFiles
from snowflake_semantic_tools.app.migrate_split import MigrateSplit, SplitFiles
from snowflake_semantic_tools.domain.migrate.refs import FilterSite

GROUP = "tools:\n  - group: g\n    immutable: true\n    reference:\n      - name: r\n        type: stage\n"


def counts(**found: int) -> dict[str, int]:
    return {kind: found.get(kind, 0) for kind in KINDS}


def test_migrate_refs_rewrites_counts_and_reports_untouched_calls() -> None:
    files = {
        "b.yml": "tables:\n  - {{ table('orders') }}\nnote: \"{{ table('orders') }}\"\n",
        "a.yml": "snowflake_filters:\n  - name: done\n    expr: \"{{ column('orders', 'state') }} = 'x'\"\n",
        "c.yml": "clean: true\n",
    }
    seen: list[str] = []

    def locate(text: str, path: str) -> tuple[FilterSite, ...]:
        seen.append(path)
        if path != "a.yml":
            return ()
        return (FilterSite("done", "{{ ref('orders', 'state') }} = 'x'", False, 3, 4),)

    report = MigrateRefs(RefsFiles(files), locate, outline).run()
    assert seen == ["a.yml", "b.yml", "c.yml"]
    assert [item.path for item in report.changed] == ["a.yml", "b.yml"]
    assert report.changed[0].counts() == counts(column=1, labels=1)
    assert report.changed[1].counts() == counts(ref=1)
    assert report.untouched == 1
    assert "labels:\n      - filter\n" in report.changed[0].result.text


def test_migrate_refs_reads_every_kind_of_file() -> None:
    files = RefsFiles(
        semantic={"s/v.yml": "semantic_view:\n  name: v\n  enabled: true\n  metrics: [m]\n", "s/bad.yml": "a: [\n"},
        tools={
            "tools/t.yml": "tool:\n  name: t\n  type: cortex_search_service\n  on: \"{{ ref('m') }}\"\n  id_column: i\n"
        },
        agents={
            "agents/a/agent.yml": "name: a\nalias: x\nspec:\n  tools:\n    - name: t\n      type: cortex_search\n"
            "      search_service: \"{{ tool('g', 't') }}\"\n      id_column: i\n",
            "agents/b/agent.yml": "- a list\n",
        },
        evals={"agents/a/evals/config.yml": "agent: a\nname: c\n"},
        config={"sst_config.yml": "x: \"{{ tool('g', 't') }}\"\n"},
    )
    report = MigrateRefs(files, lambda text, path: (), outline).run()
    by_path = {item.path: item for item in report.files}
    assert by_path["s/v.yml"].counts() == counts(reference=1, setting=1)
    assert by_path["tools/t.yml"].counts() == counts(tool_column=1)
    assert by_path["agents/a/agent.yml"].counts() == counts(tool=1, tool_column=1, setting=1)
    assert not by_path["agents/b/agent.yml"].result.changed and not by_path["s/bad.yml"].result.changed
    assert by_path["agents/a/evals/config.yml"].counts() == counts(removed=1)
    assert by_path["sst_config.yml"].counts() == counts(tool=1)


def test_migrate_split_plans_each_file_refuses_overwrites_and_rewrites_the_configuration() -> None:
    files = SplitFiles(
        semantic={
            "s/metrics.yml": "snowflake_metrics:\n  - name: a\n  - name: b\n",
            "s/filters.yml": "snowflake_filters:\n  - name: a\n",
            "s/one.yml": "metric:\n  name: c\n",
            "s/views.yml": "semantic_views:\n  - name: taken\n",
            "s/bad.yml": "a: [\n",
            "s/list.yml": "- a\n",
        },
        tools={
            "tools/g.yml": GROUP,
            "tools/bad.yml": "x: [\n",
        },
        config=("sst_config.yml", "tools:\n  +schema: S\n"),
    )
    report = MigrateSplit(files, outline, {"s/taken.yml"}.__contains__).run()
    assert [(plan.path, plan.refused) for plan in report.plans] == [
        ("s/filters.yml", None),
        ("s/metrics.yml", "another list is split into s/a.yml too"),
        ("s/views.yml", "s/taken.yml already exists"),
        ("tools/g.yml", None),
    ]
    assert [plan.path for plan in report.splits] == ["s/filters.yml", "tools/g.yml"]
    assert [plan.path for plan in report.refused] == ["s/metrics.yml", "s/views.yml"]
    assert report.changed and report.config is not None
    assert report.config[1].text == "tools:\n  +schema: S\n  g:\n    +immutable: true\n"


def test_a_split_without_configuration_or_with_unreadable_configuration() -> None:
    empty = MigrateSplit(SplitFiles(semantic={}), outline, lambda path: False).run()
    assert empty.config is None and not empty.changed
    broken = MigrateSplit(SplitFiles(semantic={}, config=("c.yml", "a: [\n")), outline, lambda path: False).run()
    assert broken.config is not None and not broken.config[1].changed and not broken.changed
    claimed = MigrateSplit(
        SplitFiles(
            semantic={"a/x.yml": "snowflake_metrics:\n  - name: m\n", "a/y.yml": "snowflake_filters:\n  - name: m\n"}
        ),
        outline,
        lambda path: False,
    ).run()
    assert [plan.refused for plan in claimed.plans] == [None, "another list is split into a/m.yml too"]
    # A profile stage under skills: has no 1.0 form to move to, so it is reported, not rewritten.
    moved = MigrateSplit(
        SplitFiles(semantic={}, config=("c.yml", "skills:\n  stage:\n    +stage: S\n")), outline, lambda path: False
    ).run()
    assert moved.config is not None and not moved.changed
    assert [(item.text, item.reason) for item in moved.config[1].untouched] == [
        ("skills.stage", "delete it, and move each profile to coco_profiles/")
    ]


def test_migrate_refs_writes_a_0_3_2_view_scope_in_the_1_0_form() -> None:
    source = (
        "semantic_view:\n  name: v\n  tables: [\"{{ table('orders') }}\"]\n"
        "  dimensions:\n    - status\n    - \"{{ column('orders', 'total') }}\"\n"
        "  exclude_columns: [orders.notes]\n"
    )
    report = MigrateRefs(RefsFiles({"v.yml": source}), lambda text, path: (), outline).run()
    [migrated] = report.files
    assert migrated.result.text == (
        "semantic_view:\n  name: v\n  tables: [\"{{ ref('orders') }}\"]\n"
        "  columns:\n    - \"{{ ref('orders', 'status') }}\"\n    - \"{{ ref('orders', 'total') }}\"\n"
        "  exclude_columns: [\"{{ ref('orders', 'notes') }}\"]\n"
    )
    assert migrated.counts() == counts(ref=1, column=1, reference=2, setting=1) and report.untouched == 0


ORPHANS = """snowflake_metrics:
  - name: upgrades
    tables:
      - {{ table('orders') }}
    expr: SUM(CASE WHEN is_upgrade = TRUE THEN amount::NUMBER ELSE 0 END)
  - name: order_count
    tables:
      - {{ table('orders') }}
    expr: COUNT(*)
  - name: mixed
    tables:
      - {{ table('orders') }}
      - {{ table('items') }}
    expr: "{{ metric('upgrades') }} / SUM({{ column('orders', 'amount') }})"
snowflake_filters:
  - name: recent
    tables: [orders]
    expr: order_date >= DATEADD(day, -7, CURRENT_DATE())
snowflake_verified_queries:
  - name: q
    question: How many?
    tables:
      - {{ table('orders') }}
      - {{ table('items') }}
    sql: |
      SELECT COUNT(*)
      FROM orders o
      JOIN items AS i ON i.order_id = o.id
      WHERE o.x = 'FROM orders' AND o.y IN (SELECT y FROM other.orders)
"""


def test_migrate_refs_names_each_listed_table_before_dropping_tables_or_keeps_the_list() -> None:
    report = MigrateRefs(RefsFiles({"m.yml": ORPHANS}), lambda text, path: (), outline).run()
    [migrated] = report.files
    assert migrated.result.text == (
        "snowflake_metrics:\n  - name: upgrades\n"
        "    expr: \"SUM(CASE WHEN {{ ref('orders', 'is_upgrade') }} = TRUE"
        " THEN {{ ref('orders', 'amount') }}::NUMBER ELSE 0 END)\"\n"
        "  - name: order_count\n    tables:\n      - {{ ref('orders') }}\n    expr: COUNT(*)\n"
        "  - name: mixed\n"
        "    expr: \"{{ metric('upgrades') }} / SUM({{ ref('orders', 'amount') }})\"\n"
        "snowflake_filters:\n  - name: recent\n"
        "    expr: \"{{ ref('orders', 'order_date') }} >= DATEADD(day, -7, CURRENT_DATE())\"\n"
        "snowflake_verified_queries:\n  - name: q\n    question: How many?\n    sql: |\n"
        "      SELECT COUNT(*)\n      FROM {{ ref('orders') }} o\n"
        "      JOIN {{ ref('items') }} AS i ON i.order_id = o.id\n"
        "      WHERE o.x = 'FROM orders' AND o.y IN (SELECT y FROM {{ ref('orders') }})\n"
    )
    left = {item.text: item.reason for item in migrated.result.untouched}
    assert set(left) == {"order_count"}
    assert "COUNT({{ ref('orders', '<primary key>') }})" in left["order_count"]


def test_migrate_refs_keeps_tables_when_the_sql_is_in_a_file_or_an_escaped_scalar() -> None:
    source = (
        "snowflake_verified_queries:\n  - name: q\n    tables: [orders]\n    sql_file: q.sql\n"
        '  - name: r\n    tables: [orders]\n    sql: "SELECT \\"x\\" FROM orders"\n'
    )
    [migrated] = MigrateRefs(RefsFiles({"v.yml": source}), lambda text, path: (), outline).run().files
    assert migrated.result.text.count("tables: [orders]") == 2
    assert {item.text for item in migrated.result.untouched} == {"q", "r"}


def test_migrate_refs_names_a_qualified_table_in_folded_sql_and_leaves_a_derived_metric() -> None:
    source = (
        "snowflake_verified_queries:\n  - name: q\n    tables: [orders]\n    sql: >\n"
        "      SELECT x\n      FROM ANALYTICS.CORE.ORDERS AS o\n      WHERE o.y = 1\n"
        "snowflake_metrics:\n  - name: share\n    tables: [orders]\n"
        "    expr: \"{{ metric('a') }} / {{ metric('b') }}\"\n"
    )
    [migrated] = MigrateRefs(RefsFiles({"v.yml": source}), lambda text, path: (), outline).run().files
    assert migrated.result.text == (
        "snowflake_verified_queries:\n  - name: q\n    sql: >\n"
        "      SELECT x\n      FROM {{ ref('orders') }} AS o\n      WHERE o.y = 1\n"
        "snowflake_metrics:\n  - name: share\n"
        "    expr: \"{{ metric('a') }} / {{ metric('b') }}\"\n"
    )
    assert migrated.result.untouched == ()


def test_migrate_refs_keeps_two_tables_an_expression_names_by_bare_columns() -> None:
    source = "snowflake_metrics:\n  - name: m\n    tables: [orders, items]\n    expr: SUM(amount)\n"
    [migrated] = MigrateRefs(RefsFiles({"m.yml": source}), lambda text, path: (), outline).run().files
    assert migrated.result.text == source
    [left] = migrated.result.untouched
    assert left.text == "m" and "lists 2 tables" in left.reason
