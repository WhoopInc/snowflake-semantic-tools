"""The semantic content rules called directly: files, prose, hardcoded names, questions and fan-out."""

from __future__ import annotations

from typing import Any

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.authored import FilterDef, InstructionDef, MetricDef, VerifiedQueryDef
from snowflake_semantic_tools.domain.model.project import ParsedMember, ParsedProject, ParsedView
from snowflake_semantic_tools.domain.parse.template import scan_template_calls
from snowflake_semantic_tools.domain.resolve.membership import ViewMembers
from snowflake_semantic_tools.domain.validate.semantic.fanout import attachment_diagnostics
from snowflake_semantic_tools.domain.validate.semantic.files import file_diagnostics, formatting_problem
from snowflake_semantic_tools.domain.validate.semantic.nodes import list_of, load_nodes, node_origin, node_root
from snowflake_semantic_tools.domain.validate.semantic.rules import rule_diagnostics
from snowflake_semantic_tools.domain.validate.semantic.text import (
    hardcoded_name_diagnostics,
    hardcoded_names,
    overlap_diagnostics,
    unresolved_prose_diagnostics,
)
from snowflake_semantic_tools.domain.validate.semantic.verified_queries import (
    duplicate_question_diagnostics,
    relative_date_diagnostics,
)
from tests.helpers.authored_documents import ORIGIN, Document, document, documents

METRICS = node_root("metric")
VIEWS = node_root("semantic_view")


def _view(name: str = "v", **source: Any) -> ParsedView:
    return ParsedView(name, ORIGIN, "semantic_models/views.yml", source, ("orders",))


def _query(name: str, question: str, sql: str = "SELECT 1") -> VerifiedQueryDef:
    return VerifiedQueryDef(name, question, sql, ("orders",), None, None, None, origin=ORIGIN)


def _filter(name: str, expr: str) -> FilterDef:
    return FilterDef(name, expr, None, ("orders",), False, origin=ORIGIN)


def _member(source: object, type_name: str, name: str, *, poisoned: bool = False, expr: str = "") -> ParsedMember:
    return ParsedMember(type_name, name, ORIGIN, source, ("orders",), scan_template_calls(expr), poisoned)


def test_the_node_walk_reads_mapping_entries_with_their_positions() -> None:
    assert list_of("x") == [] and list_of([1]) == [1]
    assert node_root("semantic_view") == VIEWS
    tree = {METRICS: [{"name": "m", "description": "two\nlines "}, "not a node"]}
    file = document("m.yml", tree, ((METRICS, 0), 4), ((METRICS, 0, "expr"), 5))
    empty = document("e.yml", {METRICS: None})
    other = document("o.yml", {VIEWS: []})
    [(found, index, node)] = load_nodes(documents(file, empty, other), METRICS)
    assert (found, index, node["description"]) == (file, 0, "two lines")
    assert node_origin(file, METRICS, 0) == Origin("m.yml", 4, 1)
    assert node_origin(file, METRICS, 0, "expr") == Origin("m.yml", 5, 1)
    assert node_origin(file, METRICS, 1) == Origin("m.yml")


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ("a: 1\r\n", "a carriage return"),
        ("a: 1 \n", "trailing whitespace"),
        ("a:\n\t- 1\n", "a tab in its indentation"),
        ("a: 1", "no single final newline"),
        ("a: 1\n\n", "no single final newline"),
        ("", None),
        ("a:\n  b: '\t'\n", None),
    ],
)
def test_a_file_is_canonical_with_lf_no_tabs_or_trailing_space_and_one_final_newline(
    text: str, problem: str | None
) -> None:
    assert formatting_problem(text) == problem


def test_each_folded_scalar_of_a_node_is_reported_with_its_field() -> None:
    text = "\n".join(
        (
            f"{METRICS}:",  # 1
            "  - name: m",  # 2
            "    description: >-",  # 3
            "    window:",  # 4
            "      order_by: >",  # 5 -- `order_by[0].ref` starts here, deeper than `order_by`
            "    expr: >",  # 6 -- a node with no name is named by its index
            "  - >",  # 7 -- the node itself, not a field of it
            "other: >",  # 8 -- not a semantic-model root
            "    note: >",  # 9 -- a node beyond the list
            "    gone: >",  # 10 -- recorded on no node path
            "    odd: >",  # 11 -- a position with no line
            "",
        )
    )
    tree = {METRICS: [{"name": "m"}, {"expr": "x"}], "other": "y", "x": {"k": "v"}}
    file = Document(
        "m.yml",
        tree,
        {
            **document(
                "m.yml",
                tree,
                ((METRICS, 0, "description"), 3),
                ((METRICS, 0, "window", "order_by", 0, "ref"), 5),
                ((METRICS, 0, "window", "order_by"), 5),
                ((METRICS, 1, "expr"), 6),
                ((METRICS, 0), 7),
                (("other", 0, "k"), 8),
                ((METRICS, 9, "note"), 9),
                (("x", "k", "v"), 12),
            ).line_index,
            (METRICS, 3, "odd"): object(),  # type: ignore[dict-item]
        },
    )
    unread = document("unread.yml", {})
    found = file_diagnostics(documents(file, unread), {"m.yml": text})
    assert [(item.code, item.context["name"], item.context["field"]) for item in found] == [
        ("SST-VAL008", "m", "description"),
        ("SST-VAL008", "m", "window.order_by[0].ref"),
        ("SST-VAL008", "1", "expr"),
        ("SST-VAL008", "9", "note"),
    ]
    beside = Document("m.yml", {METRICS: "not a list"}, file.line_index)
    assert [
        item.context["name"] for item in file_diagnostics(documents(beside), {"m.yml": "    description: >-\n"})
    ] == []
    assert [
        item.context["name"] for item in file_diagnostics(documents(beside), {"m.yml": "\n\n    description: >-\n"})
    ] == ["0"]
    assert [item.code for item in file_diagnostics(documents(unread), {"unread.yml": "a: 1"})] == ["SST-VAL009"]


def test_a_three_part_name_outside_templates_strings_and_comments_is_hardcoded() -> None:
    text = "SELECT a.b.c, \"X\".\"Y\".z.w FROM {{ ref('d.e.f') }} -- g.h.i\nWHERE n = 'j.k.l' /* m.n.o */ AND a.b.c > 0"
    assert hardcoded_names(text) == ("a.b.c", '"X"."Y".z.w')
    views = (_view(tables=["DB.S.ORDERS", "{{ ref('orders') }}"]), _view("w", tables="DB.S.T"))
    metric = MetricDef("m", "SUM(DB.S.T.X)", None, (), origin=ORIGIN)
    found = hardcoded_name_diagnostics(
        views, (metric,), (_filter("f", "DB.S.T.Y > 0"),), (_query("q", "?", "SELECT * FROM DB.S.T"),)
    )
    assert [(item.subject, item.context["field"], item.context["value"]) for item in found] == [
        ("semantic_view:v", "tables[0]", "DB.S.ORDERS"),
        ("metric:m", "expr", "DB.S.T.X"),
        ("filter:f", "expr", "DB.S.T.Y"),
        ("verified_query:q", "sql", "DB.S.T"),
    ]


def test_an_instruction_that_writes_out_a_filters_predicate_overlaps_it() -> None:
    filters = (
        _filter("is_done", "{{ ref('orders', 'state') }} = '{{ var('done') }}'"),
        _filter("is_new", "{{ ref('orders', 'state') }} = '{{ var('fresh') }}'"),
        _filter("tagged", "{{ ref('orders', 'tag') }} like 'x%'"),
        _filter("plain", "{{ ref('orders', 'total') }}"),
    )
    instructions = {
        "a": InstructionDef("A", "Only count rows where STATE  = 'complete'.", None, origin=ORIGIN),
        "b": InstructionDef("B", None, "Use tag like 'x%' for tags.", origin=ORIGIN),
        "c": InstructionDef("C", None, None, origin=ORIGIN),
    }
    found = overlap_diagnostics(filters, instructions, {"done": "complete"})
    assert [(item.context["name"], item.context["other"]) for item in found] == [
        ("A", "filter 'is_done'"),
        ("B", "filter 'tagged'"),
    ]


def test_prose_naming_a_metric_or_filter_the_project_lacks_is_unresolved() -> None:
    instructions = {
        "b": InstructionDef("B", "Prefer the net_revenue metric and the is_done filter.", None, origin=ORIGIN),
        "a": InstructionDef("A", None, "Never use the gross_revenue metric or the is_old filter.", origin=ORIGIN),
    }
    found = unresolved_prose_diagnostics(instructions, frozenset(("net_revenue",)), frozenset(("is_done",)))
    assert [(item.context["name"], item.context["value"]) for item in found] == [
        ("A", "metric gross_revenue"),
        ("A", "filter is_old"),
    ]


def test_verified_queries_are_checked_for_the_clock_and_for_a_question_asked_twice() -> None:
    queries = (_query("now", "?", "SELECT CURRENT_DATE()"), _query("then", "?", "SELECT 'CURRENT_DATE()'"))
    assert [item.subject for item in relative_date_diagnostics(queries)] == ["verified_query:now"]
    first = _member(_query("first", "How many  orders?"), "verified_query", "first")
    second = _member(_query("second", "how many orders?"), "verified_query", "second")
    other = _member(_query("other", "Something else?"), "verified_query", "other")
    stray = _member(object(), "verified_query", "stray")
    metric = _member(MetricDef("m", "1", None, ()), "metric", "m")
    attachment = {
        "verified_query:first": ("semantic_view:b", "semantic_view:a"),
        "verified_query:second": ("semantic_view:a",),
        "verified_query:other": ("semantic_view:a",),
        "verified_query:stray": ("semantic_view:a",),
        "metric:m": ("semantic_view:a",),
    }
    [found] = duplicate_question_diagnostics((first, second, other, stray, metric), attachment)
    assert (found.code, found.subject, found.context["a"], found.context["b"]) == (
        "SST-VAL413",
        "semantic_view:a",
        "first",
        "second",
    )
    assert found.related == (ORIGIN,)
    unplaced = _member(
        VerifiedQueryDef("x", "How many orders?", "SELECT 1", (), None, None, None), "verified_query", "x"
    )
    [found] = duplicate_question_diagnostics(
        (first, unplaced), {**attachment, "verified_query:x": ("semantic_view:b",)}
    )
    assert found.related == (ORIGIN,)


def test_an_authored_member_attaching_to_no_view_is_reported_unless_poison_explains_it() -> None:
    members = (
        _member(object(), "metric", "placed"),
        _member(object(), "metric", "alone"),
        _member(object(), "metric", "broken", poisoned=True),
        _member(object(), "metric", "on_broken", expr="{{ metric('Broken') }}"),
        _member(object(), "metric", "on_nothing", expr="{{ metric() }}"),
        _member(object(), "column", "col"),
    )
    found = attachment_diagnostics((), members, {"metric:placed": ("semantic_view:v",)})
    assert [item.context["name"] for item in found] == ["alone", "on_nothing"]


def test_the_rule_runner_reads_its_settings_and_reports_short_metric_descriptions() -> None:
    def run(config: dict[str, Any], *metrics: MetricDef) -> list[str]:
        parsed = ParsedProject((), tuple(_member(metric, "metric", metric.name) for metric in metrics))
        found = rule_diagnostics(documents(), {}, parsed, {}, config, ViewMembers({}, {}), {})
        return [item.code for item in found]

    short = MetricDef("short", "1", "Tiny.", (), origin=ORIGIN)
    bare = MetricDef("bare", "1", None, (), origin=ORIGIN)
    assert run({"validation": {"description_floor": 40}}, short, bare) == ["SST-VAL004"]
    assert run({"validation": {"description_floor": True}}, short) == []
    assert run({"validation": {"description_floor": 0}}, short) == []
    assert run({"validation": "no"}, short) == []
    filters = (_filter("is_done", "{{ ref('orders', 'state') }} = '{{ var('done') }}'"),)
    instruction = InstructionDef("Tone", "Rows where state = 'complete'.", None, origin=ORIGIN)
    members = (
        *(_member(item, "filter", item.name) for item in filters),
        _member(instruction, "custom_instruction", "Tone"),
    )
    parsed = ParsedProject((), members)
    found = rule_diagnostics(documents(), {}, parsed, {}, {"vars": {"done": "complete"}}, ViewMembers({}, {}), {})
    assert [item.code for item in found] == ["SST-VAL017"]
    assert rule_diagnostics(documents(), {}, parsed, {}, {"vars": ["done"]}, ViewMembers({}, {}), {}) == ()
