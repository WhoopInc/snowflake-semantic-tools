"""The render-phase checks every renderer runs before its output is published."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin, RegistryIntegrityError
from snowflake_semantic_tools.domain.model.agent import AgentModel, ResolvedAgentTool
from snowflake_semantic_tools.domain.model.eval import EvalGroundTruth, EvalInvocation, EvalQuestion
from snowflake_semantic_tools.domain.model.semantic_view import Metric, Table
from snowflake_semantic_tools.domain.model.skill import Plugin, Skill, SkillFile
from snowflake_semantic_tools.domain.model.tool import ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.render.agent import agent_render_checks, render_agent_json
from snowflake_semantic_tools.domain.render.eval import eval_render_checks
from snowflake_semantic_tools.domain.render.invariants import dollar_quote_offset, statement_size, unquotable
from snowflake_semantic_tools.domain.render.semantic_view import render, render_checked
from snowflake_semantic_tools.domain.render.skill_bundle import (
    SKILL_MD_BUDGET_BYTES,
    build_plugin_bundle,
    build_skill_bundle,
)
from snowflake_semantic_tools.domain.render.tool import tool_render_checks
from snowflake_semantic_tools.domain.resolve.ref_fields import check_ref_policies, ref_field
from snowflake_semantic_tools.domain.resolve.template import FILTER_EXPR, METRIC_EXPR
from tests.helpers.compile_builders import view
from tests.helpers.eval_builders import ORIGIN, resolved_eval
from tests.helpers.sql_values import authored


def test_the_invariants_find_unquotable_names_dollar_quotes_and_byte_sizes() -> None:
    assert (unquotable("A\x00"), unquotable("A\ud800"), unquotable("Plain Name")) == (True, True, False)
    assert (dollar_quote_offset("a $$ b"), dollar_quote_offset("a $ b")) == (2, None)
    assert statement_size("é") == 2


def test_a_checked_view_renders_what_render_does_and_reports_every_unquotable_name() -> None:
    plain = view("V")
    ddl, found = render_checked(plain)
    assert (ddl, found) == (render(plain), ())
    metric = Metric(name='"M\x00"', expr=authored("SUM(T.C)"), table="T")
    broken = replace(plain, tables=(Table(logical_name='"T\x00"', fqn="DB.SCH.T"),), metrics=(metric,))
    ddl, found = render_checked(broken)
    assert ddl is None and [item.code for item in found] == ["SST-RND002", "SST-RND002"]
    _, missing = render_checked(replace(plain, tables=()))
    assert [item.context["field"] for item in missing] == ["tables"]


def test_the_agent_checks_report_each_generic_tool_with_resources() -> None:
    model = AgentModel("router", Origin("agent.yml"), ("agent.yml",))
    generic = ResolvedAgentTool("generic", "lookup", "Look.", MappingProxyType({"identifier": "X"}))
    bare = ResolvedAgentTool("generic", "bare", "Bare.")
    tools = (generic, bare)
    assert [item.code for item in agent_render_checks(model, tools, render_agent_json(model, tools))] == ["SST-RND013"]


def test_the_eval_checks_read_every_question_and_skip_one_without_ground_truth() -> None:
    value = resolved_eval()
    sql = EvalInvocation(ORIGIN, tool_input="with x as (select 1) select * from x")
    questions = (
        EvalQuestion(ORIGIN, "No truth", None),
        EvalQuestion(ORIGIN, "Prose", EvalGroundTruth(ORIGIN, None, "Answer", extra={"ground_truth_output": "x"})),
        EvalQuestion(ORIGIN, "Query", EvalGroundTruth(ORIGIN, (sql, EvalInvocation(ORIGIN)), "Answer")),
    )
    found = eval_render_checks(replace(value, dataset=replace(value.dataset, questions=questions)))
    assert [(item.code, item.context.get("index")) for item in found] == [("SST-RND022", 2)]


def test_the_skill_checks_hold_the_flattened_skill_md_to_its_budget_only_once() -> None:
    def skill(*files: SkillFile) -> Skill:
        return Skill("close", "skills/close", "close", "Close.", "Body.\n", files, Origin("SKILL.md"))

    empty, found = build_skill_bundle(skill())
    assert empty is None and "SST-RND030" not in [item.code for item in found]
    head = b"---\nname: close\n---\n"
    within = skill(SkillFile("SKILL.md", head + b"x" * (SKILL_MD_BUDGET_BYTES - len(head))))
    assert "SST-RND032" not in [item.code for item in build_skill_bundle(within)[1]]
    plugin = Plugin("kit", "plugins/kit", "plugins/kit/plugin.yml", "Kit.", "Data", ("close",), Origin("plugin.yml"))
    broken = Skill(
        "close", "skills/close", "close", None, "", (SkillFile("a/b.md", b""), SkillFile("a__b.md", b"")), Origin("x")
    )
    bundle, plugin_found = build_plugin_bundle(plugin, {"close": broken})
    assert bundle is None and [item.code for item in plugin_found] == ["SST-VAL836"]


@pytest.mark.parametrize(
    ("kind", "fields", "relation", "codes"),
    [
        ("agent", {}, None, ["SST-RND040"]),
        ("function", {"body": None}, None, ["SST-RND041"]),
        ("function", {"body": "SELECT 1"}, None, []),
        ("stage", {}, None, []),
    ],
)
def test_the_tool_checks_refuse_what_render_tool_cannot_render(
    kind: str, fields: dict[str, object], relation: object, codes: list[str]
) -> None:
    member = ToolMember("g", "lookup", kind, ToolOwnership.DEFINE, Origin("t.yml"), "t.yml", **fields)  # type: ignore[arg-type]
    assert [item.code for item in tool_render_checks(member, None)] == codes


def test_ref_fields_bind_each_field_to_its_policy_and_refuse_a_policy_both_bound_and_explained() -> None:
    assert (ref_field("filter.expression"), ref_field("metric.window")) == (FILTER_EXPR, METRIC_EXPR)
    with pytest.raises(RegistryIntegrityError, match="also explained as unbound"):
        check_ref_policies({"metric_expr": METRIC_EXPR}, {"metric.expression": "metric_expr"}, {"metric_expr": "why"})
    check_ref_policies({"metric_expr": METRIC_EXPR}, {}, {"metric_expr": "no field uses it yet"})


def test_every_render_finding_is_reported_on_its_own_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    from snowflake_semantic_tools.domain.render import semantic_view

    model = AgentModel("router", Origin("agent.yml"), ("agent.yml",))
    found = agent_render_checks(model, (), '{"x": "$$"}')
    assert [item.code for item in found] == ["SST-RND010", "SST-RND011"]
    value = resolved_eval()
    assert [item.code for item in eval_render_checks(replace(value, dataset=replace(value.dataset, questions=())))] == [
        "SST-RND021"
    ]
    _, large = render_checked(replace(view("V"), comment="x" * 1_100_000))
    assert [item.code for item in large] == ["SST-RND003"]
    monkeypatch.setattr(semantic_view, "MEMBER_INDEX", MappingProxyType({"fact": lambda item: (1, 2, 3)}))
    ddl, mismatch = render_checked(view("V"))
    assert ddl is None and [item.code for item in mismatch] == ["SST-RND900"]


def test_a_bundle_names_no_path_it_does_not_hold() -> None:
    def skill(*files: SkillFile) -> Skill:
        return Skill("close", "skills/close", "close", "Close.", "Body.\n", files, Origin("SKILL.md"))

    _, missing = build_skill_bundle(skill(SkillFile("notes.md", b"x")))
    assert "SST-RND030" in [item.code for item in missing]
    head = b"---\nname: close\n---\n"
    refs = b"[a](scripts/a.md)\n" * 10
    grown = skill(
        SkillFile("SKILL.md", head + b"x" * (SKILL_MD_BUDGET_BYTES - len(head) - len(refs)) + refs),
        SkillFile("scripts/a.md", b"A.\n"),
    )
    assert "SST-RND032" in [item.code for item in build_skill_bundle(grown)[1]]
    plugin = Plugin("kit", "plugins/kit", "plugins/kit/plugin.yml", "Kit.", "Data", ("ghost",), Origin("plugin.yml"))
    assert [item.code for item in build_plugin_bundle(plugin, {})[1]] == ["SST-RND030"]


def test_ref_policies_refuse_an_undeclared_binding_and_an_unexplained_policy() -> None:
    with pytest.raises(RegistryIntegrityError, match="is not declared"):
        check_ref_policies({}, {"metric.expression": "metric_expr"}, {})
    with pytest.raises(RegistryIntegrityError, match="states no reason"):
        check_ref_policies({"metric_expr": METRIC_EXPR}, {}, {"metric_expr": "  "})
