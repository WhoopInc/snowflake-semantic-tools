"""What spans artifact types, and the prose rules of a custom instruction's two channels."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.eval import EvalDefaults, EvalRunConfig
from snowflake_semantic_tools.domain.model.tool import ToolKind, ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.validate.cross_artifact import (
    agent_eval_problems,
    blocking,
    publish_order,
    reachable_tool_problems,
    shared_questions,
)
from snowflake_semantic_tools.domain.validate.instruction import (
    CATEGORIZATION_CHANNEL,
    SQL_CHANNEL,
    contradicts,
    misplaced_rule,
    state_keywords,
)
from tests.helpers.eval_builders import resolved_eval

ORIGIN = Origin("tools/platform.yml")


def _member(kind: ToolKind, name: str = "t", **fields: object) -> ToolMember:
    return ToolMember(
        "platform",
        name,
        kind.value,
        ToolOwnership.DEFINE,
        ORIGIN,
        "tools/platform.yml",
        **fields,  # type: ignore[arg-type]
    )


def test_a_question_is_shared_only_when_all_three_sets_ask_it() -> None:
    found = shared_questions((" How many? ", "Only here"), ("How many?", ""), ("How many?", "Only here"))
    assert [item.context["value"] for item in found] == ["How many?"]


def test_an_eval_blocks_by_its_own_tier_else_the_default() -> None:
    evaluation = resolved_eval()
    assert not blocking(evaluation, EvalDefaults())
    assert blocking(evaluation, EvalDefaults(eval_tier="BLOCKING"))
    reporting = replace(evaluation, config=replace(evaluation.config, run=EvalRunConfig(tier="report")))
    assert not blocking(reporting, EvalDefaults(eval_tier="blocking"))
    unset = replace(evaluation, config=replace(evaluation.config, run=None))
    assert blocking(unset, EvalDefaults(eval_tier="blocking"))


def test_a_gated_auto_agent_and_a_relative_dated_shared_question_are_reported() -> None:
    evaluation = resolved_eval()
    question = "How many orders last week?"
    rows = (replace(evaluation.dataset.questions[0], question=question),)
    evaluation = replace(evaluation, dataset=replace(evaluation.dataset, questions=rows))
    agent = AgentModel("sales_agent", ORIGIN, ("agent.yml",), sample_questions=(question, "How many orders?"))
    found = agent_eval_problems(agent, evaluation, EvalDefaults(eval_tier="blocking"))
    assert [item.code for item in found] == ["SST-VAL544", "SST-VAL537"]
    pinned = replace(agent, orchestration_model="claude-sonnet-4-6")
    assert [item.code for item in agent_eval_problems(pinned, evaluation, EvalDefaults())] == ["SST-VAL537"]


def test_reachable_tools_report_owner_rights_and_unpinned_search_for_a_gated_agent() -> None:
    agent = AgentModel("router", ORIGIN, ("agent.yml",))
    tools = {
        "tool:owner": _member(ToolKind.PROCEDURE, "owner", execute_as=" OWNER "),
        "tool:caller": _member(ToolKind.PROCEDURE, "caller", execute_as="caller"),
        "tool:search": _member(ToolKind.CORTEX_SEARCH_SERVICE, "search"),
        "tool:pinned": _member(ToolKind.CORTEX_SEARCH_SERVICE, "pinned", embedding_model="e5"),
    }
    keys = ("tool:missing", *tools)
    assert [item.code for item in reachable_tool_problems(agent, keys, tools, gated=True)] == [
        "SST-VAL610",
        "SST-VAL611",
    ]
    assert [item.code for item in reachable_tool_problems(agent, keys, tools, gated=False)] == ["SST-VAL610"]


def test_a_search_service_over_a_relation_created_no_earlier_is_reported() -> None:
    search = _member(ToolKind.CORTEX_SEARCH_SERVICE, "Docs")
    later = {"db.s.docs": "semantic_view:docs"}
    found = publish_order(search, "DB.S.DOCS", later)
    assert found is not None and (found.code, found.subject) == ("SST-VAL614", "tool:docs")
    assert publish_order(search, "DB.S.OTHER", later) is None
    assert publish_order(_member(ToolKind.PROCEDURE), "DB.S.DOCS", later) is None


def test_each_channel_names_the_rule_only_the_other_acts_on() -> None:
    assert misplaced_rule("Decline to answer questions about salaries.", SQL_CHANNEL) == "question categorization"
    assert misplaced_rule("Round to cents.", SQL_CHANNEL) is None
    assert misplaced_rule("Always GROUP BY the region.", CATEGORIZATION_CHANNEL) == "sql generation"
    assert misplaced_rule("Ask for clarification.", CATEGORIZATION_CHANNEL) is None


def test_state_keywords_are_read_once_each_in_order() -> None:
    assert state_keywords("UNCLEAR, then OUT_OF_SCOPE, then UNCLEAR again") == ("UNCLEAR", "OUT_OF_SCOPE")
    assert state_keywords("unclear") == ()


def test_two_directives_contradict_when_one_forbids_what_the_other_directs() -> None:
    assert contradicts("Always round to cents.", "Never round to  cents!")
    assert contradicts("You must not join on email.", "You should join on email.")
    assert not contradicts("Always round to cents.", "Always round to cents.")
    assert not contradicts("Always round to cents.", "Never show salaries.")
