"""`sst ask`'s use case: finding the agent, where it runs, what it cannot see, and its reply."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.app.ask import (
    AskAgent,
    agent_scope,
    compiled_agents,
    find_agent,
    reply_json,
    stale_dependencies,
)
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import Identifier, SchemaScope, TargetIdentity
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply, ToolResult, ToolUse
from snowflake_semantic_tools.domain.state import DEACTIVATED, AppliedEntry
from tests.helpers.ask_builders import ScriptedAgentRun, applied_entry, compiled_agent

TARGET = TargetIdentity("dev", "acct", Identifier.parse("DB"), Identifier.parse("S"), "ROLE", "WH")


def test_an_agent_is_found_by_its_name_or_its_key_casefolded() -> None:
    sales, ops = compiled_agent("sales"), compiled_agent("ops")
    result = CompileResult((sales, ops))
    assert compiled_agents(result) == (sales, ops)
    assert find_agent(result, "SALES") is sales
    assert find_agent(result, "agent:ops") is ops


def test_an_unknown_agent_names_those_that_compile() -> None:
    missing = find_agent(CompileResult((compiled_agent("sales"), compiled_agent("ops"))), "finance")
    assert isinstance(missing, Diagnostic) and missing.code == "SST-ASK003"
    assert missing.message.endswith("it compiles ops, sales")
    nothing = find_agent(CompileResult(()), "finance")
    assert isinstance(nothing, Diagnostic) and nothing.message.endswith("it compiles no agent")


def test_the_temporary_agent_runs_in_the_agents_schema_unless_a_scratch_schema_is_set() -> None:
    assert agent_scope(compiled_agent("sales"), TARGET) == SchemaScope(
        Identifier.parse("db"), Identifier.parse("agents")
    )
    scratch = compiled_agent("sales", settings={"scratch_schema": "{{ target.database }}.SCRATCH"})
    assert agent_scope(scratch, TARGET) == SchemaScope(Identifier.parse("DB"), Identifier.parse("SCRATCH"))
    blank = compiled_agent("sales", settings={"scratch_schema": ""})
    assert agent_scope(blank, TARGET) == SchemaScope(Identifier.parse("db"), Identifier.parse("agents"))


def test_a_scratch_schema_a_template_renders_unusable_is_refused() -> None:
    odd = TargetIdentity("dev", "acct", Identifier.parse('"my db"'), Identifier.parse("S"))
    refused = agent_scope(compiled_agent("sales", settings={"scratch_schema": "{{ target.database }}.S"}), odd)
    assert isinstance(refused, Diagnostic) and refused.code == "SST-CFG008"
    assert refused.subject == "agent:sales"


@pytest.mark.parametrize(
    ("applied", "reason"),
    [
        (None, "the target's state could not be read, so whether it is published as compiled is unknown"),
        ({}, "it is not published to this target"),
        ("deactivated", "it is not published to this target"),
        ("other fingerprint", "what is published differs from what compiles now"),
        ("other target", "what is published differs from what compiles now"),
    ],
)
def test_each_dependency_the_target_does_not_hold_as_compiled_is_named(applied: object, reason: str) -> None:
    view = compiled_agent("helper")
    agent = compiled_agent("sales", view.artifact_key, "semantic_view:not_compiled")
    entries: dict[str, AppliedEntry] | None
    if applied == "deactivated":
        entries = {view.artifact_key: applied_entry(view, outcome=DEACTIVATED)}
    elif applied == "other fingerprint":
        entries = {view.artifact_key: applied_entry(view, fingerprint="f" * 64)}
    elif applied == "other target":
        entries = {view.artifact_key: applied_entry(view, target="DB.ELSEWHERE.HELPER")}
    else:
        entries = applied  # type: ignore[assignment]
    [warning] = stale_dependencies(agent, CompileResult((agent, view)), entries)
    assert warning.code == "SST-ASK001" and warning.subject == "agent:sales"
    assert (
        warning.message == f"agent:sales calls the published agent:helper (DB.AGENTS.HELPER), not your edit: {reason}"
    )


def test_a_dependency_published_as_compiled_is_not_named() -> None:
    view = compiled_agent("helper")
    agent = compiled_agent("sales", view.artifact_key)
    assert stale_dependencies(agent, CompileResult((agent, view)), {view.artifact_key: applied_entry(view)}) == ()


def test_a_question_runs_the_compiled_specification_and_reports_the_reply() -> None:
    agent = compiled_agent("sales")
    scope = SchemaScope(Identifier.parse("DB"), Identifier.parse("S"))
    port = ScriptedAgentRun(
        {
            "How many?": AgentReply(
                "12 orders", (ToolUse("SALES", "generic", {"q": 1}),), (ToolResult("SALES", "success"),)
            )
        }
    )
    note = stale_dependencies(compiled_agent("x", "agent:sales"), CompileResult((agent,)), None)
    outcome = AskAgent(port).run(agent, "How many?", scope=scope, timeout=30, notes=note)
    assert port.asked == [(scope, "sales", "{}\n", "How many?", 30)]
    assert outcome.answered and [item.code for item in outcome.diagnostics] == ["SST-ASK001"]
    assert reply_json(outcome.reply) == {
        "answer": "12 orders",
        "tool_uses": [{"name": "SALES", "type": "generic", "input": {"q": 1}}],
        "tool_results": [{"name": "SALES", "status": "success"}],
        "error": None,
        "duration_ms": 0,
    }


def test_an_error_reply_is_reported() -> None:
    scope = SchemaScope(Identifier.parse("DB"), Identifier.parse("S"))
    outcome = AskAgent(ScriptedAgentRun({"q": AgentReply(error="model not allowed")})).run(
        compiled_agent("sales"), "q", scope=scope
    )
    assert not outcome.answered
    [failed] = outcome.diagnostics
    assert (failed.code, failed.message) == ("SST-ASK002", "agent:sales returned an error: model not allowed")
