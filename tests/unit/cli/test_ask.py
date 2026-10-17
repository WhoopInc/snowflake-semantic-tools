"""`sst ask`: one question to an agent as it compiles now, through a temporary agent, publishing nothing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import Result

from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.model.identifier import Identifier, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply, ToolResult, ToolUse
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cli_projects import applied_port, common, compile_project, invoke_with_port
from tests.helpers.reference_project import break_menu_view, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

AGENT = "jaffle_analytics_agent"


def _ask(
    monkeypatch: pytest.MonkeyPatch, project: Path, port: FakeSnowflake, *args: str, stdin: str | None = None
) -> Result:
    return invoke_with_port(monkeypatch, port, ["ask", *args, *common(project), "--target", "dev"], input=stdin)


def test_a_question_runs_the_compiled_specification_and_prints_the_answer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    port = FakeSnowflake(state={})
    result = _ask(monkeypatch, project, port, AGENT, "How many orders?", "--output", "json")
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["data"]["answer"] == "answer to How many orders?"
    assert envelope["data"]["scope"] == "SST_REF_DEV.JAFFLE"
    [(scope, agent, spec, question, timeout)] = port.inline_runs
    assert (agent, question, timeout) == (AGENT, "How many orders?", 300)
    assert scope == SchemaScope(Identifier.parse("SST_REF_DEV"), Identifier.parse("JAFFLE"))
    assert json.loads(spec)["models"] == {"orchestration": "claude-sonnet-4-6"}
    # Nothing is published, so every view and tool the agent calls is the published one.
    warned = [item for item in envelope["diagnostics"] if item["code"] == "SST-ASK001"]
    assert warned and all(item["severity"] == "warning" for item in warned)
    # It takes no lock, and writes neither state nor any object.
    assert port.scripts == [] and not (project / "target" / "sst").exists()


def test_every_dependency_published_as_compiled_warns_of_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    result = _ask(monkeypatch, project, applied_port(project), AGENT, "Why?", "--output", "json")
    assert result.exit_code == 0, result.output
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == []


def test_show_tools_prints_each_call_and_result_before_the_answer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = FakeSnowflake(state={})
    port.agent_replies = {
        "Why?": AgentReply(
            "Because.",
            (ToolUse("JAFFLE_SALES", "cortex_analyst_text_to_sql", {"q": "x"}),),
            (ToolResult("JAFFLE_SALES", "success"),),
        )
    }
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "Why?", "--show-tools")
    assert result.exit_code == 0, result.output
    lines = result.stdout.splitlines()
    assert lines[-3:] == [
        'tool_use JAFFLE_SALES (cortex_analyst_text_to_sql): {"q": "x"}',
        "tool_result JAFFLE_SALES: success",
        "Because.",
    ]
    quiet = _ask(monkeypatch, project_copy(tmp_path / "again"), port, AGENT, "Why?")
    assert quiet.stdout.splitlines()[-1] == "Because." and "tool_use" not in quiet.stdout


def test_a_dash_reads_the_question_from_stdin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake(state={})
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "-", "--timeout", "20", stdin="Which location?\n")
    assert result.exit_code == 0, result.output
    assert [(question, timeout) for *_, question, timeout in port.inline_runs] == [("Which location?", 20)]


def test_an_empty_question_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = _ask(monkeypatch, project_copy(tmp_path), FakeSnowflake(), AGENT, "-", stdin="  \n")
    assert result.exit_code == 3


def test_an_agent_that_answers_with_an_error_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake(state={})
    port.agent_replies = {"q": AgentReply(error="model is not allowed")}
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "q", "--output", "json")
    assert result.exit_code == 1
    envelope = json.loads(result.output)
    assert envelope["data"]["error"] == "model is not allowed"
    assert "SST-ASK002" in [item["code"] for item in envelope["diagnostics"]]


def test_an_unknown_agent_is_refused_before_connecting(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake()
    result = _ask(monkeypatch, project_copy(tmp_path), port, "finance", "q", "--output", "json")
    assert result.exit_code == 4
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == ["SST-ASK003"]
    assert port.inline_runs == [] and port.log == []


def test_a_project_that_does_not_compile_asks_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    break_menu_view(project)
    port = FakeSnowflake()
    assert _ask(monkeypatch, project, port, AGENT, "q").exit_code == 1
    assert port.inline_runs == []


def test_a_refused_privilege_exits_5(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake(state={})
    port.agent_replies = {"q": SnowflakePortError("Insufficient privileges to operate on schema", sqlstate="42501")}
    assert _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "q").exit_code == 5


def test_an_unreadable_state_table_still_asks_and_warns_of_every_dependency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = FakeSnowflake(state={})
    port.fail("read_state", SnowflakePortError("cannot read"))
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "q", "--output", "json")
    assert result.exit_code == 0, result.output
    details = {item["params"]["detail"] for item in json.loads(result.output)["diagnostics"]}
    assert details == {"the target's state could not be read, so whether it is published as compiled is unknown"}


def test_a_scratch_schema_holds_the_temporary_agent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    config = project / "sst_config.yml"
    config.write_text(
        config.read_text(encoding="utf-8").replace(
            "\nagents:\n", '\nagents:\n  +scratch_schema: "{{ target.database }}.SCRATCH"\n', 1
        ),
        encoding="utf-8",
    )
    port = FakeSnowflake(state={})
    assert _ask(monkeypatch, project, port, AGENT, "q").exit_code == 0
    assert port.inline_runs[0][0] == SchemaScope(Identifier.parse("SST_REF_DEV"), Identifier.parse("SCRATCH"))


def test_a_scratch_schema_that_renders_unusable_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    refused = D("SST-CFG008", key="agents.+scratch_schema", found="'.S'", expected="two-part names, DATABASE.SCHEMA")
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.ask.agent_scope", lambda agent, target: refused)
    port = FakeSnowflake(state={})
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "q", "--output", "json")
    assert result.exit_code == 4
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == ["SST-CFG008"]
    assert port.inline_runs == []


def test_an_agent_in_a_personal_database_can_be_asked_though_its_eval_cannot_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    config = project / "sst_config.yml"
    personal = '\nagents:\n  +database: "USER$JDOE"\n  +schema: PUBLIC\n'
    text = config.read_text(encoding="utf-8").replace("\nagents:\n", personal, 1)
    config.write_text(
        text.replace('  +database: "{{ target.database }}"\n  +schema: JAFFLE\n  +enabled', "  +enabled", 1),
        encoding="utf-8",
    )
    port = FakeSnowflake(state={})
    asked = _ask(monkeypatch, project, port, AGENT, "q")
    assert asked.exit_code == 0, asked.output
    assert port.inline_runs[0][0] == SchemaScope(Identifier.parse("USER$JDOE"), Identifier.parse("PUBLIC"))
    validated = invoke_with_port(
        monkeypatch, port, ["validate", *common(project), "--no-snowflake-syntax-check", "--output", "json"]
    )
    assert validated.exit_code == 1
    assert "SST-VAL766" in [item["code"] for item in json.loads(validated.output)["diagnostics"]]
