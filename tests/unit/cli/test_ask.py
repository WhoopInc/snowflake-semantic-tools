"""`sst ask`: an agent as it compiles now, on scratch copies of your edits, over the REST API, publishing nothing."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from click.testing import Result

from snowflake_semantic_tools.domain.ask.events import AgentEvent, RunError, Status, TextDelta, ToolResult
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply, ToolUse
from snowflake_semantic_tools.domain.ports.snowflake.agent_stream import ThreadSummary
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cli_projects import applied_port, common, compile_project, invoke_with_port
from tests.helpers.reference_project import break_menu_view, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake, failed

AGENT = "jaffle_analytics_agent"


def _ask(
    monkeypatch: pytest.MonkeyPatch, project: Path, port: FakeSnowflake, *args: str, stdin: str | None = None
) -> Result:
    return invoke_with_port(monkeypatch, port, ["ask", *args, *common(project), "--target", "dev"], input=stdin)


def _question(body: object) -> str:
    assert isinstance(body, dict)
    return str(body["messages"][0]["content"][0]["text"])


def test_a_question_runs_on_copies_of_what_is_not_published_and_removes_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    port = FakeSnowflake(state={})
    result = _ask(monkeypatch, project, port, AGENT, "How many orders?", "--output", "json")
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    data = envelope["data"]
    assert (data["answer"], data["status"], data["scope"]) == (
        "answer to How many orders?",
        "completed",
        "SST_REF_DEV.JAFFLE",
    )
    # Nothing is published here, so every view, tool and skill the agent calls is copied.
    assert {"semantic_view:jaffle_sales", "tool:order_status"} <= set(data["copied"])
    created = [statement for script in port.scripts for statement in script if "__SST_ASK_" in statement]
    assert any(
        statement.startswith("CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.JAFFLE_SALES__SST_ASK_")
        for statement in created
    )
    [body] = port.stream_runs
    spec = json.dumps(body)
    assert "JAFFLE_SALES__SST_ASK_" in spec and body["parent_message_id"] == 0
    assert body["orchestration"]["tool_not_accessible"] == "reject"  # type: ignore[index]
    # Every copy made was dropped, the thread went, and no state, lock or ASK001 warning was written.
    made = set(re.findall(r"[\w$.]+__SST_ASK_[0-9A-F]{8}", " ".join(created)))
    dropped = " ".join(statement for script in port.scripts for statement in script if statement.startswith("DROP"))
    assert made and all(name in dropped for name in made)
    assert port.threads == {} and port.deleted_threads
    assert envelope["diagnostics"] == [] and not (project / "target" / "sst").exists()


def test_published_copies_nothing_and_warns_of_each_edit_it_cannot_see(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = FakeSnowflake(state={})
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "q", "--published", "--output", "json")
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["data"]["copied"] == [] and envelope["data"]["published"] is True
    warned = [item for item in envelope["diagnostics"] if item["code"] == "SST-ASK001"]
    assert warned and all(item["severity"] == "warning" for item in warned)
    assert "__SST_ASK_" not in json.dumps(port.stream_runs)


def test_every_dependency_published_as_compiled_is_called_as_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    port = applied_port(project)
    result = _ask(monkeypatch, project, port, AGENT, "Why?", "--output", "json")
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["data"]["copied"] == [] and envelope["diagnostics"] == []


def test_show_tools_prints_each_call_and_result_on_stderr(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake(state={})
    port.agent_streams = {
        "Why?": [
            *_tool("JAFFLE_SALES", "cortex_analyst_text_to_sql"),
            ToolResult("t", "JAFFLE_SALES", "cortex_analyst_text_to_sql", "success", semantic_model="DB.S.V"),
            TextDelta("Because."),
        ]
    }
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "Why?", "--show-tools")
    assert result.exit_code == 0, result.output
    assert "tool_use JAFFLE_SALES (cortex_analyst_text_to_sql)" in result.stderr
    assert "tool_result JAFFLE_SALES: success DB.S.V" in result.stderr
    assert result.stdout.splitlines()[-1] == "Because."


def _tool(name: str, kind: str) -> list[AgentEvent]:
    from snowflake_semantic_tools.domain.ask.events import ToolUse as Use

    return [Use("t", name, kind)]


def test_a_dash_reads_the_question_from_stdin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake(state={})
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "-", "--timeout", "20", stdin="Which location?\n")
    assert result.exit_code == 0, result.output
    assert [_question(body) for body in port.stream_runs] == ["Which location?"]


def test_an_empty_question_or_none_without_a_terminal_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert _ask(monkeypatch, project_copy(tmp_path), FakeSnowflake(), AGENT, "-", stdin="  \n").exit_code == 3
    assert _ask(monkeypatch, project_copy(tmp_path / "bare"), FakeSnowflake(), AGENT).exit_code == 3
    assert _ask(monkeypatch, project_copy(tmp_path / "none"), FakeSnowflake()).exit_code == 3


def test_an_agent_that_answers_with_an_error_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake(state={})
    port.agent_replies = {"q": AgentReply(error="model is not allowed")}
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "q", "--output", "json")
    assert result.exit_code == 1
    envelope = json.loads(result.output)
    assert envelope["data"]["error"] == "model is not allowed"
    assert "SST-ASK002" in [item["code"] for item in envelope["diagnostics"]]
    assert port.threads == {}


def test_a_tool_the_agent_cannot_reach_is_sst_ask010(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake(state={})
    port.agent_streams = {"q": [RunError("TOOL_NOT_ACCESSIBLE: SALES (cortex_analyst_text_to_sql)", "399569", "r-1")]}
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "q", "--output", "json")
    assert result.exit_code == 1
    [diagnostic] = json.loads(result.output)["diagnostics"]
    assert diagnostic["code"] == "SST-ASK010" and "(request r-1)" in diagnostic["message"]


def test_an_unknown_agent_is_refused_before_connecting(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake()
    result = _ask(monkeypatch, project_copy(tmp_path), port, "finance", "q", "--output", "json")
    assert result.exit_code == 4
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == ["SST-ASK003"]
    assert port.stream_runs == [] and port.log == []


def test_a_project_that_does_not_compile_asks_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    break_menu_view(project)
    port = FakeSnowflake()
    assert _ask(monkeypatch, project, port, AGENT, "q").exit_code == 1
    assert port.stream_runs == []


def test_a_refused_privilege_exits_5(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake(state={})
    port.fail("create_thread", SnowflakePortError("Insufficient privileges to operate on schema", sqlstate="42501"))
    assert _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "q").exit_code == 5


def test_a_copy_snowflake_refuses_stops_the_run_and_drops_what_was_made(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = FakeSnowflake(state={})
    port.execute_results = [failed("Insufficient privileges to operate on schema 'JAFFLE'")]
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "q", "--output", "json")
    assert result.exit_code == 1
    [diagnostic] = json.loads(result.output)["diagnostics"]
    assert diagnostic["code"] == "SST-ASK008" and "__SST_ASK_" in diagnostic["message"]
    assert port.stream_runs == []


def test_an_unreadable_state_table_copies_every_dependency(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake(state={})
    port.fail("read_state", SnowflakePortError("cannot read"))
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "q", "--output", "json")
    assert result.exit_code == 0, result.output
    assert "semantic_view:jaffle_sales" in json.loads(result.output)["data"]["copied"]


def test_a_scratch_schema_holds_the_copies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    config = project / "sst_config.yml"
    config.write_text(
        config.read_text(encoding="utf-8").replace(
            "\nagents:\n", '\nagents:\n  +scratch_schema: "{{ target.database }}.SCRATCH"\n', 1
        ),
        encoding="utf-8",
    )
    port = FakeSnowflake(state={})
    result = _ask(monkeypatch, project, port, AGENT, "q", "--output", "json")
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["data"]["scope"] == "SST_REF_DEV.SCRATCH"
    assert "SST_REF_DEV.SCRATCH.JAFFLE_SALES__SST_ASK_" in json.dumps(port.stream_runs)


def test_a_scratch_schema_that_renders_unusable_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    refused = D("SST-CFG008", key="agents.+scratch_schema", found="'.S'", expected="two-part names, DATABASE.SCHEMA")
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.ask.agent_scope", lambda agent, target: refused)
    port = FakeSnowflake(state={})
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "q", "--output", "json")
    assert result.exit_code == 4
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == ["SST-CFG008"]
    assert port.stream_runs == []


def test_cleanup_removes_every_thread_and_copy_left_behind(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake(state={})
    port.stale_threads = (ThreadSummary(7, "sst_ask", 1),)
    result = _ask(monkeypatch, project_copy(tmp_path), port, "--cleanup", "--output", "json")
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)["data"]
    assert data["removed"] == ["thread 7"] and "SST_REF_DEV.JAFFLE" in data["schemas"]
    assert port.deleted_threads == [7]
    human = _ask(monkeypatch, project_copy(tmp_path / "again"), FakeSnowflake(state={}), "--cleanup")
    assert human.exit_code == 0 and "nothing was left behind" in human.stdout


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
    asked = _ask(monkeypatch, project, port, AGENT, "q", "--published", "--output", "json")
    assert asked.exit_code == 0, asked.output
    assert json.loads(asked.output)["data"]["scope"] == "USER$JDOE.PUBLIC"
    validated = invoke_with_port(
        monkeypatch, port, ["validate", *common(project), "--no-snowflake-syntax-check", "--output", "json"]
    )
    assert validated.exit_code == 1
    assert "SST-VAL766" in [item["code"] for item in json.loads(validated.output)["diagnostics"]]


def test_agent_replies_script_the_stream_too(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake(state={})
    port.agent_replies = {"Why?": AgentReply("Because.", (ToolUse("SALES", "cortex_analyst_text_to_sql"),))}
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "Why?", "--output", "json")
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)["data"]
    assert data["answer"] == "Because." and [use["name"] for use in data["tool_uses"]] == ["SALES"]


def test_on_a_terminal_the_answer_streams_as_it_is_generated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.ask._streams_to_terminal", lambda: True)
    port = FakeSnowflake(state={})
    port.agent_streams = {
        "Why?": [
            Status("planning", "Planning the next steps"),
            Status("x", ""),
            TextDelta("Because "),
            TextDelta("so."),
        ]
    }
    result = _ask(monkeypatch, project_copy(tmp_path), port, AGENT, "Why?")
    assert result.exit_code == 0, result.output
    assert "Because so." in result.stdout
    assert "Planning the next steps…" in result.stderr
