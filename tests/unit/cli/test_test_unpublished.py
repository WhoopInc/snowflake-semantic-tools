"""`sst test --suite evals --unpublished`: evals run against agents as they compile now, publishing nothing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.compile import compile_result
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply, ToolUse
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cli_projects import common, invoke_with_port
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake


def _unpublished(monkeypatch: pytest.MonkeyPatch, project: Path, port: FakeSnowflake, *args: str) -> Result:
    return invoke_with_port(
        monkeypatch, port, ["test", "--suite", "evals", "--unpublished", *common(project), "--target", "dev", *args]
    )


def _right_tools(project: Path) -> dict[str, AgentReply | object]:
    """A reply to each of the reference eval's questions that calls exactly the tools it expects."""
    compiled = compile_result(project_paths(project), "dev", DBT_MANIFEST)
    [item] = [entry for entry in compiled.compiled if isinstance(entry, CompiledEval)]
    replies: dict[str, AgentReply | object] = {}
    for row in json.loads(item.rendered.dataset_payload):
        tools = [call["tool_name"] for call in row["ground_truth"].get("ground_truth_invocations", [])]
        replies[row["input_query"]] = AgentReply("ok", tuple(ToolUse(name, "") for name in tools))
    return replies


def test_an_agent_that_calls_the_right_tools_passes_with_no_publish_lock_or_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    port = FakeSnowflake(state={})
    port.agent_replies = _right_tools(project)  # type: ignore[assignment]
    for method in ("acquire_run_lock", "write_state", "upload", "execute_script"):
        port.fail(method, SnowflakePortError(f"an unpublished run never calls {method}"))
    result = _unpublished(monkeypatch, project, port, "--output", "json")
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)["data"]
    assert (data["mode"], data["gate_verdict"]) == ("unpublished", "passed")
    assert data["approximated_metrics"] == ["answer_correctness", "answer_grounding"]
    assert [item["eval_key"] for item in data["evals"]] == ["eval:jaffle_analytics_agent"]
    assert len(port.inline_runs) == 4
    # No `sst compile` ran first, and nothing was written beside the project either.
    assert not (project / "target" / "sst").exists()


def test_a_wrong_tool_fails_the_run_on_the_exact_metric(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = FakeSnowflake(state={})
    result = _unpublished(monkeypatch, project_copy(tmp_path), port, "--output", "json")
    assert result.exit_code == 1
    envelope = json.loads(result.output)
    assert "SST-ASK007" in [item["code"] for item in envelope["diagnostics"]]
    assert envelope["data"]["gate_verdict"] == "failed"


def test_the_human_report_names_the_approximate_metrics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    port = FakeSnowflake(state={})
    port.agent_replies = _right_tools(project)  # type: ignore[assignment]
    result = _unpublished(monkeypatch, project, port, "--fail-fast", "--threads", "2")
    assert result.exit_code == 0, result.output
    assert "attempt 1: unpublished COMPLETED" in result.stdout
    assert "approximate, never gating: answer_correctness, answer_grounding" in result.stdout
    assert result.stdout.rstrip().endswith("unpublished eval run passed")


def test_an_eval_scored_only_exactly_names_no_approximate_metric(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    config = project / "agents" / "jaffle_analytics" / "evals" / "config.yml"
    text = config.read_text(encoding="utf-8")
    text = text.replace("    - name: answer_correctness\n      version: v3\n      gate: false\n", "")
    config.write_text(
        text.replace("  custom:\n    - \"{{ eval_metric('answer_grounding') }}\"\n", ""), encoding="utf-8"
    )
    port = FakeSnowflake(state={})
    port.agent_replies = _right_tools(project)  # type: ignore[assignment]
    result = _unpublished(monkeypatch, project, port)
    assert result.exit_code == 0, result.output
    assert "approximate" not in result.stdout and port.judged == []


def test_a_scratch_schema_that_renders_unusable_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    refused = D("SST-CFG008", key="agents.+scratch_schema", found="'.S'", expected="two-part names, DATABASE.SCHEMA")
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.test.agent_scope", lambda agent, target: refused)
    port = FakeSnowflake(state={})
    result = _unpublished(monkeypatch, project_copy(tmp_path), port, "--output", "json")
    assert result.exit_code == 4
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == ["SST-CFG008"]
    assert port.inline_runs == []


def test_with_no_suite_the_unpublished_run_is_the_eval_suite_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    port = FakeSnowflake(state={})
    port.agent_replies = _right_tools(project)  # type: ignore[assignment]
    result = invoke_with_port(
        monkeypatch, port, ["test", "--unpublished", *common(project), "--target", "dev", "--output", "json"]
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)["data"]
    assert (data["suites"], data["skipped_suites"]) == (["evals"], ["smoke"])
    assert data["results"][0]["mode"] == "unpublished"


@pytest.mark.parametrize(
    "flags",
    [
        ("--suite", "evals", "--unpublished", "--capture-baseline", "--reason", "why"),
        ("--suite", "smoke", "--unpublished"),
    ],
)
def test_an_unpublished_run_refuses_a_baseline_capture_and_the_smoke_suite_alone(flags: tuple[str, ...]) -> None:
    assert CliRunner().invoke(cli, ["test", *flags]).exit_code == 3


def test_a_selection_without_an_eval_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = _unpublished(monkeypatch, project_copy(tmp_path), FakeSnowflake(state={}), "--select", "type:tool")
    assert result.exit_code == 4
