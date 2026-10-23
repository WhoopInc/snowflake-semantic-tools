"""`sst eval --unpublished`: evals run against agents as they compile now, publishing nothing."""

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
from tests.helpers.reference_project import DBT_MANIFEST, project_copy, with_profile_eval
from tests.helpers.snowflake_fake import FakeSnowflake


def _unpublished(monkeypatch: pytest.MonkeyPatch, project: Path, port: FakeSnowflake, *args: str) -> Result:
    return invoke_with_port(monkeypatch, port, ["eval", "--unpublished", *common(project), "--target", "dev", *args])


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
    for method in ("acquire_run_lock", "write_state"):
        port.fail(method, SnowflakePortError(f"an unpublished run never calls {method}"))
    result = _unpublished(monkeypatch, project, port, "--output", "json")
    assert result.exit_code == 0, result.output
    # Nothing is published to this target, so the agent runs on scratch copies of what it calls,
    # each made and then dropped: every statement names a copy, and every upload goes to one's stage.
    statements = [statement for script in port.scripts for statement in script]
    assert statements and all("__SST_ASK_" in statement for statement in statements)
    assert all("__SST_ASK_" in sent.statements[0] for sent in port.log if sent.kind == "upload")
    assert len([item for item in statements if item.startswith("DROP")]) == len(
        [item for item in statements if item.startswith("CREATE")]
    )
    assert "__SST_ASK_" in port.inline_runs[0][2]
    data = json.loads(result.output)["data"]
    assert (data["mode"], data["gate_verdict"]) == ("unpublished", "passed")
    assert data["approximated_metrics"] == ["answer_correctness", "answer_grounding"]
    assert [item["eval_key"] for item in data["evals"]] == ["eval:jaffle_analytics_agent"]
    assert len(port.inline_runs) == 4
    # No `sst compile` ran first, and nothing was written beside the project either.
    assert not (project / "sst_target").exists()


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
    assert "eval:jaffle_analytics_agent  unpublished  COMPLETED  4/4 questions passed\n" in result.stdout
    assert "├── answer_correctness       4/4 passed  average 1.000  approximate, never gates\n" in result.stdout
    # A judged metric's miss fails no question, so only -v lists them.
    assert "answer_grounding         0/4 passed  average 1.000  needs at least 3  approximate, never gates" in (
        result.stdout
    )
    assert "not measured: agent tokens, LLM calls, judge tokens" in result.stdout
    assert "└── all 4 questions passed  (-v lists them)\n" in result.stdout
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
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.eval.agent_scope", lambda agent, target: refused)
    port = FakeSnowflake(state={})
    result = _unpublished(monkeypatch, project_copy(tmp_path), port, "--output", "json")
    assert result.exit_code == 4
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == ["SST-CFG008"]
    assert port.inline_runs == []


@pytest.mark.parametrize(
    "flags",
    [
        ("--unpublished", "--capture-baseline", "--reason", "why"),
        ("--unpublished", "--compare-models"),
        ("--unpublished", "--refresh-cost", "compare.json"),
    ],
)
def test_an_unpublished_run_refuses_a_baseline_capture_and_a_comparison(flags: tuple[str, ...]) -> None:
    assert CliRunner().invoke(cli, ["eval", *flags]).exit_code == 3


def test_a_selection_without_an_eval_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = _unpublished(monkeypatch, project_copy(tmp_path), FakeSnowflake(state={}), "--select", "type:tool")
    assert result.exit_code == 4


def test_a_cowork_profile_eval_is_refused_before_anything_connects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = with_profile_eval(project_copy(tmp_path))
    port = FakeSnowflake(state={})
    result = _unpublished(monkeypatch, project, port, "--output", "json")
    assert result.exit_code == 1, result.output
    refused = [item for item in json.loads(result.output)["diagnostics"] if item["code"] == "SST-ASK013"]
    assert [item["artifact"] for item in refused] == [{"type": "eval", "name": "cowork_profile:jaffle_concierge"}]
    assert (port.inline_runs, port.scripts) == ([], [])


def test_leaving_the_profile_eval_out_runs_the_agent_evals_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = with_profile_eval(project_copy(tmp_path / "evaluated"))
    port = FakeSnowflake(state={})
    port.agent_replies = _right_tools(project_copy(tmp_path / "plain"))  # type: ignore[assignment]
    exclude = ("--exclude", "eval:cowork_profile:jaffle_concierge", "--output", "json")
    result = _unpublished(monkeypatch, project, port, *exclude)
    assert result.exit_code == 0, result.output
    assert [item["eval_key"] for item in json.loads(result.output)["data"]["evals"]] == ["eval:jaffle_analytics_agent"]
