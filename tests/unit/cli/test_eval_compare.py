"""`sst eval --compare-models` and `--refresh-cost` on the command line: what each refuses, writes and prints."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.app.evals.compare import CompareOutcome
from snowflake_semantic_tools.cli.compare import (
    RenderedReportWriter,
    compare_report_writer,
    given_models,
)
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.evaluate.compare_json import report_json
from snowflake_semantic_tools.domain.ports.snowflake.usage import AgentUsage
from tests.helpers.cli_projects import common, compile_project, invoke_with_port
from tests.helpers.compare_reports import JsonReportWriter, jaffle_outcome
from tests.helpers.compare_samples import sample_compare_report
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

MODELS = "claude-opus-4-8,openai-gpt-5"


def run(*args: str) -> Result:
    return CliRunner().invoke(cli, ["-o", "json", "eval", *args])


def refusal(result: Result) -> str:
    assert result.exit_code == 3, result.output
    return str(json.loads(result.stdout)["diagnostics"][0]["message"])


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (("--compare-models", "--capture-baseline", "--reason", "x"), "--compare-models never captures a baseline"),
        (("--compare-models", MODELS, "--unpublished"), "--compare-models compares published versions"),
        (("--refresh-cost", "c.json", "--compare-models"), "--refresh-cost rereads a comparison's cost"),
        (("--refresh-cost", "c.json", "--unpublished"), "--refresh-cost compares published versions"),
        (("--compare-runs", "2"), "--compare-runs sets how --compare-models compares"),
        (("--compare-report", "out"), "--compare-report sets how --compare-models compares"),
    ],
)
def test_a_comparison_refuses_what_it_never_does(args: tuple[str, ...], message: str) -> None:
    assert message in refusal(run(*args))


def test_compare_runs_must_be_at_least_one() -> None:
    assert run("--compare-models", "--compare-runs", "0").exit_code == 3


def test_the_list_names_each_model_once_and_the_flag_alone_names_none() -> None:
    assert given_models(" a, b ,a,") == ("a", "b")
    assert given_models("") is None
    with pytest.raises(SstUsageError, match="names no model"):
        given_models(",")


def compiled(tmp: Path) -> Path:
    project = project_copy(tmp)
    compile_project(project)
    return project


def compare(
    tmp: Path, monkeypatch: pytest.MonkeyPatch, *flags: str, outcome: CompareOutcome | None = None
) -> tuple[Result, Path]:
    project = compiled(tmp)
    chosen = outcome or jaffle_outcome()
    monkeypatch.setattr("snowflake_semantic_tools.app.evals.compare.RunEvalComparison.run", lambda *a, **k: chosen)
    monkeypatch.setattr("snowflake_semantic_tools.cli.compare.compare_report_writer", JsonReportWriter)
    directory = project / "out"
    args = ["-o", "json", "eval", *common(project), "--target", "dev", "--compare-report", str(directory), *flags]
    return invoke_with_port(monkeypatch, FakeSnowflake(state={}), args), directory


def test_a_comparison_writes_compare_json_and_reports_it_as_its_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result, directory = compare(tmp_path, monkeypatch, "--compare-models", MODELS)
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.stdout)
    written = json.loads((directory / "compare.json").read_text(encoding="utf-8"))
    expected = json.loads(json.dumps(report_json(jaffle_outcome().report)))  # type: ignore[arg-type]
    assert envelope["data"] == written == expected
    assert list(written) == ["schema", "generated_at", "cost_read_at", "target", "run_id", "git_sha", "evals"]
    assert [item["code"] for item in envelope["diagnostics"]] == ["SST-VAL773", "SST-VAL774", "SST-VAL775"]
    assert envelope["next_steps"][0].startswith("run `sst eval --refresh-cost out/compare.json`")


def test_a_comparison_whose_versions_did_not_commit_exits_one(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    failed = replace(jaffle_outcome(), passed=False)
    result, _ = compare(tmp_path, monkeypatch, "--compare-models", MODELS, outcome=failed)
    assert result.exit_code == 1
    refused = CompareOutcome(
        DiagnosticBag((D("SST-APL108", artifact="agent:x", detail="it moved", subject="agent:x"),)), None, False
    )
    result, directory = compare(tmp_path / "again", monkeypatch, "--compare-models", MODELS, outcome=refused)
    assert result.exit_code == 1 and not (directory / "compare.json").exists()


def test_a_candidate_the_command_line_names_is_checked_as_a_setting_is(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result, _ = compare(tmp_path, monkeypatch, "--compare-models", "auto")
    assert result.exit_code == 3
    assert json.loads(result.stdout)["diagnostics"][0]["code"] == "SST-VAL770"
    result, _ = compare(tmp_path / "unlisted", monkeypatch, "--compare-models", "llama-9")
    assert json.loads(result.stdout)["diagnostics"][0]["code"] == "SST-VAL771"


def test_a_comparison_with_no_candidate_anywhere_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result, _ = compare(tmp_path, monkeypatch, "--compare-models")
    assert result.exit_code == 3
    assert json.loads(result.stdout)["diagnostics"][0]["code"] == "SST-VAL772"


def test_another_run_holding_the_lock_refuses_the_comparison(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from snowflake_semantic_tools.app.evals.suite import EvalGateRefused

    project = compiled(tmp_path)
    refused = EvalGateRefused("cannot compare models while apply-run holds the target lock")
    monkeypatch.setattr("snowflake_semantic_tools.app.evals.compare.RunEvalComparison.run", lambda *a, **k: refused)
    args = ["-o", "json", "eval", *common(project), "--target", "dev", "--compare-models", MODELS]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)
    assert result.exit_code == 1
    assert json.loads(result.stdout)["data"]["error"] == refused.reason


def test_refreshing_a_report_rewrites_it_with_the_cost_that_landed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    report = jaffle_outcome().report
    assert report is not None
    path = project / "sst_target" / "compare" / "run" / "compare.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(report_json(report)), encoding="utf-8")
    port = FakeSnowflake(state={})
    opus = next(model for model in report.evals[0].models if model.model == "claude-opus-4-8")
    for request in opus.request_ids:
        port.agent_usage_rows[request] = AgentUsage(request, 0.5, 0.0, "2026-10-22T14:00:00+00:00")
    monkeypatch.setattr("snowflake_semantic_tools.cli.compare.compare_report_writer", JsonReportWriter)
    result = invoke_with_port(
        monkeypatch, port, ["-o", "json", "eval", *common(project), "--target", "dev", "--refresh-cost", str(path)]
    )
    assert result.exit_code == 0, result.output
    rewritten = json.loads(path.read_text(encoding="utf-8"))
    [refreshed] = [model for model in rewritten["evals"][0]["models"] if model["model"] == "claude-opus-4-8"]
    assert refreshed["cost"]["agent_credits"] == {"state": "known", "value": 2.0}
    assert rewritten["cost_read_at"] != report.cost_read_at


@pytest.mark.parametrize("content", ["{not json", json.dumps({"schema": "sst.plan/1"})])
def test_refreshing_a_file_sst_did_not_write_is_refused_with_exit_four(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, content: str
) -> None:
    project = project_copy(tmp_path)
    path = project / "c.json"
    path.write_text(content, encoding="utf-8")
    result = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["-o", "json", "eval", *common(project), "--refresh-cost", str(path)]
    )
    assert result.exit_code == 4
    assert json.loads(result.stdout)["diagnostics"][0]["code"] == "SST-MAN032"


def test_the_writer_writes_the_page_and_compare_json(tmp_path: Path) -> None:
    writer = compare_report_writer()
    assert isinstance(writer, RenderedReportWriter)
    page = writer.write(tmp_path / "out", sample_compare_report())
    assert page == tmp_path / "out" / "report.html" and page.read_text(encoding="utf-8").startswith("<!doctype html>")
    assert json.loads((tmp_path / "out" / "compare.json").read_text(encoding="utf-8")) == sample_compare_report()


def test_a_report_goes_under_sst_target_compare_unless_a_directory_is_given(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = compiled(tmp_path)
    outcome = jaffle_outcome()
    monkeypatch.setattr("snowflake_semantic_tools.app.evals.compare.RunEvalComparison.run", lambda *a, **k: outcome)
    monkeypatch.setattr("snowflake_semantic_tools.cli.compare.compare_report_writer", JsonReportWriter)
    args = ["eval", *common(project), "--target", "dev", "--compare-models", MODELS]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)
    assert result.exit_code == 0, result.output
    [written] = (project / "sst_target" / "compare").glob("*/compare.json")
    assert written.parent.name.endswith("Z") and "report  sst_target/compare/" in result.stdout
    outside = tmp_path / "elsewhere"
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), [*args, "--compare-report", str(outside)])
    assert (outside / "compare.json").is_file() and outside.as_posix() in result.stdout
