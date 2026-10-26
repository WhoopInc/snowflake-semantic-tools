"""Small CLI helpers at the edges the broader suites skip: settings, prompts, flags, logs, and stderr."""

from __future__ import annotations

import io
from collections.abc import Iterator
from pathlib import Path

import click
import pytest

from snowflake_semantic_tools.adapters.json_files import JsonFileError
from snowflake_semantic_tools.cli import confirm
from snowflake_semantic_tools.cli import output as _output
from snowflake_semantic_tools.cli.envelope import announce_interrupt, json_envelope
from snowflake_semantic_tools.cli.globals import SstCommand, StrictBool
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.help_form import _FULL_ASKED
from snowflake_semantic_tools.cli.help_text import command_docs
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.options import with_model_paths
from snowflake_semantic_tools.cli.output import (
    RenderPolicy,
    record_reported,
    render_diagnostics,
    reported_diagnostics,
    start_invocation,
    use_render_policy,
)
from snowflake_semantic_tools.cli.plan_output import print_changed_names
from snowflake_semantic_tools.cli.policy import run_is_strict
from snowflake_semantic_tools.cli.render.terminal import PLAIN
from snowflake_semantic_tools.cli.run_log import RUN_LOG, signature_report
from snowflake_semantic_tools.cli.settings import apply_fail_fast
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, cascade
from snowflake_semantic_tools.domain.model.lifecycle import Action
from tests.helpers.artifact_builders import change, changeset, rendered
from tests.helpers.projects import project_paths


@pytest.fixture
def plain() -> Iterator[None]:
    use_render_policy(RenderPolicy(terminal=PLAIN))
    yield
    use_render_policy(RenderPolicy(terminal=PLAIN))


def test_the_signature_report_skips_lines_that_name_no_code(tmp_path: Path) -> None:
    (tmp_path / RUN_LOG).write_text(
        '{"code": "SST-SNO001"}\n[1, 2]\n{"code": 7}\nnot json\n{"code": "SST-SNO002"}\n', encoding="utf-8"
    )
    report = signature_report(tmp_path)
    assert (report["matched"], report["unmatched"], report["sno001_rate"]) == (1, 1, 0.5)


def test_a_run_log_that_is_not_utf8_cannot_be_read(tmp_path: Path) -> None:
    (tmp_path / RUN_LOG).write_bytes(b'{"code": "\xff"}\n')
    with pytest.raises(JsonFileError):
        signature_report(tmp_path)


def test_the_fail_fast_flag_wins_over_the_configuration(tmp_path: Path) -> None:
    (tmp_path / "sst_config.yml").write_text("apply:\n  fail_fast: true\n", encoding="utf-8")
    paths = project_paths(tmp_path)
    assert apply_fail_fast(paths, False) is False
    assert apply_fail_fast(paths, None) is True


def test_a_strict_command_with_no_configuration_file_is_not_strict(tmp_path: Path) -> None:
    paths = project_paths(tmp_path)
    assert paths.config_file is None
    assert run_is_strict("validate", paths, None) is False


def test_ask_returns_what_was_typed_or_the_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO("semantic\n\n"))
    assert confirm.ask("Semantic models directory", "models") == "semantic"
    assert confirm.ask("Semantic models directory", "models") == "models"


class _Closed(io.StringIO):
    def isatty(self) -> bool:
        raise ValueError("I/O operation on closed file")


def test_a_closed_or_absent_stream_is_not_a_terminal() -> None:
    assert confirm._is_terminal(None) is False
    assert confirm._is_terminal(_Closed()) is False


@pytest.mark.parametrize(("text", "expected"), [("on", True), ("OFF", False), (" no ", False), (True, True)])
def test_a_strict_boolean_reads_each_accepted_spelling(text: object, expected: bool) -> None:
    assert StrictBool().convert(text, None, None) is expected


def test_a_strict_boolean_refuses_any_other_spelling() -> None:
    with pytest.raises(click.BadParameter, match="is not a boolean"):
        StrictBool().convert("maybe", None, None)


@pytest.mark.parametrize("full", [False, True])
def test_a_command_with_no_help_and_no_options_prints_its_usage_alone(full: bool) -> None:
    command = SstCommand("bare", callback=lambda: None, add_help_option=False)
    with click.Context(command, info_name="bare") as ctx:
        ctx.meta[_FULL_ASKED] = full
        text = command.get_help(ctx)
    assert text.startswith("Usage: bare")
    assert "Options" not in text and "Global options" not in text


def test_reported_diagnostics_are_the_default_until_a_run_records_them() -> None:
    start_invocation(["sst", "validate"])
    default = DiagnosticBag((D("SST-CFG001", path="sst_config.yml"),))
    assert reported_diagnostics(default) is default
    recorded = DiagnosticBag((D("SST-CFG018", key="x"),))
    record_reported(recorded)
    assert reported_diagnostics(default) is recorded
    start_invocation(["sst", "validate"])


def test_quiet_output_leaves_no_blank_line_between_findings_or_after_a_collapsed_group(
    plain: None, capsys: pytest.CaptureFixture[str]
) -> None:
    use_render_policy(RenderPolicy(terminal=PLAIN, quiet=True))
    many = [D("SST-PRT005", value=f"DB.S.T{index}", detail="absent") for index in range(5)]
    render_diagnostics(DiagnosticBag((*many, D("SST-CFG001", path="sst_config.yml"))))
    lines = capsys.readouterr().err.splitlines()
    assert "" not in lines
    assert any("SST-PRT005" in line and "2" in line and "more" in line for line in lines), lines


def test_a_cascade_whose_cause_is_not_shown_is_counted_under_no_finding(
    plain: None, capsys: pytest.CaptureFixture[str]
) -> None:
    orphan = cascade(D("SST-MEM107", member="m", count=1, subject="semantic_view:v"), "SST-REF001")
    render_diagnostics(DiagnosticBag((D("SST-CFG001", path="sst_config.yml"), orphan)))
    err = capsys.readouterr().err
    assert "SST-CFG001" in err and "SST-MEM107" not in err


def test_the_envelope_reads_an_option_written_with_an_equals_sign() -> None:
    start_invocation(["sst", "validate", "--target=prod", "--project-dir=/somewhere"])
    invocation = json_envelope("validate", DiagnosticBag())["invocation"]
    assert isinstance(invocation, dict) and invocation["target"] == "prod"
    assert invocation["project_dir"] == str(Path("/somewhere").resolve())


def test_an_envelope_with_no_recorded_command_line_names_the_command_alone() -> None:
    _output._INVOCATION.clear()
    invocation = json_envelope("validate", DiagnosticBag())["invocation"]
    assert isinstance(invocation, dict)
    assert (invocation["argv"], invocation["target"]) == (["sst", "validate"], None)
    start_invocation(["sst", "validate"])


def test_an_interrupt_notice_the_render_shows_anyway_is_not_printed_twice(
    plain: None, capsys: pytest.CaptureFixture[str]
) -> None:
    announce_interrupt(D("SST-CFG001", path="sst_config.yml"))
    assert capsys.readouterr().err.splitlines() == ["Aborted."]


def test_a_model_path_outside_the_project_is_named_in_full(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    paths = with_model_paths(project_paths(project), elsewhere, None)
    assert paths.model_paths == (str(elsewhere.resolve()),)


def test_a_usage_error_without_a_context_prints_the_refusal_alone(capsys: pytest.CaptureFixture[str]) -> None:
    SstUsageError("--select conflicts with the saved plan selection").show()
    err = capsys.readouterr().err
    assert not err.startswith("Usage:") and "SST-PRT100" in err


def test_a_usage_error_without_machine_output_propagates_to_click() -> None:
    with pytest.raises(SstUsageError, match="No such option"):
        cli.main(["validate", "--no-such-flag"], standalone_mode=False)


def test_the_command_reference_leaves_out_a_hidden_command() -> None:
    group = click.Group("sst", commands=[click.Command("shown", help="Shown."), click.Command("secret", hidden=True)])
    assert [doc.path for doc in command_docs(group)] == ["sst shown"]


def test_only_written_changes_are_listed_by_name(capsys: pytest.CaptureFixture[str]) -> None:
    plan = changeset(change(rendered("A")), change(rendered("B"), Action.NOOP), change(rendered("C"), Action.UPDATE))
    print_changed_names(plan)
    assert capsys.readouterr().out.splitlines() == ["a", "c"]
