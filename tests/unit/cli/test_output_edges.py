"""The edges of the redesigned output: debug's dbt checks, every way to ask for agent output, and stderr."""

from __future__ import annotations

import io
import json
import logging
import sys
from collections.abc import Iterator
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun
from snowflake_semantic_tools.cli import output
from snowflake_semantic_tools.cli.commands import debug
from snowflake_semantic_tools.cli.group import _machine_output
from snowflake_semantic_tools.cli.logs import configure_logging
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.output import RenderPolicy, emit_agent, interrupted, use_render_policy
from snowflake_semantic_tools.cli.render.agent import agent_document
from snowflake_semantic_tools.cli.render.progress import ProgressDisplay
from snowflake_semantic_tools.cli.render.report import outcome_lines, plan_lines, table
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.render.terminal import PLAIN, Terminal
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.diagnostics.baseline import stable_fingerprint
from snowflake_semantic_tools.domain.model.lifecycle import Action, ApplyOutcome, OwnershipMarker
from snowflake_semantic_tools.domain.model.lifecycle.apply import ClassifiedError, ErrorKind, OutcomeStatus
from tests.helpers import artifact_builders as builders
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.seam_projects import FakeDbt


@pytest.fixture
def policy() -> Iterator[None]:
    use_render_policy(RenderPolicy(terminal=PLAIN))
    yield
    use_render_policy(RenderPolicy(terminal=PLAIN))


@pytest.mark.parametrize(
    ("version", "expected", "fails"),
    [
        (OSError(2, "No such file or directory"), None, "dbt: No such file or directory"),
        (CompletedRun(1, "", "boom"), None, "`dbt --version` exited 1"),
        (CompletedRun(0, "", ""), None, None),
        (CompletedRun(0, "dbt 2.0.8\n", ""), "2.0.8", None),
        (CompletedRun(0, "dbt Cloud CLI - 0.38.6 (abc)\n", ""), "0.38.6", None),
        (CompletedRun(0, "something else\n", ""), "something else", None),
    ],
)
def test_debug_reads_the_dbt_version_and_says_why_it_cannot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, version: object, expected: str | None, fails: str | None
) -> None:
    monkeypatch.setattr(debug, "dbt_runner", FakeDbt(version=version))  # type: ignore[arg-type]
    found, _, reason = debug._dbt_version(tmp_path)
    assert (found, reason) == (expected, fails)


def test_debug_fails_when_the_dbt_it_runs_cannot_be_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(debug, "dbt_runner", FakeDbt(version=OSError(2, "No such file or directory")))
    project = project_copy(tmp_path)
    human = CliRunner().invoke(cli, ["debug", "--project-dir", str(project), "--no-connect"])
    assert human.exit_code == 1 and "error[SST-DBT027]" in human.output
    assert "cannot be run" in human.stdout
    machine = CliRunner().invoke(cli, ["debug", "--project-dir", str(project), "--no-connect", "-o", "json"])
    assert [item["code"] for item in json.loads(machine.stdout)["diagnostics"] if item["severity"] == "error"] == [
        "SST-DBT027"
    ]


def test_debug_without_a_configuration_still_reports_what_it_found(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text("name: x\nprofile: x\n")
    result = CliRunner().invoke(
        cli, ["debug", "--project-dir", str(tmp_path), "--no-connect", "--manifest", str(DBT_MANIFEST)]
    )
    assert result.exit_code == 4 and "no sst_config.yml found" in result.stdout


def test_debug_lists_the_agent_kit_and_a_failed_connection() -> None:
    data = {
        "versions": {"sst": "1.0"},
        "config": {"file": None},
        "manifest": {"source": "--manifest", "path": "m.json", "supported": None},
        "agent_kit": {"agents": ["claude"], "installed_by": "0.9", "matches_sst": False},
        "connection": {"tested": True, "ok": False, "error": "refused"},
    }
    rows = debug._checks(data)
    assert ("agent kit", False, "installed by SST 0.9 for claude") in rows
    assert ("connection", False, "refused") in rows


def test_the_signature_report_prints_its_fields(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, ["debug", "--project-dir", str(project_copy(tmp_path)), "--snowflake-signatures"])
    assert result.exit_code == 0 and result.stdout


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        (["--output=agent", "validate"], "agent"),
        (["-oagent", "validate"], "agent"),
        (["-o", "json", "validate", "-o", "agent"], "agent"),
        (["-o", "table", "validate"], None),
        (["validate", "-o"], None),
    ],
)
def test_every_spelling_of_a_machine_format_is_read(arguments: list[str], expected: str | None) -> None:
    assert _machine_output(arguments) == expected


def test_a_refusal_under_agent_output_returns_its_code_without_standalone_mode(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli.main(["-o", "agent", "validate", "--nope"], prog_name="sst", standalone_mode=False) == 3
    assert capsys.readouterr().out.startswith("sst validate: USAGE exit=3")


def test_a_configuration_refusal_under_agent_output_is_an_agent_document(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SST_THREADS", "many")
    result = CliRunner().invoke(cli, ["-o", "agent", "validate"])
    assert result.exit_code == 4 and result.stdout.startswith("sst validate: CONFIG exit=4")


@pytest.mark.usefixtures("policy")
def test_an_agent_document_carrying_a_credential_is_withheld(capsys: pytest.CaptureFixture[str]) -> None:
    output.register_secrets(("hunter2-secret",))
    bag = DiagnosticBag((D("SST-REF002", column="hunter2-secret", model="m"),))
    with pytest.raises(click.exceptions.Exit) as ended:
        emit_agent("validate", bag, 1)
    out = capsys.readouterr().out
    assert ended.value.exit_code == 1 and "hunter2-secret" not in out and "SST-PRT012" in out


@pytest.mark.usefixtures("policy")
def test_an_interrupt_under_agent_output_is_an_agent_document(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(click.exceptions.Exit) as ended:
        interrupted("plan", "agent", KeyboardInterrupt())
    assert ended.value.exit_code == 130
    assert capsys.readouterr().out.startswith("sst plan: INTERRUPTED exit=130")


def test_an_agent_document_lists_what_list_listed() -> None:
    data = {
        "items": [
            {"key": "table:a", "relation": "DB.S.A"},
            {"key": "semantic_view:v", "status": "applied", "target": "DB.S.V"},
        ]
    }
    out = agent_document(
        "list",
        0,
        DiagnosticBag(),
        baselined=frozenset(),
        fingerprint=stable_fingerprint,
        project_dir=Path("."),
        data=data,
    )
    assert out.splitlines()[1:3] == ["table:a DB.S.A", "semantic_view:v applied DB.S.V"]


def test_an_agent_document_skips_unchanged_outcomes_and_drops_a_repeated_subject() -> None:
    data = {
        "outcomes": [
            {"action": "noop", "artifact_key": "a", "status": "applied"},
            {"action": "create", "artifact_key": "b", "status": "applied"},
        ]
    }
    finding = D("SST-PRS004", subject="semantic_view:v", artifact="semantic_view:v", field="f")
    out = agent_document(
        "apply",
        0,
        DiagnosticBag((finding,)),
        baselined=frozenset(),
        fingerprint=stable_fingerprint,
        project_dir=Path("."),
        data=data,
    )
    assert "applied b create" in out and "applied a" not in out
    assert "semantic_view:v | unknown field 'f'" in out


def test_apply_outcomes_show_a_failure_and_skip_the_unchanged() -> None:
    def outcome(key: str, action: Action, status: OutcomeStatus, error: ClassifiedError | None = None) -> ApplyOutcome:
        return ApplyOutcome(key, action, status, 1, 0, "", error=error)

    refused = ClassifiedError("SST-SNO003", "no privilege\nmore", ErrorKind.PRIVILEGE)
    lines = outcome_lines(
        (
            outcome("agent:b", Action.UPDATE, OutcomeStatus.FAILED, refused),
            outcome("tool:c", Action.CREATE, OutcomeStatus.SKIPPED),
            outcome("tool:d", Action.PRUNE, OutcomeStatus.APPLIED),
            outcome("view:e", Action.NOOP, OutcomeStatus.APPLIED),
        ),
        Style(Terminal(unicode=False)),
    )
    assert [" ".join(line.split()) for line in lines] == [
        "x failed agent:b no privilege",
        "~ skipped tool:c 0.0s",
        "ok pruned tool:d 0.0s",
    ]


def test_a_table_cuts_the_widest_column_but_keeps_the_named_one_until_last() -> None:
    rows = [["a" * 40, "b" * 40, "c"]]
    [line] = table(rows, 60, keep=0)
    assert line.startswith("a" * 40) and "..." in line
    assert table([], 60) == []
    [narrow] = table([["a" * 30, "b" * 30, "c" * 30]], 20)
    assert narrow.count("...") == 2


def test_a_cut_qualified_name_keeps_its_database_and_its_object() -> None:
    rows = [["+", "semantic_view:sales_by_location", "SALES_DB.SEMANTIC_VIEWS.SALES_BY_LOCATION", "n" * 30]]
    [line] = table(rows, 100, keep=1)
    assert "SALES_DB...SALES_BY_LOCATION" in line
    [tight] = table([["SALES_DB.SEMANTIC_VIEWS.SALES_BY_LOCATION", "n" * 30]], 42)
    assert tight.startswith("...SALES_BY_LOCATION ")
    [prose] = table([["a b c.d.e " * 4, "n" * 30]], 40)
    assert prose.startswith("a b c.d.e") and "..." in prose


def test_an_update_says_its_definition_changed_not_its_marker() -> None:
    view = builders.rendered("SALES")
    stale = builders.observed(view, ownership=OwnershipMarker("a" * 64, "b" * 64))
    remarked = builders.observed(view, ownership=OwnershipMarker("a" * 64, view.fingerprint))
    plan = builders.changeset(
        builders.change(view, Action.UPDATE, live=stale), builders.change(view, Action.UPDATE, live=remarked)
    )
    edited, marker_only = plan_lines(plan, Style(PLAIN))
    assert edited.endswith("  definition") and "fingerprint" not in edited
    assert marker_only.endswith("  ownership marker")


def test_stderr_writes_go_around_a_live_line(monkeypatch: pytest.MonkeyPatch) -> None:
    stream, captured = io.StringIO(), io.StringIO()
    display = ProgressDisplay(Style(PLAIN), live=True, stream=stream)
    monkeypatch.setattr("sys.stderr", captured)
    display.start()
    try:
        print("a finding", file=sys.stderr)
        sys.stderr.flush()
        assert sys.stderr.encoding == captured.encoding
    finally:
        display.stop()
    assert captured.getvalue() == "a finding\n" and sys.stderr is captured


@pytest.mark.usefixtures("policy")
def test_a_log_record_prints_above_the_progress_line() -> None:
    stream = io.StringIO()
    display = ProgressDisplay(Style(PLAIN), live=False, stream=stream)
    root = logging.getLogger()
    level, handlers = root.level, list(root.handlers)
    display.start()
    try:
        configure_logging("warn")
        logging.getLogger("snowflake.connector").warning("retrying")
    finally:
        display.stop()
        root.setLevel(level)
        root.handlers[:] = handlers
    assert stream.getvalue() == "warn  snowflake.connector: retrying\n"
