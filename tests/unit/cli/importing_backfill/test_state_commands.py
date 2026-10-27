"""`sst state recover`, `adopt` and `convert` from the command line: questions, refusals, and reports."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector.convert import Conversion
from snowflake_semantic_tools.app.adopt import AdoptedArtifact, AdoptObjects, AdoptResult
from snowflake_semantic_tools.app.plan_values import PlanRefused
from snowflake_semantic_tools.app.recover import RecoverResult
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.cli_projects import common, compile_project, invoke_with_port, on_a_terminal
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

_STATE = "snowflake_semantic_tools.cli.commands.state"
_KEY = "semantic_view:jaffle_sales"


@pytest.fixture
def project(tmp_path: Path) -> Path:
    copied = project_copy(tmp_path)
    compile_project(copied)
    return copied


def _adopting(monkeypatch: pytest.MonkeyPatch, *artifacts: AdoptedArtifact, refused: bool = False) -> list[str]:
    """Answer adopt's compare with `artifacts`; its record writes each recorded one, unless refused."""
    recorded: list[str] = []

    def compare(self: AdoptObjects, changeset: object, result: object, **_: object) -> tuple[object, tuple[()]]:
        return artifacts, ()

    def record(self: AdoptObjects, found: tuple[AdoptedArtifact, ...], *args: object, **_: object) -> AdoptResult:
        wrote = () if refused else tuple(item.key for item in found if item.recorded)
        recorded.extend(wrote)
        return AdoptResult(found, DiagnosticBag(), wrote=wrote, refused=refused)

    monkeypatch.setattr(f"{_STATE}.AdoptObjects.compare", compare)
    monkeypatch.setattr(f"{_STATE}.AdoptObjects.record", record)
    return recorded


# -- recover ------------------------------------------------------------------------------------------


def test_recover_reports_a_refused_plan_and_records_nothing(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    refused = PlanRefused(DiagnosticBag((D("SST-APL011", value="run r1"),)))
    monkeypatch.setattr(f"{_STATE}.plan_runtime", lambda request: refused)
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["state", "recover", *common(project), "--yes"])
    assert result.exit_code == 1, result.output
    assert "SST-APL011" in result.output


def test_recover_asks_before_it_records_on_a_terminal(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    on_a_terminal(monkeypatch)
    monkeypatch.setattr(f"{_STATE}.RecoverState.run", lambda self, *args, **kwargs: RecoverResult((), DiagnosticBag()))
    declined = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["state", "recover", *common(project)], input="n\n"
    )
    assert declined.exit_code == 130, declined.output
    accepted = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["state", "recover", *common(project)], input="y\n"
    )
    assert accepted.exit_code == 0, accepted.output
    assert "Record lost state entries for target" in accepted.output


def test_recover_off_a_terminal_without_yes_is_refused(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["state", "recover", *common(project)])
    assert result.exit_code == 3 and "SST-PRT109" in result.output


# -- adopt --------------------------------------------------------------------------------------------


def test_adopt_off_a_terminal_without_yes_is_refused(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["state", "adopt", "--select", _KEY, *common(project)]
    )
    assert result.exit_code == 3, result.output
    assert "SST-PRT109" in result.output


def test_adopt_reports_a_selection_that_plans_nothing(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    args = ["state", "adopt", "--select", "semantic_view:no_such_view", *common(project), "--yes"]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)
    assert result.exit_code != 0, result.output
    assert "SST-DIS010" in result.output


def test_adopt_reports_a_refused_plan_and_records_nothing(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    refused = PlanRefused(DiagnosticBag((D("SST-APL011", value="run r1"),)))
    monkeypatch.setattr(f"{_STATE}.plan_runtime", lambda request: refused)
    recorded = _adopting(monkeypatch)
    args = ["state", "adopt", "--select", _KEY, *common(project), "--yes"]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)
    assert result.exit_code == 1, result.output
    assert "SST-APL011" in result.output and recorded == []


def test_adopt_asks_then_records_and_lists_each_artifact(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    on_a_terminal(monkeypatch)
    recorded = _adopting(
        monkeypatch,
        AdoptedArtifact(_KEY, "DB.S.JAFFLE_SALES", "differs", ("view comment (text: Old. -> New.)",), recorded=True),
        AdoptedArtifact("tool:faq", "DB.S.FAQ", "state records it already"),
    )
    args = ["state", "adopt", "--select", _KEY, *common(project), "--accept-differences"]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args, input="y\n")
    assert result.exit_code == 0, result.output
    assert recorded == [_KEY]
    assert "Record 1 object(s) on target" in result.output
    assert f"{_KEY}  adopted" in result.stdout
    assert "view comment (text: Old. -> New.)" in result.stdout
    assert "tool:faq                    state records it already" in result.stdout


def test_adopt_reports_json_and_fails_when_the_lock_refuses_the_record(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _adopting(monkeypatch, AdoptedArtifact(_KEY, "DB.S.JAFFLE_SALES", "matches", recorded=True), refused=True)
    args = ["state", "adopt", "--select", _KEY, *common(project), "--yes", "-o", "json"]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)
    assert result.exit_code == 1, result.output
    data = json.loads(result.stdout)["data"]
    assert data["adopted"] == []
    assert data["artifacts"] == [
        {"key": _KEY, "object": "DB.S.JAFFLE_SALES", "found": "matches", "differences": [], "recorded": False}
    ]


def test_adopt_says_when_the_selection_plans_no_artifact(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _adopting(monkeypatch)
    args = ["state", "adopt", "--select", _KEY, *common(project), "--yes"]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)
    assert result.exit_code == 0, result.output
    assert "the selection plans no artifact; nothing to adopt" in result.stdout


# -- convert ------------------------------------------------------------------------------------------


def test_convert_asks_on_a_terminal_before_it_rebuilds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    on_a_terminal(monkeypatch)
    project = project_copy(tmp_path)
    args = ["state", "convert", *common(project), "--to", "hybrid"]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args, input="y\n")
    assert result.exit_code == 0, result.output
    assert "and its lock table as hybrid tables?" in result.output


def test_convert_off_a_terminal_without_yes_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    args = ["state", "convert", *common(project_copy(tmp_path)), "--to", "hybrid"]
    result = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)
    assert result.exit_code == 3 and "SST-PRT109" in result.output


def test_a_conversion_snowflake_refuses_without_a_diagnostic_is_a_connection_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = FakeSnowflake(state={})
    port.refuse("convert_state_tables")
    args = ["state", "convert", *common(project_copy(tmp_path)), "--to", "hybrid", "--yes"]
    result = invoke_with_port(monkeypatch, port, args)
    assert result.exit_code not in (0, 1), result.output
    assert "convert_state_tables refused" in result.output


def test_a_conversion_with_no_lock_table_to_the_configured_kind_names_only_the_state_backup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class NoLockTable(FakeSnowflake):
        def convert_state_tables(self, state_table: QualifiedName, to_kind: str) -> Conversion:
            return Conversion("hybrid", to_kind, 3, QualifiedName.parse(f"{state_table.sql}_HYBRID_BAK"), None)

    args = ["state", "convert", *common(project_copy(tmp_path)), "--to", "standard", "--yes"]
    result = invoke_with_port(monkeypatch, NoLockTable(state={}), args)
    assert result.exit_code == 0, result.output
    assert "from hybrid to standard: 3 rows" in result.stdout
    assert "kept the old lock table" not in result.stdout
    assert "set state.+table_kind" not in result.stdout
