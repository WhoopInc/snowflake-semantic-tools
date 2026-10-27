"""`sst import` from the command line: refusals, `--to`, stage paths, and what the human report prints."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.app.importing import ImportReport
from snowflake_semantic_tools.domain.importing.model import ColumnMeta, ImportedFile, ImportedObject, ModelEdit
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.cli_projects import common, invoke_with_port
from tests.helpers.import_fakes import ANALYST_SPEC, FakeImportPort, staged_agent
from tests.helpers.reference_project import project_copy

_MARKER = "[sst:" + "a" * 64 + ":" + "b" * 64 + "]"
_COWORK_SPEC = {**ANALYST_SPEC, "tools": [{"tool_spec": {"type": "agent_router", "name": "ROUTER"}}]}


def _port() -> FakeImportPort:
    port = FakeImportPort()
    staged_agent(port, "DB.S.SALES_BOT")
    return port


def _reported(monkeypatch: pytest.MonkeyPatch, report: ImportReport) -> None:
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.import_.ImportObjects.run", lambda self, request: report)


@pytest.mark.parametrize(
    ("args", "detail"),
    [
        (["import", "tool", "DB.T.FAQ", "--agent-version", "VERSION$1"], "--agent-version picks the version"),
        (["import", "agent", "DB.S.X", "--to", "/abs/team"], "--to names a folder inside the project"),
        (["import", "agent", "DB.S.X", "--to", "agents/../../out"], "--to names a folder inside the project"),
        (["import", "skill", "A.B.C.D"], "is none of them"),
        (["import", "eval", "DB.S", "--to", "agents/sales_bot"], "reads one eval input table"),
    ],
)
def test_import_refuses_a_command_line_naming_what_it_cannot_take(
    args: list[str], detail: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = invoke_with_port(monkeypatch, _port(), [*args, *common(project_copy(tmp_path))])
    assert result.exit_code == 3, result.output
    assert "SST-PRT100" in result.output and detail in result.output


@pytest.mark.parametrize("to_folder", ["agents/team", "team", "./team"])
def test_to_names_the_folder_below_the_types_directory_given_inside_it_or_relative_to_it(
    to_folder: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    args = ["import", "agent", "DB.S.SALES_BOT", "--to", to_folder, *common(project), "-o", "json"]
    result = invoke_with_port(monkeypatch, _port(), args)
    assert result.exit_code == 2, result.output
    [obj] = json.loads(result.stdout)["data"]["objects"]
    assert "agents/team/sales_bot/agent.yml" in obj["files"]


def test_a_skill_is_imported_from_a_stage_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    port = FakeImportPort()
    port.stage_file("@DB.SK.SRC/fresh_check/SKILL.md", b"---\nname: fresh_check\n---\n")
    args = ["import", "skill", "@DB.SK.SRC/fresh_check/", *common(project), "--write", "-o", "json"]
    result = invoke_with_port(monkeypatch, port, args)
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)["data"]
    assert data["scope"] == "DB.SK.SRC" and data["written"] == ["skills/fresh_check/SKILL.md"]
    assert (project / "skills" / "fresh_check" / "SKILL.md").read_bytes() == b"---\nname: fresh_check\n---\n"


def test_the_human_report_says_what_was_refused_and_what_was_not_imported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = FakeImportPort()
    staged_agent(port, "DB.S.OWNED", comment=f"Owned. {_MARKER}")
    project = project_copy(tmp_path)
    owned = invoke_with_port(monkeypatch, port, ["import", "agent", "DB.S.OWNED", *common(project)])
    assert owned.exit_code == 0, owned.output
    assert "not imported: agent DB.S.OWNED" in owned.stdout
    assert "nothing to import" in owned.output
    missing = invoke_with_port(monkeypatch, port, ["import", "agent", "DB.S.MISSING", *common(project)])
    assert missing.exit_code == 1, missing.output
    assert "refused: agent DB.S.MISSING" in missing.stdout


def test_a_schema_run_skips_the_other_agent_kind_and_says_how_many(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = _port()
    staged_agent(port, "DB.S.ROUTER", _COWORK_SPEC)
    result = invoke_with_port(monkeypatch, port, ["import", "agent", "DB.S", *common(project_copy(tmp_path))])
    assert result.exit_code == 2, result.output
    assert "skipped 1 object(s) of the other agent kind" in result.stdout


def _edited_report(*edits: ModelEdit, files: tuple[ImportedFile, ...] = ()) -> ImportReport:
    name = QualifiedName.parse("DB.SV.ORDERS_VIEW")
    item = ImportedObject("semantic_view", name, "orders_view", files, model_edits=edits)
    return ImportReport((item,))


def test_model_edits_are_reported_pending_then_made_with_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    edits = (
        ModelEdit("imported_orders", "models/imported/orders.yml", description="Orders.", columns=(ColumnMeta("id"),)),
        ModelEdit("imported_items", "models/imported/items.yml", meta=(("synonyms", ("lines",)),)),
    )
    files = (
        ImportedFile("semantic_models/imported/notes.md", text="Read me.\n"),
        ImportedFile("semantic_models/imported/blob.bin", data=b"\x00\x01"),
        ImportedFile("semantic_models/imported/empty.bin"),
    )
    _reported(monkeypatch, _edited_report(*edits, files=files))
    pending = invoke_with_port(monkeypatch, _port(), ["import", "semantic_view", "DB.SV.ORDERS_VIEW", *common(project)])
    assert pending.exit_code == 2, pending.output
    assert "would edit models/imported/orders.yml (model imported_orders, 1 column(s))" in pending.stdout
    assert "would edit models/imported/items.yml (model imported_items)" in pending.stdout
    assert "3 file(s) and 2 model file(s) pending" in pending.output

    args = ["import", "semantic_view", "DB.SV.ORDERS_VIEW", *common(project), "--write", "-o", "json"]
    written = invoke_with_port(monkeypatch, _port(), args)
    assert written.exit_code == 0, written.output
    data = json.loads(written.stdout)["data"]
    assert data["edited"] == ["models/imported/orders.yml", "models/imported/items.yml"]
    assert data["objects"][0]["model_edits"][0] == {
        "model": "imported_orders",
        "file": "models/imported/orders.yml",
        "keys": ["description"],
        "columns": ["id"],
    }
    assert "imported_orders" in (project / "models" / "imported" / "orders.yml").read_text(encoding="utf-8")
    assert (project / "semantic_models" / "imported" / "notes.md").read_text(encoding="utf-8") == "Read me.\n"
    assert (project / "semantic_models" / "imported" / "blob.bin").read_bytes() == b"\x00\x01"
    assert (project / "semantic_models" / "imported" / "empty.bin").read_bytes() == b""


def test_the_human_report_names_what_write_edited(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    _reported(monkeypatch, _edited_report(ModelEdit("imported_orders", "models/imported/orders.yml", description="O.")))
    args = ["import", "semantic_view", "DB.SV.ORDERS_VIEW", *common(project), "--write"]
    result = invoke_with_port(monkeypatch, _port(), args)
    assert result.exit_code == 0, result.output
    assert "edited models/imported/orders.yml (model imported_orders)" in result.stdout
    assert "wrote 0 file(s), edited 1 model file(s)" in result.output


def test_a_model_file_that_cannot_be_read_fails_the_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    (project / "models" / "imported" / "orders.yml").mkdir(parents=True)
    _reported(monkeypatch, _edited_report(ModelEdit("imported_orders", "models/imported/orders.yml", description="O.")))
    args = ["import", "semantic_view", "DB.SV.ORDERS_VIEW", *common(project), "--write"]
    result = invoke_with_port(monkeypatch, _port(), args)
    assert result.exit_code == 1, result.output
    assert "models/imported/orders.yml" in result.output


def test_a_file_that_appeared_since_the_check_is_never_replaced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    target = project / "semantic_models" / "raced.md"
    target.write_text("mine\n", encoding="utf-8")
    _reported(monkeypatch, _edited_report(files=(ImportedFile("semantic_models/raced.md", text="theirs\n"),)))
    args = ["import", "semantic_view", "DB.SV.ORDERS_VIEW", *common(project), "--write"]
    result = invoke_with_port(monkeypatch, _port(), args)
    assert result.exit_code == 1, result.output
    assert "raced.md" in result.output
    assert target.read_text(encoding="utf-8") == "mine\n"
