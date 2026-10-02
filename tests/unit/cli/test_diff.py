"""`sst diff`: local, a saved plan, and live targets compared, exit 2 on a difference."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry
from tests.helpers.cli_projects import common, compile_project, invoke_with_port, project_copy
from tests.helpers.recorded_snowflake import RecordedSnowflake

TABLE = QualifiedName.from_parts("DB", "S", "SST_STATE")


def _entry(target: str, fingerprint: str, *, outcome: str = APPLIED) -> AppliedEntry:
    return AppliedEntry(fingerprint, target, "now", "run", outcome, fingerprint, "a" * 64)


def _local(project: Path) -> dict[str, tuple[str, str]]:
    manifest = ManifestFileStore(project / "target" / "sst" / "manifest.json").read()
    assert manifest is not None
    return {key: (entry.fingerprint, entry.publish_target) for key, entry in manifest.artifacts.items()}


def _deployed(project: Path, *, drop: str, change: str) -> RecordedSnowflake:
    """A target holding every local view and agent as compiled, less `drop`, with `change` re-rendered."""
    state: dict[str, AppliedEntry] = {}
    markers: dict[str, OwnershipMarker | None] = {}
    for key, (fingerprint, target) in _local(project).items():
        if key == drop or not key.startswith(("semantic_view:", "agent:")):
            continue
        live = "b" * 64 if key == change else fingerprint
        state[key] = _entry(target, live)
        markers[target] = OwnershipMarker("a" * 64, live)
    # Only the target holds this one; the next is in state, but its object was dropped by hand.
    state["semantic_view:only_live"] = _entry("DB.S.ONLY", "c" * 64)
    markers["DB.S.ONLY"] = OwnershipMarker("a" * 64, "c" * 64)
    state["semantic_view:gone_by_hand"] = _entry("DB.S.GONE", "d" * 64)
    return RecordedSnowflake(state=state, markers=markers)


def _diff(monkeypatch: pytest.MonkeyPatch, port: RecordedSnowflake, project: Path, *args: str) -> Result:
    return invoke_with_port(monkeypatch, port, ["diff", *common(project), *args])


def test_diff_local_against_the_default_target_exits_2_and_explains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    port = _deployed(project, drop="semantic_view:jaffle_menu", change="agent:jaffle_minimal_agent")
    result = _diff(monkeypatch, port, project, "--full", "--output", "json")
    assert result.exit_code == 2, result.output
    data = json.loads(result.output)["data"]
    assert (data["from"], data["to"]) == ({"ref": "local", "kind": "local"}, {"ref": "dev", "kind": "target"})
    statuses = {item["key"]: item["status"] for item in data["differences"]}
    assert statuses["semantic_view:jaffle_menu"] == "orphaned"
    assert statuses["agent:jaffle_minimal_agent"] == "modified"
    assert statuses["semantic_view:only_live"] == "new"
    assert "semantic_view:gone_by_hand" not in statuses
    modified = next(item for item in data["differences"] if item["status"] == "modified")
    assert modified["properties"] == ["fingerprint", "manifest_id"]
    human = _diff(monkeypatch, port, project, "--full")
    assert "    fingerprint: " in human.output
    assert "artifact(s) differ between local and dev" in human.output


def test_names_only_selection_and_no_detailed_exitcode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    port = _deployed(project, drop="semantic_view:jaffle_menu", change="agent:jaffle_minimal_agent")
    names = _diff(monkeypatch, port, project, "--names-only", "--select", "type:agent")
    assert (names.exit_code, names.output) == (2, "jaffle_minimal_agent\n")
    excluded = _diff(monkeypatch, port, project, "--exclude", "type:agent", "--names-only", "--no-detailed-exitcode")
    assert excluded.exit_code == 0
    assert "jaffle_minimal_agent" not in excluded.output
    assert _diff(monkeypatch, port, project, "--select", "path:nothing/*", "--names-only").output == ""


def test_two_targets_agree_with_no_local_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    port = RecordedSnowflake(state={"skill:bundle": _entry("DB.S.BUNDLE", "3" * 64)})
    result = _diff(monkeypatch, port, project, "--from", "dev", "--to", "prod")
    assert result.exit_code == 0, result.output
    assert "dev and prod agree" in result.output


def test_local_against_a_saved_plan_and_unreadable_states(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    missing = CliRunner().invoke(cli, ["diff", *common(project), "--to", "plan.json"])
    assert missing.exit_code == 1
    assert "SST-MAN001" in missing.output
    compile_project(project)
    absent = CliRunner().invoke(cli, ["diff", *common(project), "--to", "plan.json"])
    assert (absent.exit_code, "there is no saved plan" in absent.output) == (1, True)
    planned = invoke_with_port(monkeypatch, RecordedSnowflake(state={}), ["plan", *common(project)])
    assert planned.exit_code == 2, planned.output
    saved = CliRunner().invoke(cli, ["diff", *common(project), "--to", "target/sst/plan.json"])
    assert saved.exit_code == 0, saved.output
    (project / "broken.json").write_text("{", encoding="utf-8")
    assert CliRunner().invoke(cli, ["diff", *common(project), "--to", "broken.json"]).exit_code == 1
    port = RecordedSnowflake()
    monkeypatch.setattr(port, "read_state", lambda table, target: None)
    unreadable = _diff(monkeypatch, port, project)
    assert (unreadable.exit_code, "SST-MAN022" in unreadable.output) == (1, True)
