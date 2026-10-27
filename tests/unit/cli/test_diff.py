"""`sst diff`: local, a saved plan, and live targets compared, exit 2 on a difference."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.adapters.fs.local import ManifestFileStore
from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.app.compile import CompiledView
from snowflake_semantic_tools.app.compile.tools import CompiledTool
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.compile import compile_result
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.plan.definitions.tool import search_query
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.artifact_builders import applied_entry
from tests.helpers.cli_projects import common, compile_project, invoke_with_port
from tests.helpers.described_objects import described_routine, described_search, described_view
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

TABLE = QualifiedName.from_parts("DB", "S", "SST_STATE")


def _local(project: Path) -> dict[str, tuple[str, str]]:
    manifest = ManifestFileStore(project / "sst_target" / "manifest.json").read()
    assert manifest is not None
    return {key: (entry.fingerprint, entry.publish_target) for key, entry in manifest.artifacts.items()}


def _deployed(project: Path, *, drop: str, change: str) -> FakeSnowflake:
    """A target holding every local view and agent as compiled, less `drop`, with `change` re-rendered."""
    state: dict[str, AppliedEntry] = {}
    markers: dict[str, OwnershipMarker | None] = {}
    for key, (fingerprint, target) in _local(project).items():
        if key == drop or not key.startswith(("semantic_view:", "agent:")):
            continue
        live = "b" * 64 if key == change else fingerprint
        state[key] = applied_entry(target, live)
        markers[target] = OwnershipMarker("a" * 64, live)
    # Only the target holds this one; the next is in state, but its object was dropped by hand.
    state["semantic_view:only_live"] = applied_entry("DB.S.ONLY", "c" * 64)
    markers["DB.S.ONLY"] = OwnershipMarker("a" * 64, "c" * 64)
    state["semantic_view:gone_by_hand"] = applied_entry("DB.S.GONE", "d" * 64)
    return FakeSnowflake(state=state, markers=markers)


def _diff(monkeypatch: pytest.MonkeyPatch, port: FakeSnowflake, project: Path, *args: str) -> Result:
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
    # The names alone on stdout, for a script; the verdict goes to stderr.
    assert (names.exit_code, names.stdout) == (2, "jaffle_minimal_agent\n")
    excluded = _diff(monkeypatch, port, project, "--exclude", "type:agent", "--names-only", "--no-detailed-exitcode")
    assert excluded.exit_code == 0
    assert "jaffle_minimal_agent" not in excluded.output
    assert _diff(monkeypatch, port, project, "--select", "path:nothing/*", "--names-only").stdout == ""


def test_two_targets_agree_with_no_local_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    port = FakeSnowflake(state={"skill:bundle": applied_entry("DB.S.BUNDLE", "3" * 64)})
    result = _diff(monkeypatch, port, project, "--from", "dev", "--to", "prod")
    assert result.exit_code == 0, result.output
    assert "dev and prod agree" in result.output


def _held(project: Path, target: str, *, defer_to: str | None) -> FakeSnowflake:
    """A target holding each view, search service and routine `project` compiles for it, deferred to `defer_to`."""
    paths = dataclasses.replace(locate_project(project), defer_target=defer_to, defer_disabled=defer_to is None)
    state: dict[str, AppliedEntry] = {}
    port = FakeSnowflake(state=state)
    for item in compile_result(paths, target, DBT_MANIFEST).compiled:
        rendered = item.rendered_artifact
        name = rendered.target.sql
        if isinstance(item, CompiledView):
            port.view_descriptions[name] = described_view(item.view)
        elif isinstance(item, CompiledTool) and rendered.object_type == "CORTEX SEARCH SERVICE":
            port.descriptions[f"CORTEX SEARCH SERVICE {name}"] = described_search(
                item.member, search_query(rendered.ddl)
            )
        elif isinstance(item, CompiledTool) and rendered.object_type in ("PROCEDURE", "FUNCTION"):
            types = ", ".join(parameter.type for parameter in item.member.signature)
            port.routine_descriptions[f"{rendered.object_type} {name}({types})"] = described_routine(item.member)
        else:
            continue
        state[item.artifact_key] = applied_entry(name, rendered.fingerprint)
        port.markers[name] = OwnershipMarker("a" * 64, rendered.fingerprint)
    port.remote_state = state
    port.close = lambda: None  # type: ignore[attr-defined]
    return port


def test_a_target_deferring_to_the_other_agrees_with_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    deferred = project / "sst_target" / "defer" / "prod" / "manifest.json"
    deferred.parent.mkdir(parents=True)
    deferred.write_text(
        DBT_MANIFEST.read_text(encoding="utf-8").replace("SST_REF_DEV", "SST_REF_PROD"), encoding="utf-8"
    )
    config = project / "sst_config.yml"
    defer = "\ndefer:\n  target: \"{{ 'prod' if target.name == 'dev' else '' }}\"\n"
    config.write_text(config.read_text(encoding="utf-8") + defer, encoding="utf-8")
    # dev's objects read prod's relations, as `sst apply --target dev` publishes them. --manifest
    # gives both compiles one dbt manifest, so prod's own relations are the ones it holds.
    ports = {
        "SST_REF_PROD": _held(project, "prod", defer_to=None),
        "SST_REF_DEV": _held(project, "dev", defer_to="prod"),
    }
    monkeypatch.setattr(
        "snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: ports[str(params["database"]).upper()]
    )
    # The run resolves its configuration for prod, where dev's deferral renders empty.
    args = ["diff", *common(project), "--target", "prod", "--from", "prod", "--to", "dev", "--output", "json"]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["data"]["differences"] == []
    assert any(key.startswith("tool:") for key in ports["SST_REF_DEV"].remote_state or {})


def test_local_against_a_saved_plan_and_unreadable_states(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    missing = CliRunner().invoke(cli, ["diff", *common(project), "--to", "plan.json"])
    assert missing.exit_code == 1
    assert "SST-MAN001" in missing.output
    compile_project(project)
    absent = CliRunner().invoke(cli, ["diff", *common(project), "--to", "plan.json"])
    assert (absent.exit_code, "there is no saved plan" in absent.output) == (1, True)
    planned = invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["plan", *common(project)])
    assert planned.exit_code == 2, planned.output
    saved = CliRunner().invoke(cli, ["diff", *common(project), "--to", "sst_target/plan.json"])
    assert saved.exit_code == 0, saved.output
    (project / "broken.json").write_text("{", encoding="utf-8")
    assert CliRunner().invoke(cli, ["diff", *common(project), "--to", "broken.json"]).exit_code == 1
    port = FakeSnowflake()
    monkeypatch.setattr(port, "read_state", lambda table, target: None)
    unreadable = _diff(monkeypatch, port, project)
    assert (unreadable.exit_code, "SST-MAN022" in unreadable.output) == (1, True)
