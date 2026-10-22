"""`sst state recover` and `sst state convert` from the command line, against the fake target."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from snowflake_semantic_tools.app.recover import RecoveredArtifact, RecoverResult, RecoverState
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle import BUNDLE_PREFIX_MISSING, RECORDED
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cli_projects import applied_port, common, compile_project, invoke_with_port
from tests.helpers.published_target import publishing_port
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

_FOUND = RecoverResult(
    (
        RecoveredArtifact("semantic_view:jaffle_sales", RECORDED, "DB.S.JAFFLE_SALES"),
        RecoveredArtifact("skill:jaffle-semantics", BUNDLE_PREFIX_MISSING, "DB.S.JAFFLE_SEMANTICS", "blocked"),
    ),
    DiagnosticBag((D("SST-APL032", count=1, value=1),)),
    wrote=True,
)


def test_recover_reports_what_it_recorded_and_what_the_next_apply_does(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # What recover decides is app/recover.py's, tested there and live; this is the command around it.
    project = project_copy(tmp_path)
    compile_project(project)
    seen: list[str] = []

    def run(self: RecoverState, changeset: object, state: object, target_name: str, **_: object) -> RecoverResult:
        seen.append(target_name)
        return _FOUND

    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.state.RecoverState.run", run)
    result = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["state", "recover", *common(project), "--yes", "-o", "json"]
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)["data"]
    assert data["recorded"] == ["semantic_view:jaffle_sales"] and seen == [data["target"]]
    assert data["artifacts"][1] == {
        "key": "skill:jaffle-semantics",
        "found": BUNDLE_PREFIX_MISSING,
        "object": "DB.S.JAFFLE_SEMANTICS",
        "next_apply": "blocked",
    }
    printed = invoke_with_port(monkeypatch, FakeSnowflake(state={}), ["state", "recover", *common(project), "--yes"])
    assert "semantic_view:jaffle_sales  recorded" in printed.stdout
    assert "bundle prefix missing  (next apply: blocked)" in printed.stdout


def test_a_recover_with_everything_recorded_says_so(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    monkeypatch.setattr(
        "snowflake_semantic_tools.cli.commands.state.RecoverState.run",
        lambda self, *args, **kwargs: RecoverResult((), DiagnosticBag()),
    )
    result = invoke_with_port(monkeypatch, applied_port(project), ["state", "recover", *common(project), "--yes"])
    assert result.exit_code == 0, result.output
    assert "state records every planned artifact; nothing to recover" in result.stdout


def _plan(monkeypatch: pytest.MonkeyPatch, port: FakeSnowflake, project: Path) -> dict[str, str]:
    result = invoke_with_port(monkeypatch, port, ["plan", *common(project), "--target", "dev", "--output", "json"])
    return {item["artifact_key"]: item["action"] for item in json.loads(result.output)["data"]["changes"]}


def test_recover_after_lost_state_brings_every_artifact_back_so_the_next_plan_changes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    port = publishing_port(project)
    applied = invoke_with_port(monkeypatch, port, ["apply", *common(project), "--target", "dev", "--yes"])
    assert applied.exit_code == 0, applied.output
    published = dict(port.remote_state or {})
    kinds = {key.split(":", 1)[0] for key in published}
    assert {"skill", "plugin", "coco_desktop_profile", "eval", "agent", "semantic_view", "tool"} <= kinds
    assert set(_plan(monkeypatch, port, project).values()) == {"noop"}

    port.remote_state, port.state_manifest = {}, None
    lost = _plan(monkeypatch, port, project)
    assert set(lost.values()) == {"blocked"} and set(lost) == set(published)
    result = invoke_with_port(
        monkeypatch, port, ["state", "recover", *common(project), "--target", "dev", "--yes", "--output", "json"]
    )
    assert result.exit_code == 0, result.output
    assert sorted(json.loads(result.output)["data"]["recorded"]) == sorted(published)
    assert set(_plan(monkeypatch, port, project).values()) == {"noop"}
    # A composite entry is the one apply recorded, its parts and resources included.
    recovered = port.remote_state or {}
    for key, entry in published.items():
        if key.split(":", 1)[0] in ("skill", "plugin", "coco_desktop_profile", "eval"):
            again = recovered[key]
            assert (again.fingerprint, again.qualified_name, again.outcome) == (
                entry.fingerprint,
                entry.qualified_name,
                "observed",
            )
            assert again.component_fingerprints == entry.component_fingerprints, key
            assert again.applied_resources == entry.applied_resources, key


def test_convert_names_the_backups_and_the_setting_to_change(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    result = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["state", "convert", *common(project), "--to", "hybrid", "--yes"]
    )
    assert result.exit_code == 0, result.output
    assert "converted" in result.stdout and "from standard to hybrid" in result.stdout
    assert "_STANDARD_BAK" in result.stdout
    assert "set state.+table_kind: hybrid" in result.stdout
    refused = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["state", "convert", *common(project), "--to", "standard", "--yes"]
    )
    assert refused.exit_code == 1 and "SST-APL034" in refused.output


@pytest.mark.parametrize("code", ["SST-APL030", "SST-APL035"])
def test_convert_reports_a_hybrid_table_snowflake_will_not_create(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: str
) -> None:
    project = project_copy(tmp_path)
    port = FakeSnowflake(state={})
    diagnostic = D(code, value="DB.S.SST_STATE", detail="Hybrid tables are not available for this account.")
    port.fail("convert_state_tables", SnowflakePortError(diagnostic.message, diagnostic=diagnostic))
    result = invoke_with_port(monkeypatch, port, ["state", "convert", *common(project), "--to", "hybrid", "--yes"])
    assert result.exit_code == 1 and code in result.output
