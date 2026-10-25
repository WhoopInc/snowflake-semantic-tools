"""A bare `sst apply` applies the plan `sst plan` just saved, planning again only what it writes."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.commands import apply as apply_command
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.plan import PlanRequest
from snowflake_semantic_tools.domain.state import content_hash
from tests.helpers.cli_projects import invoke_with_port, skill_project
from tests.helpers.projects import project_paths
from tests.helpers.snowflake_fake import FakeSnowflake


def _published(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, FakeSnowflake]:
    """Two skills applied, then one of them edited and compiled: a plan of one update, one no-op."""
    project = skill_project(tmp_path / "skills")
    other = project / "skills" / "year-close"
    other.mkdir(parents=True)
    (other / "SKILL.md").write_text(
        "---\nname: year-close\ndescription: Close the year.\n---\nBody.\n", encoding="utf-8"
    )
    port = FakeSnowflake(state={})
    first = invoke_with_port(monkeypatch, port, ["apply", *_dir(project), "--yes", "--replan", "-o", "json"])
    assert first.exit_code == 0, first.output[-2000:]
    steps = project / "skills" / "month-close" / "reference" / "steps.md"
    steps.write_text("new steps\n", encoding="utf-8")
    compiled = CliRunner().invoke(cli, ["compile", *_dir(project)])
    assert compiled.exit_code == 0, compiled.output[-2000:]
    return project, port


def _dir(project: Path) -> list[str]:
    return ["--project-dir", str(project)]


def _planned_selections(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    seen: list[tuple[str, ...]] = []
    real = apply_command.plan_runtime

    def recording(request: PlanRequest) -> Any:
        seen.append(request.selected)
        return real(request)

    monkeypatch.setattr(apply_command, "plan_runtime", recording)
    return seen


def _plan(monkeypatch: pytest.MonkeyPatch, project: Path, port: FakeSnowflake) -> dict[str, Any]:
    planned = invoke_with_port(monkeypatch, port, ["plan", *_dir(project), "-o", "json"])
    assert planned.exit_code == 2, planned.output[-2000:]
    saved = project / "sst_target" / "plan.json"
    document: dict[str, Any] = json.loads(saved.read_text(encoding="utf-8"))
    return document


def _apply(monkeypatch: pytest.MonkeyPatch, project: Path, port: FakeSnowflake, *flags: str) -> dict[str, Any]:
    applied = invoke_with_port(monkeypatch, port, ["apply", *_dir(project), "--yes", "-o", "json", *flags])
    assert applied.exit_code == 0, applied.output[-2000:]
    envelope: dict[str, Any] = json.loads(applied.output)
    return envelope


def test_a_bare_apply_replans_only_the_saved_writes_and_spends_the_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _published(tmp_path, monkeypatch)
    document = _plan(monkeypatch, project, port)
    writes = [change["key"] for change in document["changes"] if change["action"] not in ("noop", "blocked")]
    assert writes and len(writes) < len(document["changes"])
    seen = _planned_selections(monkeypatch)
    envelope = _apply(monkeypatch, project, port)
    assert seen == [tuple(writes)]
    assert sorted(envelope["data"]["applied"]) == sorted(writes)
    assert not (project / "sst_target" / "plan.json").exists()
    # Spent, so the next bare apply plans afresh and finds nothing to do.
    again = _apply(monkeypatch, project, port)
    assert seen[-1] == () and again["data"]["applied"] == []


def test_replan_an_old_plan_or_a_flag_plans_the_whole_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, port = _published(tmp_path, monkeypatch)
    document = _plan(monkeypatch, project, port)
    saved = project / "sst_target" / "plan.json"
    fresh = saved.read_text(encoding="utf-8")
    old = (datetime.now(UTC) - timedelta(minutes=45)).isoformat()
    aged = {key: value for key, value in document.items() if key != "plan_id"} | {"observation_at": old}
    saved.write_text(json.dumps({**aged, "plan_id": content_hash(aged)}), encoding="utf-8")
    reason = apply_command.reusable_plan(_request(project), flagged=False)
    assert isinstance(reason, str) and "45m old" in reason
    assert apply_command.reusable_plan(_request(project), flagged=True) == "a flag shapes the plan"
    saved.write_text(fresh, encoding="utf-8")
    assert isinstance(apply_command.reusable_plan(_request(project), flagged=False), apply_command.Reuse)
    seen = _planned_selections(monkeypatch)
    _apply(monkeypatch, project, port, "--replan")
    assert seen == [()]


def test_a_saved_write_that_drifted_falls_back_to_a_whole_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, port = _published(tmp_path, monkeypatch)
    document = _plan(monkeypatch, project, port)
    [change] = [item for item in document["changes"] if item["action"] == "update"]
    reuse = apply_command.reusable_plan(_request(project), flagged=False)
    assert isinstance(reuse, apply_command.Reuse)
    session = apply_command.plan_runtime(_request(project, selected=reuse.selectors))
    assert not isinstance(session, apply_command.PlanRefused)
    try:
        assert apply_command.drifted(reuse, session.ready.changeset) is None
        moved = dataclass_replace_hashes(reuse)
        assert apply_command.drifted(moved, session.ready.changeset) == (
            f"{change['key']} changed in Snowflake since sst plan"
        )
    finally:
        session.port.close()


def dataclass_replace_hashes(reuse: Any) -> Any:
    """The saved plan with its one update's statements changed, as a hand edit would change them."""
    import dataclasses

    changes = tuple(
        dataclasses.replace(change, statement_hashes=("0" * 64,)) if change.action == "update" else change
        for change in reuse.saved.changes
    )
    return dataclasses.replace(reuse, saved=dataclasses.replace(reuse.saved, changes=changes))


def _request(project: Path, selected: tuple[str, ...] = ()) -> PlanRequest:
    return PlanRequest(project_paths(project), None, None, selected, (), False, False, None, None, threads=1)
