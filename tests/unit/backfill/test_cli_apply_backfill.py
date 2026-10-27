"""`sst apply`: the saved-plan refusals and the reuse paths the broader suites skip."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.commands import apply as apply_command
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.plan import PlanRequest, plan_runtime
from snowflake_semantic_tools.domain.state import content_hash
from tests.helpers.cli_projects import common, invoke_with_port, skill_project
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake


@pytest.mark.parametrize(
    ("flags", "said"),
    [
        (("--select", "semantic_view:sales"), "--select conflicts with the saved plan selection"),
        (("--exclude", "semantic_view:sales"), "--exclude conflicts with the saved plan selection"),
        (("--prune",), "--prune conflicts with a non-pruning saved plan"),
        (("--partial",), "--partial conflicts with a saved plan that is not partial"),
    ],
)
def test_apply_refuses_flags_that_disagree_with_the_saved_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flags: tuple[str, ...], said: str
) -> None:
    project = project_copy(tmp_path)
    planned = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["plan", *common(project), "--target", "dev", "--output", "json"]
    )
    path = json.loads(planned.output)["data"]["plan_path"]
    refused = CliRunner().invoke(cli, ["apply", *common(project), "--target", "dev", "--plan", path, *flags, "--yes"])
    assert refused.exit_code == 3 and said in refused.output, refused.output


def _dir(project: Path) -> list[str]:
    return ["--project-dir", str(project)]


def _two_skills(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, FakeSnowflake]:
    """Two skills applied; the caller edits one, or leaves both as published."""
    project = skill_project(tmp_path / "skills")
    other = project / "skills" / "year-close"
    other.mkdir(parents=True)
    (other / "SKILL.md").write_text(
        "---\nname: year-close\ndescription: Close the year.\n---\nBody.\n", encoding="utf-8"
    )
    port = FakeSnowflake(state={})
    first = invoke_with_port(monkeypatch, port, ["apply", *_dir(project), "--yes", "--replan", "-o", "json"])
    assert first.exit_code == 0, first.output[-2000:]
    return project, port


def _planned_selections(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    seen: list[tuple[str, ...]] = []
    real = plan_runtime

    def recording(request: PlanRequest) -> Any:
        seen.append(request.selected)
        return real(request)

    monkeypatch.setattr(apply_command, "plan_runtime", recording)
    return seen


def _announced(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record what apply announces, which only a progress display would show."""
    lines: list[str] = []
    monkeypatch.setattr(apply_command, "announce", lambda *parts: lines.append(" · ".join(parts)))
    return lines


def test_a_saved_plan_that_writes_nothing_is_applied_without_planning_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _two_skills(tmp_path, monkeypatch)
    planned = invoke_with_port(monkeypatch, port, ["plan", *_dir(project), "-o", "json"])
    assert planned.exit_code == 0, planned.output[-2000:]
    seen = _planned_selections(monkeypatch)
    announced = _announced(monkeypatch)
    applied = invoke_with_port(monkeypatch, port, ["apply", *_dir(project), "--yes"])
    assert applied.exit_code == 0, applied.output[-2000:]
    assert seen == []
    assert [line for line in announced if "writes nothing" in line]


def test_a_saved_write_that_no_longer_matches_falls_back_to_a_whole_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _two_skills(tmp_path, monkeypatch)
    (project / "skills" / "month-close" / "reference" / "steps.md").write_text("new steps\n", encoding="utf-8")
    assert CliRunner().invoke(cli, ["compile", *_dir(project)]).exit_code == 0
    planned = invoke_with_port(monkeypatch, port, ["plan", *_dir(project), "-o", "json"])
    assert planned.exit_code == 2, planned.output[-2000:]
    saved = project / "sst_target" / "plan.json"
    document: dict[str, Any] = json.loads(saved.read_text(encoding="utf-8"))
    writes = [change["key"] for change in document["changes"] if change["action"] == "update"]
    for change in document["changes"]:
        if change["action"] == "update":
            change["statement_hashes"] = ["0" * 64]
    body = {key: value for key, value in document.items() if key != "plan_id"}
    saved.write_text(json.dumps({**body, "plan_id": content_hash(body)}), encoding="utf-8")
    seen = _planned_selections(monkeypatch)
    announced = _announced(monkeypatch)
    applied = invoke_with_port(monkeypatch, port, ["apply", *_dir(project), "--yes"])
    assert applied.exit_code == 0, applied.output[-2000:]
    assert seen == [tuple(writes), ()]
    assert f"planning ({writes[0]} changed in Snowflake since sst plan)" in announced
