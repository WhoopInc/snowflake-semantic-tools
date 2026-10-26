"""Whether a bare `sst apply` may reuse the plan `sst plan` saved, for each reason it may not."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.plan import PlanRequest
from snowflake_semantic_tools.cli.wiring.saved_plan import Reuse, reusable_plan
from snowflake_semantic_tools.domain.state import content_hash
from tests.helpers.cli_projects import invoke_with_port, skill_project
from tests.helpers.projects import project_paths
from tests.helpers.snowflake_fake import FakeSnowflake


def _dir(project: Path) -> list[str]:
    return ["--project-dir", str(project)]


def _request(project: Path) -> PlanRequest:
    return PlanRequest(project_paths(project), None, None, (), (), False, False, None, None, threads=1)


def _saved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, FakeSnowflake]:
    """A skill applied, then edited, compiled and planned: `sst_target/plan.json` holds one update."""
    project = skill_project(tmp_path / "skills")
    port = FakeSnowflake(state={})
    first = invoke_with_port(monkeypatch, port, ["apply", *_dir(project), "--yes", "--replan", "-o", "json"])
    assert first.exit_code == 0, first.output[-2000:]
    (project / "skills" / "month-close" / "reference" / "steps.md").write_text("new steps\n", encoding="utf-8")
    assert CliRunner().invoke(cli, ["compile", *_dir(project)]).exit_code == 0
    planned = invoke_with_port(monkeypatch, port, ["plan", *_dir(project), "-o", "json"])
    assert planned.exit_code == 2, planned.output[-2000:]
    return project, port


def _rewrite(project: Path, edit: Callable[[dict[str, Any]], None]) -> None:
    """Edit the saved plan, keeping its id the hash of its content so it still reads."""
    path = project / "sst_target" / "plan.json"
    document: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    edit(document)
    body = {key: value for key, value in document.items() if key != "plan_id"}
    path.write_text(json.dumps({**body, "plan_id": content_hash(body)}), encoding="utf-8")


def test_reuse_is_off_when_the_project_sets_reuse_plan_minutes_to_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, _ = _saved(tmp_path, monkeypatch)
    config = project / "sst_config.yml"
    config.write_text(config.read_text(encoding="utf-8") + "apply:\n  reuse_plan_minutes: 0\n", encoding="utf-8")
    assert reusable_plan(_request(project), flagged=False) == "apply.reuse_plan_minutes is 0"


def test_a_saved_plan_that_cannot_be_read_is_not_reused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, _ = _saved(tmp_path, monkeypatch)
    (project / "sst_target" / "plan.json").write_text("not a plan", encoding="utf-8")
    assert reusable_plan(_request(project), flagged=False) == "the saved plan cannot be read"


def _selected(document: dict[str, Any]) -> None:
    document["selection"]["selected"] = ["skill:month-close"]


def _other_target(document: dict[str, Any]) -> None:
    document["target"]["name"] = "prod"


def _recompiled(document: dict[str, Any]) -> None:
    document["manifest_id"] = "b" * 64


def _no_observation_time(document: dict[str, Any]) -> None:
    document["observation_at"] = "not a time"


@pytest.mark.parametrize(
    ("edit", "reason"),
    [
        (_selected, "the saved plan was made with a selection or flags"),
        (_other_target, "the saved plan is for target 'prod'"),
        (_recompiled, "the project was compiled since the plan was saved"),
        (_no_observation_time, "the saved plan records no observation time"),
    ],
)
def test_a_saved_plan_is_not_reused_for_each_reason_it_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, edit: Callable[[dict[str, Any]], None], reason: str
) -> None:
    project, _ = _saved(tmp_path, monkeypatch)
    _rewrite(project, edit)
    assert reusable_plan(_request(project), flagged=False) == reason


def test_an_observation_time_without_a_zone_is_read_as_utc(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, _ = _saved(tmp_path, monkeypatch)
    naive = (datetime.now(UTC) - timedelta(minutes=5)).replace(tzinfo=None).isoformat()
    _rewrite(project, lambda document: document.__setitem__("observation_at", naive))
    reuse = reusable_plan(_request(project), flagged=False)
    assert isinstance(reuse, Reuse) and 4 <= reuse.minutes <= 6
