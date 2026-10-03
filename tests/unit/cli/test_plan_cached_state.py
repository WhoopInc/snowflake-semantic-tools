"""`sst plan --use-cached-state`: plan from the observation an earlier plan recorded, never connecting."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.adapters.fs.local import observation_file
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.project import target_dir
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow, ShowRow
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.cli_projects import (
    break_menu_view,
    common,
    compile_project,
    invoke_with_port,
    project_copy,
    skill_project,
)
from tests.helpers.recorded_snowflake import RecordedSnowflake

VIEW = "jaffle_minimal"
RECORDED = "c" * 64
OTHER_MANIFEST = "d" * 64


class _Refusing:
    """A session that fails the test on any read at all."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"a plan from a recorded observation read {name} from Snowflake")


def _published(target: str) -> RecordedSnowflake:
    """Snowflake holding the view as an earlier manifest published it, with one explicit grant."""
    database, schema, name = target.split(".")
    row = ShowRow(name, database, schema, "OWNER", "now", f"[sst:{OTHER_MANIFEST}:{RECORDED}]")
    entry = AppliedEntry(RECORDED, target, "now", "run", "applied", RECORDED, OTHER_MANIFEST)
    return RecordedSnowflake(
        objects={("SEMANTIC VIEW", f"{database}.{schema}"): (row,)},
        grants={target: (GrantRow("SELECT", "ROLE", "ANALYST"),)},
        state={f"semantic_view:{VIEW}": entry},
    )


def _live(project: Path, monkeypatch: pytest.MonkeyPatch, port: RecordedSnowflake, *flags: str) -> Result:
    return invoke_with_port(monkeypatch, port, ["plan", *common(project), "--no-plan-out", "-o", "json", *flags])


def _cached(project: Path, monkeypatch: pytest.MonkeyPatch, *flags: str, state: Path | None = None) -> Result:
    """Plan from the recorded observation, with a connector that fails the test if it is ever used."""
    opened: list[object] = []

    def connector(params: object) -> _Refusing:
        opened.append(params)
        return _Refusing()

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", connector)
    args = ["plan", *common(project), "--use-cached-state", "--state", str(state or target_dir(project)), *flags]
    result = CliRunner().invoke(cli, [*args, "--no-plan-out"])
    assert opened == [], "a plan from a recorded observation connected to Snowflake"
    return result


def _target(project: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    first = _live(project, monkeypatch, RecordedSnowflake(state={}), "--select", VIEW)
    return str(json.loads(first.stdout)["data"]["changes"][0]["target"])


def _diagnostics(result: Result) -> list[dict[str, Any]]:
    return list(json.loads(result.stdout)["diagnostics"])


def test_a_live_plan_records_what_it_read_and_a_cached_plan_reuses_it_without_connecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    live = _live(project, monkeypatch, _published(_target(project, monkeypatch)), "--select", VIEW)
    assert live.exit_code == 2, live.output
    recorded = observation_file(target_dir(project), "dev")
    live_data = json.loads(live.stdout)["data"]
    assert live_data["observation"] == {
        "fetched_at": live_data["observation"]["fetched_at"],
        "cached": False,
        "age_seconds": None,
        "path": str(recorded),
    }
    assert recorded.is_file()
    cached = _cached(project, monkeypatch, "--select", VIEW, "-o", "json")
    assert cached.exit_code == 2, cached.output
    data = json.loads(cached.stdout)["data"]
    assert data["changes"] == live_data["changes"]
    assert data["changes"][0]["action"] == "update"
    observation = data["observation"]
    assert (observation["cached"], observation["fetched_at"]) == (True, live_data["observation"]["fetched_at"])
    assert observation["age_seconds"] >= 0
    assert observation["path"] == str(observation_file(target_dir(project), "dev"))
    [age] = [item for item in _diagnostics(cached) if item["message"].startswith("planned from the observation")]
    assert (age["code"], age["severity"]) == ("SST-PLN016", "info")
    assert f"recorded at {observation['fetched_at']}, 0h 00m old" in age["message"]
    # The recorded grants still report what a replace would drop, and SST-PLN018 never fires.
    codes = {item["code"] for item in _diagnostics(cached)}
    assert "SST-PLN013" in codes and "SST-PLN018" not in codes


def test_a_cached_plan_says_how_old_its_observation_is(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    _live(project, monkeypatch, RecordedSnowflake(state={}), "--select", VIEW)
    path = observation_file(target_dir(project), "dev")
    document = json.loads(path.read_text(encoding="utf-8"))
    document["fetched_at"] = "2020-01-01T00:00:00+00:00"
    path.write_text(json.dumps(document), encoding="utf-8")
    human = _cached(project, monkeypatch, "--select", VIEW)
    assert human.exit_code == 2, human.output
    line = "planned from the observation recorded at 2020-01-01T00:00:00+00:00 ("
    assert line in human.output and human.output.index(line) < human.output.index("Plan: 1 to create")
    data = json.loads(_cached(project, monkeypatch, "--select", VIEW, "-o", "json").stdout)["data"]
    assert data["observation"]["age_seconds"] > 365 * 24 * 3600
    assert data["changes"][0]["action"] == "create"
    document["fetched_at"] = "not a time"
    path.write_text(json.dumps(document), encoding="utf-8")
    unknown = _cached(project, monkeypatch, "--select", VIEW, "-o", "json")
    assert json.loads(unknown.stdout)["data"]["observation"]["age_seconds"] is None
    assert any("not a time, unknown old" in item["message"] for item in _diagnostics(unknown))


def test_a_cached_plan_needs_state_and_takes_no_flag_that_reads_the_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    without_state = CliRunner().invoke(cli, ["plan", *common(project), "--use-cached-state", "-o", "json"])
    assert without_state.exit_code == 3, without_state.output
    [refusal] = _diagnostics(without_state)
    assert (refusal["code"], refusal["message"]) == (
        "SST-PRT100",
        "--use-cached-state requires --state, the directory holding the recorded observation",
    )
    for flag in ("--snowflake-syntax-check", "--capture-prior"):
        refused = _cached(project, monkeypatch, flag, "-o", "json")
        assert refused.exit_code == 3, refused.output
        assert [item["code"] for item in _diagnostics(refused)] == ["SST-PRT104"]


def test_a_cached_plan_refuses_a_missing_or_foreign_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    empty = tmp_path / "previous"
    empty.mkdir()
    missing = _cached(project, monkeypatch, "--select", VIEW, "-o", "json", state=empty)
    assert missing.exit_code == 4, missing.output
    [diagnostic] = _diagnostics(missing)
    assert diagnostic["code"] == "SST-PRT009"
    assert diagnostic["message"] == (
        f"could not read {empty / 'observation.dev.json'}: no observation is recorded there; "
        "run sst plan without --use-cached-state to record one"
    )
    _live(project, monkeypatch, RecordedSnowflake(state={}), "--select", VIEW)
    path = observation_file(target_dir(project), "dev")
    document = json.loads(path.read_text(encoding="utf-8"))
    database = document["target"]["database"]
    document["target"]["database"] = "ELSEWHERE"
    path.write_text(json.dumps(document), encoding="utf-8")
    foreign = _cached(project, monkeypatch, "--select", VIEW, "-o", "json")
    assert foreign.exit_code == 4, foreign.output
    assert [(item["code"], item["message"]) for item in _diagnostics(foreign)] == [
        ("SST-MAN025", f"{path} was written by target dev for ELSEWHERE.JAFFLE")
    ]
    document["target"]["database"] = database
    document["declared_account"] = "another_account"
    path.write_text(json.dumps(document), encoding="utf-8")
    other_account = _cached(project, monkeypatch, "--select", VIEW, "-o", "json")
    assert other_account.exit_code == 4, other_account.output
    assert [(item["code"], item["message"]) for item in _diagnostics(other_account)] == [
        ("SST-MAN025", f"{path} was written by target dev on account another_account")
    ]
    path.write_text("not json", encoding="utf-8")
    unreadable = _cached(project, monkeypatch, "--select", VIEW, "-o", "json")
    assert unreadable.exit_code == 4, unreadable.output
    assert [item["code"] for item in _diagnostics(unreadable)] == ["SST-PRT009"]


def test_a_cached_plan_refuses_what_fails_validation_without_connecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    _live(project, monkeypatch, RecordedSnowflake(state={}))
    strict = _cached(project, monkeypatch, "--strict", "-o", "json")
    assert strict.exit_code == 1, strict.output
    assert "SST-PLN016" not in {item["code"] for item in _diagnostics(strict)}
    break_menu_view(project)
    refused = _cached(project, monkeypatch, "--no-validate", "-o", "json")
    assert refused.exit_code == 1, refused.output
    codes = [item["code"] for item in _diagnostics(refused)]
    assert codes and "SST-PLN016" not in codes


def test_a_plan_whose_read_was_refused_records_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    target = _target(project, monkeypatch)
    observation_file(target_dir(project), "dev").unlink()
    refused = _published(target)
    refused.definitions.clear()
    result = _live(project, monkeypatch, refused, "--select", VIEW, "--capture-prior")
    assert "SST-PLN001" in {item["code"] for item in _diagnostics(result)}
    assert json.loads(result.stdout)["data"]["observation"]["path"] is None
    assert not observation_file(target_dir(project), "dev").exists()


def test_a_cached_plan_blocks_a_composite_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = skill_project(tmp_path / "skills")
    assert CliRunner().invoke(cli, ["compile", "--project-dir", str(project)]).exit_code == 0
    live = invoke_with_port(
        monkeypatch, RecordedSnowflake(state={}), ["plan", "--project-dir", str(project), "--no-plan-out"]
    )
    assert live.exit_code == 2, live.output
    args = ["--use-cached-state", "--state", str(target_dir(project)), "--no-plan-out", "-o", "json"]
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: _Refusing())
    cached = CliRunner().invoke(cli, ["plan", "--project-dir", str(project), *args])
    assert cached.exit_code == 1, cached.output
    blocked = [item for item in _diagnostics(cached) if item["code"] == "SST-PLN001"]
    assert blocked and all("a recorded observation holds no composite artifact" in item["message"] for item in blocked)
    assert {item["action"] for item in json.loads(cached.stdout)["data"]["changes"]} == {"blocked"}
