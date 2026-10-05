"""`agents:` folder routes, end to end on the reference project: one routed location everywhere."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, invoke_with_port
from tests.helpers.reference_project import project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

ROUTED = "SST_REF_DEV.OPS.JAFFLE_MINIMAL_AGENT"


def _run(*args: str) -> dict[str, Any]:
    result = CliRunner().invoke(cli, [*args, "--output", "json"])
    envelope: dict[str, Any] = json.loads(result.output)
    return envelope


def _replace(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    assert old in text
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def test_compile_list_and_the_manifest_carry_the_routed_location(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compiled = _run("compile", *common(project))
    assert compiled["exit_code"] == 0, compiled["diagnostics"]
    targets = {item["artifact_key"]: item["target"] for item in compiled["data"]["artifacts"]}
    assert targets["agent:jaffle_minimal_agent"] == ROUTED
    # The agents outside agents/ops/ keep the block's schema.
    assert targets["agent:jaffle_analytics_agent"] == "SST_REF_DEV.JAFFLE.JAFFLE_ANALYTICS_AGENT"

    manifest = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    entry = manifest["artifacts"]["agent:jaffle_minimal_agent"]
    assert entry["publish_target"]["qualified_name"] == ROUTED
    assert entry["source_files"] == ["agents/ops/jaffle_minimal/agent.yml"]

    listed = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--select", "agent:jaffle_minimal_agent"])
    assert listed.exit_code == 0, listed.output
    assert ROUTED in listed.output


def test_plan_creates_the_routed_agent_at_its_routed_location(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    result = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        ["plan", *common(project), "--target", "dev", "--no-plan-out", "--output", "json"],
    )
    payload = json.loads(result.output)
    [change] = [item for item in payload["data"]["changes"] if item["artifact_key"] == "agent:jaffle_minimal_agent"]
    assert (change["action"], change["reason"]) == ("create", "not_present")
    assert ROUTED in json.dumps(change)
    assert "SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL_AGENT" not in result.output


def test_a_nested_route_overrides_only_the_keys_it_sets(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    nested = project / "agents" / "ops" / "core" / "jaffle_minimal"
    nested.parent.mkdir()
    (project / "agents" / "ops" / "jaffle_minimal").rename(nested)
    _replace(
        project / "sst_config.yml",
        "  ops:\n    +schema: OPS\n",
        "  ops:\n    +schema: OPS\n    +secure: true\n    core:\n      +database: ELSEWHERE\n",
    )
    compiled = _run("compile", *common(project))
    targets = {item["artifact_key"]: item["target"] for item in compiled["data"]["artifacts"]}
    # +database from the nested route, +schema from its parent: the fold is per key.
    assert targets["agent:jaffle_minimal_agent"] == "ELSEWHERE.OPS.JAFFLE_MINIMAL_AGENT"


def test_a_route_to_a_missing_agent_folder_is_an_error(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    _replace(
        project / "sst_config.yml", "  ops:\n    +schema: OPS\n", "  ops:\n    +schema: OPS\n  opz:\n    +schema: X\n"
    )
    compiled = _run("compile", *common(project))
    assert compiled["exit_code"] == 1
    [problem] = [item for item in compiled["diagnostics"] if item["code"] == "SST-CFG041"]
    assert problem["message"] == f"config key 'opz' in block 'agents' names no directory under '{project / 'agents'}'"
