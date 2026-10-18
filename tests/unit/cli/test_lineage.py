"""`sst lineage`: one node's lineage from the compiled manifest, each way, marked with what changed."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, compile_project
from tests.helpers.reference_project import project_copy


def _lineage(project: Path, *args: str, output: str = "json") -> Any:
    result = CliRunner().invoke(cli, ["lineage", "--project-dir", str(project), *args, "-o", output])
    assert result.exit_code == 0, result.output
    return json.loads(result.output)["data"] if output == "json" else result.output


def test_a_routine_reads_the_model_its_body_refs_and_an_agent_calls_it(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    data = _lineage(project, "tool:order_status")
    nodes = {(item["key"], item["direction"], item["depth"]) for item in data["nodes"]}
    assert {("tool:order_status", "self", 0), ("model:orders", "upstream", 1)} <= nodes
    assert ("agent:jaffle_analytics_agent", "downstream", 1) in nodes
    assert {"from": "model:orders", "to": "tool:order_status"} in data["edges"]
    upstream = _lineage(project, "tool:order_status", "--upstream")
    assert {item["direction"] for item in upstream["nodes"]} == {"self", "upstream"}
    near = _lineage(project, "metric:total_revenue", "--downstream", "--depth", "1")
    assert {item["depth"] for item in near["nodes"]} == {0, 1} and "changed" not in near["nodes"][0]


def test_state_marks_what_changed_since_another_manifest(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    state = tmp_path / "state"
    state.mkdir()
    shutil.copy(project / "target" / "sst" / "manifest.json", state / "manifest.json")
    metric = project / "semantic_models" / "metrics" / "line_item_count.yml"
    metric.write_text(metric.read_text(encoding="utf-8").replace("COUNT({{", "COUNT(DISTINCT {{", 1), encoding="utf-8")
    compile_project(project)
    data = _lineage(project, "metric:line_item_count", "--state", str(state))
    changed = {item["key"] for item in data["nodes"] if item["changed"]}
    assert {"metric:line_item_count", "semantic_view:jaffle_menu"} <= changed
    assert "model:order_items" not in changed
    agent = _lineage(project, "metric:line_item_count", "--state", str(state), output="agent")
    assert "self 0 metric:line_item_count metric changed" in agent


def test_without_the_manifest_the_project_is_compiled_in_memory(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(cli, ["lineage", *common(project), "model:orders", "--no-manifest", "-o", "json"])
    assert result.exit_code == 0, result.output
    assert not (project / "target" / "sst" / "manifest.json").exists()
    missing = CliRunner().invoke(cli, ["lineage", "--project-dir", str(project), "model:orders", "-o", "json"])
    assert missing.exit_code == 4
    absent = CliRunner().invoke(
        cli, ["lineage", "--project-dir", str(project), "model:orders", "--state", str(tmp_path)]
    )
    assert absent.exit_code == 4
