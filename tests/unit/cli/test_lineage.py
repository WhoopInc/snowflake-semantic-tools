"""`sst lineage`: one node's lineage from the compiled manifest, each way, marked with what changed."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.state.documents import content_hash
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
    assert {"from": "model:orders", "to": "tool:order_status", "kind": "reads"} in data["edges"]
    assert {"from": "tool:order_status", "to": "agent:jaffle_analytics_agent", "kind": "calls"} in data["edges"]
    upstream = _lineage(project, "tool:order_status", "--upstream")
    assert {item["direction"] for item in upstream["nodes"]} == {"self", "upstream"}
    near = _lineage(project, "metric:total_revenue", "--downstream", "--depth", "1")
    assert {item["depth"] for item in near["nodes"]} == {0, 1} and "changed" not in near["nodes"][0]


def test_state_marks_what_changed_since_another_manifest(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    state = tmp_path / "state"
    state.mkdir()
    shutil.copy(project / "sst_target" / "manifest.json", state / "manifest.json")
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
    assert not (project / "sst_target" / "manifest.json").exists()
    missing = CliRunner().invoke(cli, ["lineage", "--project-dir", str(project), "model:orders", "-o", "json"])
    assert missing.exit_code == 4
    absent = CliRunner().invoke(
        cli, ["lineage", "--project-dir", str(project), "model:orders", "--state", str(tmp_path)]
    )
    assert absent.exit_code == 4


def test_each_output_says_where_a_walk_ends_and_plain_prints_one_signed_depth_per_node(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    plain = _lineage(project, "agent:jaffle_analytics_agent", "--upstream", "--depth", "1", output="plain")
    assert "0 agent:jaffle_analytics_agent" in plain and "-1 tool_ref:order_tier_lookup" in plain
    table = _lineage(project, "agent:jaffle_analytics_agent", "--upstream", "--depth", "1", output="table")
    assert "─ tool_ref:order_tier_lookup  (owned by another team)" in table
    agent = _lineage(project, "agent:jaffle_analytics_agent", "--upstream", "--depth", "1", output="agent")
    assert "upstream 1 tool_ref:order_tier_lookup tool_ref | owned by another team" in agent
    data = _lineage(project, "tool_ref:order_tier_lookup")
    assert data["dbt_graph"] is True and data["nodes"][0]["marker"] == "owned by another team"


def test_a_manifest_without_dbt_s_graph_says_to_compile_again(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    path = project / "sst_target" / "manifest.json"
    document = {**json.loads(path.read_text(encoding="utf-8")), "dbt_nodes": {}}
    material = {key: value for key, value in document.items() if key not in ("manifest_id", "schema_version")}
    path.write_text(json.dumps({**document, "manifest_id": content_hash(material)}), encoding="utf-8")
    assert "run `sst compile` to trace through dbt" in _lineage(project, "tool:order_status", output="table")
    assert _lineage(project, "tool:order_status")["dbt_graph"] is False


def test_a_tree_has_connectors_headings_and_with_wide_aligned_names_and_edges(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    narrow = _lineage(project, "tool:order_status", output="table").splitlines()
    assert narrow[:3] == ["tool:order_status", "├── reads", "│   └── model:orders"]
    assert narrow[3:6] == [
        "└── read by",
        "    └── agent:jaffle_analytics_agent",
        "        └── eval:jaffle_analytics_agent",
    ]
    wide = _lineage(project, "tool:order_status", "--wide", output="table").splitlines()
    assert wide[2].split() == ["│", "└──", "model:orders", "SST_REF_DEV.JAFFLE.ORDERS", "read", "by", "it"]
    assert wide[4].endswith("calls it as a tool") and wide[5].endswith("tests it")
    columns = {line.index("SST_REF_DEV") for line in wide if "SST_REF_DEV" in line}
    assert len(columns) == 1
    # A trace from a dimension is column lineage, so it is wide without the flag.
    dimension = _lineage(project, "dimension:orders.order_id", "--depth", "1", output="table")
    assert "SST_REF_DEV.JAFFLE.ORDERS" in dimension and "holds it" in dimension
