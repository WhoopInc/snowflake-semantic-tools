"""dbt's graph operators in `--select` and `--exclude`, walking the lineage the reference project records."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common, compile_project
from tests.helpers.reference_project import project_copy


def _listed(project: Path, *selectors: str) -> list[str]:
    result = CliRunner().invoke(cli, ["list", "--project-dir", str(project), *selectors, "-o", "json"])
    assert result.exit_code == 0, result.output
    return [item["key"] for item in json.loads(result.output)["data"]["items"]]


def test_a_model_selects_every_artifact_it_feeds_through_routine_bodies_members_and_agents(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    fed = _listed(project, "--select", "model:orders+")
    assert {"tool:order_status", "semantic_view:jaffle_sales", "agent:jaffle_analytics_agent"} <= set(fed)
    assert "tool:menu_docs_search" not in fed
    assert _listed(project, "--select", "1+tool:order_status") == ["tool:order_status"]
    reads = _listed(project, "--select", "+agent:jaffle_analytics_agent", "--exclude", "type:semantic_view")
    assert "tool:order_status" in reads and not any(key.startswith("semantic_view:") for key in reads)
    assert set(_listed(project, "--select", "metric:total_revenue+")) >= {"semantic_view:jaffle_sales"}


def test_compile_and_validate_walk_the_lineage_of_the_project_as_it_compiles(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    exit_code, diagnostics = invoke_json(["validate", *common(project), "--no-strict", "--select", "model:orders+"])
    assert exit_code == 0, [item for item in diagnostics if item["severity"] == "error"]
    compiled = CliRunner().invoke(cli, ["compile", *common(project), "--select", "+tool:order_status", "-o", "json"])
    assert compiled.exit_code == 0, compiled.output
    assert [item["artifact_key"] for item in json.loads(compiled.output)["data"]["artifacts"]] == ["tool:order_status"]


def test_a_dbt_node_or_a_member_named_bare_is_refused_with_the_operator_to_write(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    result = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--select", "model:orders", "-o", "json"])
    [diagnostic] = json.loads(result.output)["diagnostics"]
    assert (result.exit_code, diagnostic["code"]) == (3, "SST-PRT100")
    assert "write model:orders+ for what reads it" in diagnostic["message"]
    nothing = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--select", "model:nope+", "-o", "json"])
    assert [item["code"] for item in json.loads(nothing.output)["diagnostics"]] == ["SST-DIS010"]
