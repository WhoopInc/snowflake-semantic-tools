"""`sst list`: compiled artifacts, and the error without a compiled manifest."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.commands.list import LIST_TYPES, type_name
from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, compile_project, project_copy


def test_type_names_are_the_registered_types_in_the_plural_and_hyphenated() -> None:
    assert (type_name("semantic_view"), type_name("verified_query"), type_name("agent")) == (
        "semantic-views",
        "verified-queries",
        "agents",
    )
    assert {
        "metrics",
        "relationships",
        "filters",
        "semantic-views",
        "custom-instructions",
        "verified-queries",
        "tables",
        "agents",
        "evals",
        "skills",
        "tools",
    } <= set(LIST_TYPES)
    assert LIST_TYPES["semantic-views"] == ("artifact", "semantic_view")
    assert LIST_TYPES["metrics"] == ("member", "metric")


def test_list_members_and_tables_with_what_carries_or_reads_them(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    metrics = CliRunner().invoke(cli, ["list", "metrics", "--project-dir", str(project), "-o", "json"])
    assert metrics.exit_code == 0, metrics.output
    data = json.loads(metrics.output)["data"]
    assert data["type"] == "metrics" and data["count"] == len(data["items"]) > 0
    product_count = next(item for item in data["items"] if item["key"] == "metric:product_count")
    assert product_count["name"] == "product_count" and "semantic_view:jaffle_minimal" in product_count["artifacts"]
    assert all(item["key"].startswith("metric:") for item in data["items"])

    narrowed = CliRunner().invoke(
        cli, ["list", "metrics", "--project-dir", str(project), "--select", "jaffle_minimal", "-o", "json"]
    )
    keys = [item["key"] for item in json.loads(narrowed.output)["data"]["items"]]
    assert "metric:product_count" in keys and len(keys) < data["count"]
    excluded = CliRunner().invoke(
        cli, ["list", "metrics", "--project-dir", str(project), "--exclude", "type:semantic_view", "-o", "json"]
    )
    assert json.loads(excluded.output)["data"]["count"] == 0

    tables = CliRunner().invoke(cli, ["list", "tables", "--project-dir", str(project)])
    assert tables.exit_code == 0, tables.output
    assert "table:customers SST_REF_DEV.JAFFLE.CUSTOMERS semantic_view:jaffle_sales" in tables.output
    as_csv = CliRunner().invoke(cli, ["list", "tables", "--project-dir", str(project), "-o", "csv"])
    assert as_csv.output.splitlines()[0].startswith("key,name,artifacts")
    assert CliRunner().invoke(cli, ["list", "semantic_view", "--project-dir", str(project)]).exit_code == 3


def test_list_without_manifest_and_failed_compile_json(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    missing = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--output", "json"])
    assert missing.exit_code == 4
    payload = json.loads(missing.output)
    assert payload["exit_code"] == 4

    views = project / "semantic_models" / "semantic_views" / "semantic_views.yml"
    views.write_text(
        views.read_text(encoding="utf-8").replace("{{ ref('products') }}", "{{ ref('missing') }}", 1), encoding="utf-8"
    )
    failed = CliRunner().invoke(cli, ["compile", *common(project), "--output", "json"])
    assert failed.exit_code == 1
    assert json.loads(failed.output)["status"] == "error"
