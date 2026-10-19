"""`sst list`: compiled artifacts, and the error without a compiled manifest."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from snowflake_semantic_tools.cli.commands.list import LIST_TYPES, type_name
from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common, compile_project
from tests.helpers.reference_project import project_copy


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

    tables = CliRunner(env={"COLUMNS": "400"}).invoke(cli, ["list", "tables", "--project-dir", str(project)])
    assert tables.exit_code == 0, tables.output
    rows = [" ".join(line.split()) for line in tables.stdout.splitlines()]
    assert rows[0] == "MEMBER RELATION ARTIFACTS MEMBERS"
    assert any(
        row.startswith("table:customers SST_REF_DEV.JAFFLE.CUSTOMERS semantic_view:jaffle_sales dimension:customers.")
        for row in rows
    )
    as_csv = CliRunner().invoke(cli, ["list", "tables", "--project-dir", str(project), "-o", "csv"])
    assert as_csv.output.splitlines()[0].startswith("key,name,artifacts")
    assert CliRunner().invoke(cli, ["list", "semantic_view", "--project-dir", str(project)]).exit_code == 3


def test_list_without_manifest_and_failed_compile_json(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    missing = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--output", "json"])
    assert missing.exit_code == 4
    payload = json.loads(missing.output)
    assert payload["exit_code"] == 4

    views = project / "semantic_models" / "semantic_views" / "jaffle_minimal.yml"
    views.write_text(
        views.read_text(encoding="utf-8").replace("{{ ref('products') }}", "{{ ref('missing') }}", 1), encoding="utf-8"
    )
    failed = CliRunner().invoke(cli, ["compile", *common(project), "--output", "json"])
    assert failed.exit_code == 1
    assert json.loads(failed.output)["status"] == "error"


def test_list_long_prints_each_setting_with_where_it_came_from(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    run = ["list", "--project-dir", str(project), "--long"]
    printed = CliRunner().invoke(cli, run)
    assert printed.exit_code == 0, printed.output
    lines = printed.output.splitlines()
    menu = lines.index(next(line for line in lines if line.startswith("semantic_view:jaffle_menu ")))
    assert lines[menu + 1 : menu + 4] == [
        '    database="SST_REF_DEV" (root)',
        "    enabled=true (root)",
        '    schema="CORE" (route:core)',
    ]
    assert '    orchestration_model="claude-sonnet-4-6" (config)' in lines
    assert "    secure=false (default)" in lines

    listed = CliRunner().invoke(cli, [*run, "-o", "json"])
    items = {item["key"]: item for item in json.loads(listed.output)["data"]["items"]}
    assert items["semantic_view:jaffle_sales"]["settings"]["max_staleness"] == {"value": 300, "source": "config"}
    assert items["agent:jaffle_minimal_agent"]["settings"]["schema"] == {"value": "OPS", "source": "route:ops"}
    assert items["skill:jaffle-semantics"]["settings"]["bundle_stage"] == {
        "value": "SKILL_BUNDLE_SRC",
        "source": "root",
    }
    assert items["skill:jaffle-operations"]["settings"]["schema"] == {
        "value": "JAFFLE",
        "source": "route:jaffle-operations",
    }
    # A root-only key paces the run, not the skill, so no skill reports it.
    assert "threads" not in items["skill:jaffle-semantics"]["settings"]
    assert items["plugin:jaffle-toolkit"]["settings"]["certified"] == {"value": False, "source": "config"}
    assert items["profile:jaffle-analyst"]["settings"]["registry_table"]["source"] == "root"
    assert items["eval:jaffle_analytics_agent"]["settings"]["retention"] == {"value": "audit", "source": "config"}
    short = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "-o", "json"])
    assert all("settings" not in item for item in json.loads(short.output)["data"]["items"])


def _listed(project: Path, *selectors: str) -> list[str]:
    result = CliRunner().invoke(cli, ["list", "--project-dir", str(project), *selectors, "-o", "json"])
    assert result.exit_code == 0, result.output
    return [item["key"] for item in json.loads(result.output)["data"]["items"]]


def test_a_selector_value_holding_spaces_is_one_selector_per_word(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    apart = ["--select", "jaffle_minimal", "--select", "jaffle_menu"]
    assert _listed(project, "--select", "jaffle_minimal jaffle_menu") == _listed(project, *apart)
    assert len(_listed(project, *apart)) == 2
    assert _listed(project, "--exclude", " jaffle_minimal\tjaffle_menu ") == _listed(
        project, "--exclude", "jaffle_minimal", "--exclude", "jaffle_menu"
    )
    validated = [
        invoke_json(["validate", *common(project), "--no-strict", *selectors, "--show-info"])
        for selectors in (["--select", "jaffle_minimal jaffle_menu"], apart)
    ]
    assert validated[0] == validated[1] and validated[0][0] == 0


def test_list_no_manifest_reports_what_failed_to_compile(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    tool = project / "tools" / "platform" / "zz_dupe.yml"
    tool.write_text(
        "tool:\n  name: zz_dupe\n  type: procedure\n  description: Returns ok.\n  language: sql\n"
        "  returns: VARCHAR\n  returns: INT\n  body: \"BEGIN RETURN 'ok'; END\"\n",
        encoding="utf-8",
    )
    result = CliRunner().invoke(cli, ["list", "--no-manifest", *common(project), "-o", "json"])
    assert result.exit_code == 1, result.output
    envelope = json.loads(result.output)
    assert "SST-LOD005" in [item["code"] for item in envelope["diagnostics"]]
    assert "tool:zz_dupe" not in [item["key"] for item in envelope["data"]["items"]]


def test_list_no_manifest_resolves_names_for_the_target_given(tmp_path: Path) -> None:
    project = project_copy(tmp_path)

    def targets(*flags: str) -> set[str]:
        result = CliRunner().invoke(cli, ["list", "--no-manifest", *common(project), *flags, "-o", "json"])
        assert result.exit_code == 0, result.output
        return {item["target"].split(".")[0] for item in json.loads(result.output)["data"]["items"] if item["target"]}

    assert targets() == {"SST_REF_DEV"}
    assert targets("--target", "prod") == {"SST_REF_PROD"}


def test_list_refuses_a_target_the_compiled_manifest_was_not_compiled_for(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compiled = CliRunner().invoke(cli, ["compile", *common(project), "--target", "dev"])
    assert compiled.exit_code == 0, compiled.output
    assert CliRunner().invoke(cli, ["list", *common(project), "--target", "dev"]).exit_code == 0
    refused = CliRunner().invoke(cli, ["list", *common(project), "--target", "prod", "-o", "json"])
    assert refused.exit_code == 4, refused.output
    [diagnostic] = json.loads(refused.output)["diagnostics"]
    assert diagnostic["code"] == "SST-MAN006"
    assert diagnostic["message"] == "manifest target 'dev', current target 'prod'"


def test_a_member_row_shows_what_it_lives_in_and_uses_and_with_wide_its_columns_and_expression(
    tmp_path: Path,
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    data = json.loads(CliRunner().invoke(cli, ["list", "metrics", "--project-dir", str(project), "-o", "json"]).output)
    items = {item["key"]: item for item in data["data"]["items"]}
    assert items["metric:total_revenue"]["tables"] == ["orders"]
    assert items["metric:total_revenue"]["columns"] == ["orders.order_total"]
    assert items["metric:revenue_per_order"]["uses"] == ["metric:order_count", "metric:total_revenue"]
    assert items["metric:revenue_per_order"]["columns"] == []
    assert items["metric:total_revenue"]["expression"].startswith("SUM(")
    runner = CliRunner(env={"COLUMNS": "400"})
    narrow = runner.invoke(cli, ["list", "metrics", "--project-dir", str(project)]).stdout.splitlines()
    assert narrow[0].split() == ["MEMBER", "TABLES", "USES", "ARTIFACTS"]
    wide = runner.invoke(cli, ["list", "metrics", "--wide", "--project-dir", str(project)]).stdout.splitlines()
    assert wide[0].split() == ["MEMBER", "TABLES", "USES", "COLUMNS", "ARTIFACTS", "EXPRESSION"]
    total = next(line for line in wide if line.startswith("metric:total_revenue "))
    assert "orders.order_total" in total and "SUM(ORDERS.ORDER_TOTAL)" in total
    tables = json.loads(CliRunner().invoke(cli, ["list", "tables", "--project-dir", str(project), "-o", "json"]).output)
    orders = next(item for item in tables["data"]["items"] if item["key"] == "table:orders")
    assert "metric:total_revenue" in orders["members"] and "tables" not in orders
