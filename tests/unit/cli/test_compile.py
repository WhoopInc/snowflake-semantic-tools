"""`sst compile`: rendering, the manifests, selection, partial runs, and projects without dbt."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import (
    FIXTURE,
    MANIFEST,
    break_menu_view,
    common,
    profile_with_commands_and_plugin,
    project_copy,
    skills_only_project,
)


def test_compile_accepts_an_explicit_manifest_without_invoking_dbt(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--select",
            "jaffle_minimal",
            "--emit-ddl",
            str(tmp_path / "ddl"),
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)["data"]
    assert (data["ddl_files"], data["artifact_counts"]) == (["jaffle_minimal.sql"], {"semantic_view": 1})
    ddl = (tmp_path / "ddl" / "jaffle_minimal.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL" in ddl


def test_compile_rejects_an_unknown_target_before_rendering() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--target",
            "does_not_exist",
        ],
    )
    assert result.exit_code != 0
    assert "target 'does_not_exist' is absent from profile 'sst_reference_impl'" in result.output


def test_compile_writes_one_deterministic_file_per_view(tmp_path: Path) -> None:
    output_dir = tmp_path / "ddl"
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--select",
            "jaffle_minimal",
            "--emit-ddl",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert sorted(path.name for path in output_dir.iterdir()) == ["jaffle_minimal.sql"]
    ddl = (output_dir / "jaffle_minimal.sql").read_text(encoding="utf-8")
    assert ddl.startswith("CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL")
    assert ddl.endswith(";\n")


def test_compile_writes_deterministic_manifest(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(cli, ["compile", *common(project)])
    assert result.exit_code == 0, result.output
    document = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    assert document["schema_version"] == 2
    assert sorted(document["artifacts"]) == [
        "agent:jaffle_analytics_agent",
        "agent:jaffle_delivery_agent",
        "agent:jaffle_minimal_agent",
        "eval:jaffle_analytics_agent",
        "plugin:jaffle-toolkit",
        "profile:jaffle-analyst",
        "profile:jaffle-operator",
        "semantic_view:jaffle_menu",
        "semantic_view:jaffle_minimal",
        "semantic_view:jaffle_sales",
        "skill:jaffle-catalogue",
        "skill:jaffle-operations",
        "skill:jaffle-semantics",
        "tool:menu_docs_search",
    ]
    assert "generated_at" not in document


def test_compile_json_emits_artifact_fingerprints() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert len(envelope["data"]["artifacts"]) == 14
    assert all(len(artifact["fingerprint"]) == 64 for artifact in envelope["data"]["artifacts"])


def test_compile_json_honors_selection() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(FIXTURE),
            "--manifest",
            str(MANIFEST),
            "--select",
            "jaffle_minimal",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    artifacts = json.loads(result.output)["data"]["artifacts"]
    assert [artifact["artifact_key"] for artifact in artifacts] == ["semantic_view:jaffle_minimal"]


def test_compile_emits_each_agent_specification_and_nothing_else(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    specs = tmp_path / "agents"
    result = CliRunner().invoke(cli, ["compile", *common(project), "--emit-agent-spec", str(specs)])
    assert result.exit_code == 0, result.output
    assert sorted(path.name for path in specs.iterdir()) == [
        "jaffle_analytics_agent.json",
        "jaffle_delivery_agent.json",
        "jaffle_minimal_agent.json",
    ]
    assert f"wrote 3 file(s) to {specs}" in result.output
    database = CliRunner().invoke(cli, ["compile", *common(project), "--database", "ELSEWHERE", "--output", "json"])
    targets = {item["artifact_key"]: item["target"] for item in json.loads(database.output)["data"]["artifacts"]}
    assert targets["semantic_view:jaffle_minimal"].startswith("ELSEWHERE.")


def test_compile_selection_keeps_the_canonical_manifest_full(tmp_path: Path) -> None:
    import shutil

    project = tmp_path / "project"
    shutil.copytree(FIXTURE, project)
    shutil.rmtree(project / "target", ignore_errors=True)
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(project),
            "--manifest",
            str(MANIFEST),
            "--select",
            "jaffle_minimal",
        ],
    )
    assert result.exit_code == 0, result.output
    canonical = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    assert len(canonical["artifacts"]) == 14


@pytest.mark.parametrize(
    ("file", "before", "after", "code"),
    [
        ("semantic_models/filters/filters.yml", "var('completed_state')", "var('no_such_var')", "SST-REF038"),
        (
            "semantic_models/semantic_views/semantic_views.yml",
            "custom_instructions('jaffle_sql_conventions')",
            "custom_instructions('no_such_instruction')",
            "SST-REF039",
        ),
        ("semantic_models/semantic_views/semantic_views.yml", "tag('cost_center')", "tag('no_such_tag')", "SST-REF040"),
    ],
)
def test_unknown_references_name_their_own_code(tmp_path: Path, file: str, before: str, after: str, code: str) -> None:
    project = project_copy(tmp_path)
    path = project / file
    text = path.read_text(encoding="utf-8")
    assert before in text
    path.write_text(text.replace(before, after, 1), encoding="utf-8")
    result = CliRunner().invoke(cli, ["compile", *common(project), "--output", "json"])
    assert result.exit_code == 1, result.output
    codes = [item["code"] for item in json.loads(result.output)["diagnostics"]]
    assert code in codes and "SST-INT902" not in codes


def test_a_partial_run_still_stops_on_an_error_that_names_no_artifact(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    break_menu_view(project)
    config = project / "sst_config.yml"
    text = config.read_text(encoding="utf-8")
    assert "  hooks_dir: hooks\n" in text
    config.write_text(text.replace("  hooks_dir: hooks\n", "  hooks_dir: nowhere\n", 1), encoding="utf-8")
    result = CliRunner().invoke(cli, ["compile", *common(project), "--partial", "--output", "json"])
    assert result.exit_code == 1
    assert "SST-CFG047" in {item["code"] for item in json.loads(result.output)["diagnostics"]}
    assert not (project / "target" / "sst" / "manifest.json").exists()


def test_a_partial_run_refuses_to_publish_a_view_without_a_broken_member(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    metrics = project / "semantic_models" / "metrics" / "metrics.yml"
    text = metrics.read_text(encoding="utf-8")
    expression = "expr: \"SUM({{ ref('orders', 'order_total') }})\""
    assert expression in text
    metrics.write_text(
        text.replace(expression, "expr: \"SUM({{ ref('orders', 'order_total') }}) * {{ var('nope') }}\"", 1),
        encoding="utf-8",
    )
    # The failing metric would silently leave every view it belongs to, so no split
    # is safe: nothing is published, and the run says why.
    result = CliRunner().invoke(cli, ["compile", *common(project), "--partial", "--output", "json"])
    assert result.exit_code == 1
    diagnostics = json.loads(result.output)["diagnostics"]
    assert {"SST-REF038", "SST-PLN033"} <= {item["code"] for item in diagnostics}
    refusal = next(item for item in diagnostics if item["code"] == "SST-PLN033")
    assert "metric:total_revenue" in refusal["message"]
    assert not (project / "target" / "sst" / "manifest.json").exists()


def test_project_without_dbt_compiles_from_target_profile(tmp_path: Path) -> None:
    project = skills_only_project(tmp_path / "skills")
    result = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["data"]["artifacts"] == []
    manifest = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["sources"]["dbt_manifest"]["path"] == ""
    assert manifest["sources"]["dbt_manifest"]["model_count"] == 0


def test_project_without_dbt_refuses_dbt_only_configuration(tmp_path: Path) -> None:
    project = skills_only_project(tmp_path / "skills", "project:\n  target_profile: skills\nsemantic_views: {}\n")
    result = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert result.exit_code == 1
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == ["SST-CFG046"]


def test_project_without_dbt_or_target_profile_is_a_config_error(tmp_path: Path) -> None:
    project = skills_only_project(tmp_path / "skills", "enrichment: {}\n")
    result = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert result.exit_code == 4
    assert "project.target_profile" in json.loads(result.output)["data"]["error"]


def test_a_configured_directory_that_does_not_exist_is_an_error(tmp_path: Path) -> None:
    config = "project:\n  target_profile: skills\n  profiles_dir: profilez\n  hooks_dir: hooks\n"
    project = skills_only_project(tmp_path / "skills", config)
    (project / "hooks").mkdir()
    result = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert result.exit_code == 1
    diagnostics = json.loads(result.output)["diagnostics"]
    assert [item["code"] for item in diagnostics] == ["SST-CFG047"]
    assert diagnostics[0]["message"] == "project.profiles_dir is profilez, which is not a directory in the project"

    # A dbt-only directory in a project without dbt is reported once, as CFG046.
    dbt_only = skills_only_project(tmp_path / "other", "project:\n  target_profile: skills\n  agents_dir: nowhere\n")
    result = CliRunner().invoke(cli, ["compile", "--project-dir", str(dbt_only), "--output", "json"])
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == ["SST-CFG046"]


def test_a_plugin_with_a_broken_member_blocks_the_profile(tmp_path: Path) -> None:
    project = profile_with_commands_and_plugin(tmp_path / "skills")
    (project / "skills" / "month-close" / "SKILL.md").write_text(
        "---\nname: month-close\ndescription: Close the month.\n---\nRead reference/missing.md.\n", encoding="utf-8"
    )
    result = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert result.exit_code == 1
    diagnostics = json.loads(result.output)["diagnostics"]
    # Without a catalog channel the skill ships nested in the profile, but the plugin copy
    # is the flattened extension bundle, so its dangling reference blocks the plugin.
    assert {"SST-VAL808", "SST-VAL836"} <= {item["code"] for item in diagnostics}
    blocked = [item["message"] for item in diagnostics if item["code"] == "SST-VAL855"]
    assert blocked == ["profile 'analyst': plugin 'kit' has errors, so the profile cannot publish"]
