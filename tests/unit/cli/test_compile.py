"""`sst compile`: rendering, the manifests, selection, partial runs, and projects without dbt."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.commands.compile import _payload_files
from snowflake_semantic_tools.cli.main import cli
from tests.helpers.cli_projects import common, profile_with_commands_and_plugin, skills_only_project
from tests.helpers.cowork_profiles import compile_profiles, cowork_profile, described_agent
from tests.helpers.reference_project import DBT_MANIFEST, break_menu_view, project_copy


def test_compile_accepts_an_explicit_manifest_without_invoking_dbt(tmp_path: Path) -> None:
    project = project_copy(tmp_path, offline=False)
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(project),
            "--manifest",
            str(DBT_MANIFEST),
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
    assert (data["ddl_files"], data["artifact_counts"]) == (["semantic_view/jaffle_minimal.sql"], {"semantic_view": 1})
    ddl = (tmp_path / "ddl" / "semantic_view" / "jaffle_minimal.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL" in ddl


def test_compile_rejects_an_unknown_target_before_rendering(tmp_path: Path) -> None:
    project = project_copy(tmp_path, offline=False)
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(project),
            "--manifest",
            str(DBT_MANIFEST),
            "--target",
            "does_not_exist",
        ],
    )
    assert result.exit_code != 0
    assert "target 'does_not_exist' is absent from profile 'sst_reference_impl'" in result.output


def test_compile_writes_one_deterministic_file_per_view(tmp_path: Path) -> None:
    project = project_copy(tmp_path, offline=False)
    output_dir = tmp_path / "ddl"
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(project),
            "--manifest",
            str(DBT_MANIFEST),
            "--select",
            "jaffle_minimal",
            "--emit-ddl",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert sorted(path.relative_to(output_dir).as_posix() for path in output_dir.rglob("*.sql")) == [
        "semantic_view/jaffle_minimal.sql"
    ]
    ddl = (output_dir / "semantic_view" / "jaffle_minimal.sql").read_text(encoding="utf-8")
    assert ddl.startswith("CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL")
    assert ddl.endswith(";\n")


def test_compile_writes_deterministic_manifest(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(cli, ["compile", *common(project)])
    assert result.exit_code == 0, result.output
    document = json.loads((project / "sst_target" / "manifest.json").read_text(encoding="utf-8"))
    assert document["schema_version"] == 6
    assert "impact" not in document
    assert sorted(document["artifacts"]) == [
        "agent:jaffle_analytics_agent",
        "agent:jaffle_delivery_agent",
        "agent:jaffle_minimal_agent",
        "cowork_profile:jaffle_concierge",
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
        "tool:order_status",
    ]
    assert "generated_at" not in document


def test_compile_json_emits_artifact_fingerprints(tmp_path: Path) -> None:
    project = project_copy(tmp_path, offline=False)
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(project),
            "--manifest",
            str(DBT_MANIFEST),
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert len(envelope["data"]["artifacts"]) == 16
    assert all(len(artifact["fingerprint"]) == 64 for artifact in envelope["data"]["artifacts"])


def test_compile_json_honors_selection(tmp_path: Path) -> None:
    project = project_copy(tmp_path, offline=False)
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(project),
            "--manifest",
            str(DBT_MANIFEST),
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
        "cowork_profile",
        "jaffle_analytics_agent.json",
        "jaffle_delivery_agent.json",
        "jaffle_minimal_agent.json",
    ]
    # The CoWork profile's specification, in its type's folder as --emit-ddl names one.
    assert [path.name for path in (specs / "cowork_profile").iterdir()] == ["jaffle_concierge.json"]
    assert f"wrote 4 files to {specs}" in result.output
    database = CliRunner().invoke(cli, ["compile", *common(project), "--database", "ELSEWHERE", "--output", "json"])
    targets = {item["artifact_key"]: item["target"] for item in json.loads(database.output)["data"]["artifacts"]}
    assert targets["semantic_view:jaffle_minimal"].startswith("ELSEWHERE.")


def test_a_cowork_profile_s_specification_is_emitted_in_its_type_s_folder_beside_the_agents() -> None:
    # CoWork profiles are a Snowflake private preview; one named for an agent never takes its file.
    result = compile_profiles(cowork_profile("sales"), agents=(described_agent(),))
    files, clashes = _payload_files(result, agents_only=True)
    assert (sorted(files), clashes) == (["cowork_profile/sales.json", "sales.json"], ())
    assert json.loads(files["cowork_profile/sales.json"])["tools"][0]["tool_spec"]["type"] == "agent_router"
    ddl, _ = _payload_files(result, agents_only=False)
    assert sorted(ddl) == ["agent/sales.json", "cowork_profile/sales.json"]


def test_compile_selection_keeps_the_canonical_manifest_full(tmp_path: Path) -> None:
    project = project_copy(tmp_path, offline=False)
    result = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(project),
            "--manifest",
            str(DBT_MANIFEST),
            "--select",
            "jaffle_minimal",
        ],
    )
    assert result.exit_code == 0, result.output
    canonical = json.loads((project / "sst_target" / "manifest.json").read_text(encoding="utf-8"))
    assert len(canonical["artifacts"]) == 16


@pytest.mark.parametrize(
    ("file", "before", "after", "code"),
    [
        ("semantic_models/filters/filters.yml", "var('completed_state')", "var('no_such_var')", "SST-CFG029"),
        (
            "semantic_models/semantic_views/semantic_views.yml",
            "custom_instructions('jaffle_sql_conventions')",
            "custom_instructions('no_such_instruction')",
            "SST-REF007",
        ),
        ("semantic_models/semantic_views/semantic_views.yml", "tag('cost_center')", "tag('no_such_tag')", "SST-REF028"),
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
    assert not (project / "sst_target" / "manifest.json").exists()


def test_a_partial_run_refuses_to_publish_a_view_without_a_broken_member(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    metrics = project / "semantic_models" / "metrics" / "total_revenue.yml"
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
    assert {"SST-CFG029", "SST-PLN033"} <= {item["code"] for item in diagnostics}
    refusal = next(item for item in diagnostics if item["code"] == "SST-PLN033")
    assert "metric:total_revenue" in refusal["message"]
    assert not (project / "sst_target" / "manifest.json").exists()


def test_project_without_dbt_compiles_from_target_profile(tmp_path: Path) -> None:
    project = skills_only_project(tmp_path / "skills")
    result = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["data"]["artifacts"] == []
    manifest = json.loads((project / "sst_target" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["sources"]["dbt_manifest"]["path"] == ""
    assert manifest["sources"]["dbt_manifest"]["model_count"] == 0


def test_project_without_dbt_refuses_dbt_only_configuration(tmp_path: Path) -> None:
    project = skills_only_project(tmp_path / "skills", "project:\n  target_profile: skills\nsemantic_views: {}\n")
    result = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert result.exit_code == 1
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == ["SST-CFG046", "SST-MAN008"]


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
    assert [item["code"] for item in diagnostics] == ["SST-CFG047", "SST-MAN008"]
    assert diagnostics[0]["message"] == "project.profiles_dir is profilez, which is not a directory in the project"

    # A dbt-only directory in a project without dbt is reported once, as CFG046.
    dbt_only = skills_only_project(tmp_path / "other", "project:\n  target_profile: skills\n  agents_dir: nowhere\n")
    result = CliRunner().invoke(cli, ["compile", "--project-dir", str(dbt_only), "--output", "json"])
    assert [item["code"] for item in json.loads(result.output)["diagnostics"]] == ["SST-CFG046", "SST-MAN008"]


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


def test_a_carriage_return_in_a_description_or_a_body_is_emitted_byte_for_byte(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    view = project / "semantic_models" / "semantic_views" / "jaffle_minimal.yml"
    text = view.read_text(encoding="utf-8")
    written = "description: |-\n    Menu products only. Use it when a question needs nothing but the product catalogue."
    assert written in text
    view.write_text(text.replace(written, 'description: "Use it when \\r\\nmenus matter."'), encoding="utf-8")
    tools = project / "tools" / "platform"
    (tools / "zz_crlf.yml").write_text(
        "tool:\n  name: zz_crlf\n  type: procedure\n  description: Returns ok.\n  language: sql\n"
        "  returns: VARCHAR\n  body: \"{{ file('zz_crlf.sql') }}\"\n",
        encoding="utf-8",
    )
    (tools / "zz_crlf.sql").write_bytes(b"BEGIN\r\n  RETURN 'ok';  \r\nEND;\r\n")
    result = CliRunner().invoke(cli, ["compile", *common(project), "--emit-ddl", str(tmp_path / "ddl")])
    assert result.exit_code == 0, result.output
    assert (
        b"COMMENT = 'Use it when \r\nmenus matter.'"
        in (tmp_path / "ddl" / "semantic_view" / "jaffle_minimal.sql").read_bytes()
    )
    assert b"$$\nBEGIN\r\n  RETURN 'ok';  \r\nEND;\n$$" in (tmp_path / "ddl" / "tool" / "zz_crlf.sql").read_bytes()


def test_emit_ddl_writes_a_view_and_a_tool_sharing_a_name_to_two_files(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    tools = project / "tools" / "platform"
    (tools / "jaffle_minimal.yml").write_text(
        "tool:\n  name: jaffle_minimal\n  type: procedure\n  description: Returns ok.\n  language: sql\n"
        "  returns: VARCHAR\n  body: \"{{ file('jaffle_minimal.sql') }}\"\n",
        encoding="utf-8",
    )
    (tools / "jaffle_minimal.sql").write_text("BEGIN\n  RETURN 'ok';\nEND;\n", encoding="utf-8")
    ddl = tmp_path / "ddl"
    result = CliRunner().invoke(cli, ["compile", *common(project), "--emit-ddl", str(ddl), "--output", "json"])
    assert result.exit_code == 0, result.output
    files = json.loads(result.output)["data"]["ddl_files"]
    assert len(files) == len(set(files)) == len(list(ddl.rglob("*.*")))
    assert {"semantic_view/jaffle_minimal.sql", "tool/jaffle_minimal.sql"} <= set(files)
    assert (
        (ddl / "semantic_view" / "jaffle_minimal.sql")
        .read_text(encoding="utf-8")
        .startswith("CREATE OR REPLACE SEMANTIC VIEW")
    )
    assert "PROCEDURE" in (ddl / "tool" / "jaffle_minimal.sql").read_text(encoding="utf-8").split("(")[0]
