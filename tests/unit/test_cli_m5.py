"""M5 command-line behavior: dbt-optional projects, publishing, docs, and migration."""

from __future__ import annotations

import json
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from tests.helpers.recorded_snowflake import RecordedSnowflake
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.model.identifier import QualifiedName

PROFILES = """
skills:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: acct
      user: me
      database: DB
      schema: SCH
"""


def skills_only_project(root: Path, config: str = "project:\n  target_profile: skills\n") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "profiles.yml").write_text(PROFILES, encoding="utf-8")
    (root / "sst_config.yml").write_text(config, encoding="utf-8")
    return root


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
    project = skills_only_project(tmp_path / "skills", "validation:\n  strict: false\n")
    result = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert result.exit_code == 4
    assert "project.target_profile" in json.loads(result.output)["data"]["error"]


SKILLS_CONFIG = """
project:
  target_profile: skills
skills:
  +version_prefix: "SST_"
  catalog:
    +bundle_stage: SKILL_BUNDLES
"""


def skill_project(root: Path) -> Path:
    project = skills_only_project(root, SKILLS_CONFIG)
    skill = project / "skills" / "month-close"
    (skill / "reference").mkdir(parents=True)
    (skill / "SKILL.md").write_text(
        "---\nname: month-close\ndescription: Close the month.\n---\nRead reference/steps.md.\n",
        encoding="utf-8",
    )
    (skill / "reference" / "steps.md").write_text("steps\n", encoding="utf-8")
    return project


def invoke_with_port(monkeypatch: pytest.MonkeyPatch, port: RecordedSnowflake, args: list[str]):
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
    port.close = lambda: None  # type: ignore[attr-defined]
    return CliRunner().invoke(cli, args)


def test_skills_only_project_publishes_through_plan_and_apply(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = skill_project(tmp_path / "skills")
    compiled = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert compiled.exit_code == 0, compiled.output
    artifacts = json.loads(compiled.output)["data"]["artifacts"]
    assert [item["artifact_key"] for item in artifacts] == ["skill:month-close"]
    assert artifacts[0]["target"] == "DB.SCH.MONTH_CLOSE"

    port = RecordedSnowflake(existing=())
    planned = invoke_with_port(monkeypatch, port, ["plan", "--project-dir", str(project), "--output", "json"])
    assert planned.exit_code == 2, planned.output
    change = json.loads(planned.output)["data"]["changes"][0]
    assert (change["action"], change["artifact_type"]) == ("create", "skill")
    assert change["component_fingerprints"]["alias"].startswith("SST_")
    assert (project / "target" / "sst" / "sql" / "skill__month-close.json").is_file()

    applied = invoke_with_port(monkeypatch, port, ["apply", "--project-dir", str(project), "--yes", "--output", "json"])
    assert applied.exit_code == 0, applied.output
    assert json.loads(applied.output)["data"]["outcomes"][0]["status"] == "applied"
    assert "DB.SCH.MONTH_CLOSE" in port.extensions

    replanned = invoke_with_port(monkeypatch, port, ["plan", "--project-dir", str(project), "--output", "json"])
    assert replanned.exit_code == 0, replanned.output
    assert [item["action"] for item in json.loads(replanned.output)["data"]["changes"]] == ["noop"]

    golden = tmp_path / "golden" / "ddl"
    golden.mkdir(parents=True)
    missing = CliRunner().invoke(
        cli,
        ["test", "--project-dir", str(project), "--suite", "golden", "--golden-dir", str(golden), "--output", "json"],
    )
    assert missing.exit_code == 1
    assert json.loads(missing.output)["data"]["failures"] == [
        f"missing golden {tmp_path / 'golden' / 'skill' / 'month-close.bundle.json'}"
    ]


def test_report_only_prunes_are_listed_but_are_not_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import shutil

    project = skill_project(tmp_path / "skills")
    port = RecordedSnowflake(existing=())
    assert CliRunner().invoke(cli, ["compile", "--project-dir", str(project)]).exit_code == 0
    applied = invoke_with_port(monkeypatch, port, ["apply", "--project-dir", str(project), "--yes", "--output", "json"])
    assert applied.exit_code == 0, applied.output
    shutil.rmtree(project / "skills" / "month-close")
    assert CliRunner().invoke(cli, ["compile", "--project-dir", str(project)]).exit_code == 0
    scripts = len(port.scripts)

    first = invoke_with_port(monkeypatch, port, ["plan", "--project-dir", str(project), "--prune", "--output", "json"])
    assert first.exit_code == 0, first.output
    assert json.loads(first.output)["data"]["report_only"] == ["skill:month-close"]
    human = invoke_with_port(monkeypatch, port, ["plan", "--project-dir", str(project), "--prune"])
    assert human.exit_code == 0, human.output
    assert "0 to prune" in human.output and "1 report-only (SST never removes these)." in human.output
    assert "- skill:month-close PRUNE - orphaned (report only)" in human.output

    # Apply executes nothing, but records the run so the state follows the manifest.
    pruned = invoke_with_port(
        monkeypatch, port, ["apply", "--project-dir", str(project), "--prune", "--yes", "--output", "json"]
    )
    assert pruned.exit_code == 0, pruned.output
    assert json.loads(pruned.output)["data"]["state_written"] is True
    executed = [statement for script in port.scripts[scripts:] for statement in script]
    assert not any("CORTEX EXTENSION" in statement for statement in executed)
    assert "DB.SCH.MONTH_CLOSE" in port.extensions

    for extra in ([], ["--strict"]):
        planned = invoke_with_port(
            monkeypatch, port, ["plan", "--project-dir", str(project), "--prune", "--output", "json", *extra]
        )
        assert planned.exit_code == 0, planned.output
        payload = json.loads(planned.output)
        assert payload["status"] == "ok" and payload["data"]["report_only"] == ["skill:month-close"]
        assert [(item["action"], item["report_only"]) for item in payload["data"]["changes"]] == [("prune", True)]
        assert [(item["code"], item["severity"]) for item in payload["diagnostics"]] == [("SST-PLN021", "info")]


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


PROFILE_CONFIG = """
project:
  target_profile: skills
skills:
  stage:
    +stage: PROFILES
"""


def test_profiles_publish_then_deactivate_under_prune(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = skill_project(tmp_path / "skills")
    (project / "sst_config.yml").write_text(PROFILE_CONFIG, encoding="utf-8")
    profile = project / "profiles" / "analyst"
    profile.mkdir(parents=True)
    (profile / "profile.yml").write_text(
        "name: analyst\ndescription: Analyst.\nowner_team: Data\nskills: [month-close]\n", encoding="utf-8"
    )
    compiled = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert compiled.exit_code == 0, compiled.output
    assert [item["artifact_key"] for item in json.loads(compiled.output)["data"]["artifacts"]] == ["profile:analyst"]
    assert json.loads(compiled.output)["data"]["artifacts"][0]["target"] == "DB.SCH.PROFILE_REGISTRY"

    port = RecordedSnowflake(existing=())
    applied = invoke_with_port(monkeypatch, port, ["apply", "--project-dir", str(project), "--yes", "--output", "json"])
    assert applied.exit_code == 0, applied.output
    row = port.desktop_profile_rows(QualifiedName.parse("DB.SCH.PROFILE_REGISTRY"))[0]
    assert row["CONFIG_NAME"] == "analyst" and str(row["VERSION"]).startswith("SST_")

    replanned = invoke_with_port(monkeypatch, port, ["plan", "--project-dir", str(project), "--output", "json"])
    assert [item["action"] for item in json.loads(replanned.output)["data"]["changes"]] == ["noop"]

    import shutil

    shutil.rmtree(profile)
    compiled = CliRunner().invoke(cli, ["compile", "--project-dir", str(project)])
    assert compiled.exit_code == 0, compiled.output
    pruned = invoke_with_port(
        monkeypatch, port, ["apply", "--project-dir", str(project), "--prune", "--yes", "--output", "json"]
    )
    assert pruned.exit_code == 0, pruned.output
    assert port.desktop_profile_rows(QualifiedName.parse("DB.SCH.PROFILE_REGISTRY")) == ()


def profile_with_commands_and_plugin(root: Path) -> Path:
    project = skill_project(root)
    (project / "sst_config.yml").write_text(PROFILE_CONFIG, encoding="utf-8")
    files = {
        "profiles/analyst/profile.yml": (
            "name: analyst\ndescription: Analyst.\nowner_team: Data\nskills: [month-close]\n"
            "commands: [sql/check]\nplugins: [kit]\n"
        ),
        "profiles/shared/profile.yml": "commands: [daily]\n",
        "commands/daily.md": "Summarise yesterday.\n",
        "commands/sql/check.md": "---\ndescription: Check SQL.\n---\nCheck it.\n",
        "plugins/kit/plugin.yml": "name: kit\ndescription: Kit.\nowner_team: Data\nskills: [month-close]\n",
    }
    for name, text in files.items():
        (project / name).parent.mkdir(parents=True, exist_ok=True)
        (project / name).write_text(text, encoding="utf-8")
    return project


def test_profile_commands_and_plugins_publish_and_every_pointer_resolves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = profile_with_commands_and_plugin(tmp_path / "skills")
    compiled = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert compiled.exit_code == 0, compiled.output
    port = RecordedSnowflake(existing=())
    applied = invoke_with_port(monkeypatch, port, ["apply", "--project-dir", str(project), "--yes", "--output", "json"])
    assert applied.exit_code == 0, applied.output
    row = port.desktop_profile_rows(QualifiedName.parse("DB.SCH.PROFILE_REGISTRY"))[0]
    commands = json.loads(str(row["COMMAND_REPOS"]))
    plugins = json.loads(str(row["PLUGINS"]))
    assert [pointer["snowflake_stage"].split("/")[1:3] for pointer in commands] == [
        ["commands", "shared"],
        ["commands", "analyst"],
    ]
    assert port.list_location(commands[0]["snowflake_stage"]) == ("daily.md",)
    assert port.list_location(commands[1]["snowflake_stage"]) == ("sql/check.md",)
    assert (
        len(plugins) == 1
        and plugins[0].startswith("@DB.SCH.PROFILES/plugins/analyst/")
        and plugins[0].endswith("/kit/")
    )
    assert ".cortex-plugin/plugin.json" in port.list_location(plugins[0])

    replanned = invoke_with_port(monkeypatch, port, ["plan", "--project-dir", str(project), "--output", "json"])
    assert replanned.exit_code == 0, replanned.output
    assert [item["action"] for item in json.loads(replanned.output)["data"]["changes"]] == ["noop"]


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


def test_migrate_refs_reports_calls_it_will_not_rewrite(tmp_path: Path) -> None:
    project = tmp_path / "legacy"
    views = project / "semantic_models" / "semantic_views"
    views.mkdir(parents=True)
    (project / "sst_config.yml").write_text("project:\n  semantic_models_dir: semantic_models\n", encoding="utf-8")
    (views / "views.yml").write_text(
        "semantic_views:\n  - name: v\n    tables:\n      - {{ table('orders') }}\n"
        "    description: \"{{ table('orders') }} is quoted prose\"\n",
        encoding="utf-8",
    )
    human = CliRunner().invoke(cli, ["migrate", "refs", "--project-dir", str(project)])
    assert human.exit_code == 1
    assert "would rewrite semantic_models/semantic_views/views.yml: 1 ref" in human.output
    assert "views.yml:5:" in human.output and "unchanged" in human.output
    machine = CliRunner().invoke(cli, ["migrate", "refs", "--project-dir", str(project), "--write", "--output", "json"])
    assert machine.exit_code == 1
    payload = json.loads(machine.output)
    assert payload["status"] == "error" and payload["data"]["written"] is True
    assert payload["data"]["files"][0]["untouched"][0]["line"] == 5
    assert "{{ ref('orders') }}" in (views / "views.yml").read_text(encoding="utf-8")

    clean = tmp_path / "clean"
    (clean / "semantic_models").mkdir(parents=True)
    (clean / "sst_config.yml").write_text("project:\n  semantic_models_dir: semantic_models\n", encoding="utf-8")
    none = CliRunner().invoke(cli, ["migrate", "refs", "--project-dir", str(clean)])
    assert none.exit_code == 0 and "no legacy references found" in none.output
    missing = CliRunner().invoke(cli, ["migrate", "refs", "--project-dir", str(tmp_path)])
    assert missing.exit_code == 4


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_docs_writes_checks_and_reports_drift(tmp_path: Path) -> None:
    written = CliRunner().invoke(cli, ["docs", "--project-dir", str(tmp_path)])
    assert written.exit_code == 0, written.output
    pages = sorted(path.name for path in (tmp_path / "docs" / "reference").iterdir())
    assert pages == ["artifacts.md", "cli.md", "config.md", "error-codes.md"]
    assert "wrote docs/reference/cli.md" in written.output
    assert CliRunner().invoke(cli, ["docs", "--project-dir", str(tmp_path), "--check"]).exit_code == 0

    config = tmp_path / "docs" / "reference" / "config.md"
    config.write_text(config.read_text(encoding="utf-8") + "hand edit\n", encoding="utf-8")
    drifted = CliRunner().invoke(cli, ["docs", "--project-dir", str(tmp_path), "--check"])
    assert drifted.exit_code == 1
    assert "out of date: docs/reference/config.md" in drifted.output
    machine = CliRunner().invoke(cli, ["docs", "--project-dir", str(tmp_path), "--check", "--output", "json"])
    assert machine.exit_code == 1
    assert json.loads(machine.output)["data"]["drifted"] == ["docs/reference/config.md"]
    assert "hand edit" in config.read_text(encoding="utf-8")

    repaired = CliRunner().invoke(cli, ["docs", "--project-dir", str(tmp_path), "--output", "json"])
    assert repaired.exit_code == 0
    assert json.loads(repaired.output)["data"]["written"] == ["docs/reference/config.md"]
    assert "hand edit" not in config.read_text(encoding="utf-8")


def test_committed_reference_pages_are_current() -> None:
    result = CliRunner().invoke(cli, ["docs", "--project-dir", str(REPO_ROOT), "--check"])
    assert result.exit_code == 0, result.output


def test_every_command_and_option_is_documented() -> None:
    def walk(command: click.Command, path: str) -> list[str]:
        missing = [path] if not command.help else []
        missing += [
            f"{path} {param.opts[0]}"
            for param in command.params
            if isinstance(param, click.Option) and not param.hidden and not param.help
        ]
        for name, child in getattr(command, "commands", {}).items():
            missing += walk(child, f"{path} {name}")
        return missing

    assert walk(cli, "sst") == []
