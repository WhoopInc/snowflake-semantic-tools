"""`sst apply`: confirmation, saved plans, partial publishing, its connection, and dbt-free projects."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, OwnershipMarker, QueryResult
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state import content_hash
from tests.helpers.cli_projects import (
    OFFLINE_VALIDATION,
    PROFILE_CONFIG,
    break_menu_view,
    common,
    compile_project,
    invoke_counting_closes,
    invoke_with_port,
    profile_with_commands_and_plugin,
    project_copy,
    skill_project,
)
from tests.helpers.recorded_snowflake import RecordedSnowflake
from tests.helpers.sql_values import texts


def configure_eval_apply(port: RecordedSnowflake, changes: list[dict[str, object]]) -> None:
    eval_change = next(item for item in changes if item["artifact_type"] == "eval")
    resources = eval_change["physical_resources"]
    assert isinstance(resources, list)
    source_table = next(
        str(resource["qualified_name"])
        for resource in resources
        if isinstance(resource, dict) and resource["object_type"] == "TABLE"
    )
    dataset = next(
        str(resource["qualified_name"])
        for resource in resources
        if isinstance(resource, dict) and resource["object_type"] == "DATASET"
    )
    existing_eval_resources: set[str] = set()
    original_object_exists = port.object_exists

    def object_exists(object_type: str, qualified_name: QualifiedName) -> bool:
        if object_type in {"TABLE", "DATASET", "STAGE"}:
            return qualified_name.sql in existing_eval_resources
        return original_object_exists(object_type, qualified_name)

    port.object_exists = object_exists  # type: ignore[method-assign]
    original_execute = port.execute_script

    def execute_with_eval_resources(statements: Sequence[Sql]) -> ExecResult:
        result = original_execute(statements)
        for statement in texts(statements):
            normalized = " ".join(statement.split())
            if normalized.startswith("CREATE TABLE "):
                existing_eval_resources.add(source_table)
            elif "SYSTEM$CREATE_EVALUATION_DATASET" in normalized:
                existing_eval_resources.add(dataset)
            elif normalized.startswith("CREATE STAGE IF NOT EXISTS SST_REF_DEV.JAFFLE.EVAL_CONFIGS "):
                existing_eval_resources.add("SST_REF_DEV.JAFFLE.EVAL_CONFIGS")
        return result

    port.execute_script = execute_with_eval_resources  # type: ignore[method-assign]
    original_query = port.query

    def query_with_eval_row_count(
        sql: Sql, params: Sequence[object] | Mapping[str, object] | None = None
    ) -> QueryResult:
        if str(sql) == f"SELECT COUNT(*) AS ROW_COUNT FROM {source_table}":
            port.queries.append((str(sql), params))
            return QueryResult(("ROW_COUNT",), ((4,),))
        return original_query(sql, params)

    port.query = query_with_eval_row_count  # type: ignore[method-assign]


def test_apply_requires_confirmation_and_accepts_current_saved_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    plan_port = RecordedSnowflake(state={})
    planned = invoke_with_port(
        monkeypatch,
        plan_port,
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    planned_payload = json.loads(planned.output)
    plan_path = planned_payload["data"]["plan_path"]
    saved_plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    assert len(saved_plan["changes"]) == 14
    assert {item["artifact_type"] for item in saved_plan["changes"]} == {
        "semantic_view",
        "tool",
        "skill",
        "plugin",
        "profile",
        "agent",
        "eval",
    }
    assert {
        resource["object_type"]
        for item in saved_plan["changes"]
        if item["artifact_type"] == "eval"
        for resource in item["physical_resources"]
    } == {"TABLE", "DATASET"}
    usage = CliRunner().invoke(
        cli,
        ["apply", *common(project), "--target", "dev", "--plan", plan_path, "--output", "json"],
    )
    assert usage.exit_code == 3

    apply_port = plan_port
    original_execute = apply_port.execute_script

    def execute_with_markers(statements: Sequence[Sql]) -> ExecResult:
        result = original_execute(statements)
        for planned_change in planned_payload["data"]["changes"]:
            if planned_change["artifact_type"] in {"tool", "agent"}:
                apply_port.markers[planned_change["target"]] = OwnershipMarker(
                    planned_payload["data"]["manifest_id"],
                    planned_change["fingerprint"],
                )
        return result

    apply_port.execute_script = execute_with_markers  # type: ignore[method-assign]
    configure_eval_apply(apply_port, planned_payload["data"]["changes"])
    applied = invoke_with_port(
        monkeypatch,
        apply_port,
        ["apply", *common(project), "--target", "dev", "--plan", plan_path, "--yes", "--output", "json"],
    )
    assert applied.exit_code == 0, applied.output
    payload = json.loads(applied.output)
    assert payload["data"]["state_written"] is True
    # Ten more than M4: the bundle stage, CREATE and ADD VERSION for each of three
    # skills and the plugin, and the profile stage; and the eval dataset's own version.
    assert len(apply_port.scripts) == 22
    first_lines = [statements[0].split("\n", 1)[0] for statements in apply_port.scripts]
    assert sum("JAFFLE_TOOLKIT" in line for line in first_lines) == 2
    assert sum("SKILL_BUNDLES " in line and line.startswith("CREATE STAGE") for line in first_lines) == 1
    assert sum("[sst:" in statement for statements in apply_port.scripts for statement in statements) == 7


def test_partial_runs_publish_what_is_healthy_and_still_exit_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    break_menu_view(project)
    manifest_file = project / "target" / "sst" / "manifest.json"
    refused = CliRunner().invoke(cli, ["compile", *common(project), "--output", "json"])
    assert refused.exit_code == 1 and not manifest_file.exists()

    compiled = CliRunner().invoke(cli, ["compile", *common(project), "--partial", "--output", "json"])
    assert compiled.exit_code == 1, compiled.output
    payload = json.loads(compiled.output)
    excluded = set(payload["data"]["partial"]["excluded"])
    # The analytics agent uses the broken view, and its eval uses the agent.
    assert {"semantic_view:jaffle_menu", "agent:jaffle_analytics_agent", "eval:jaffle_analytics_agent"} <= excluded
    kept = {item["artifact_key"] for item in payload["data"]["artifacts"]}
    assert "semantic_view:jaffle_sales" in kept and not kept & excluded and manifest_file.is_file()
    assert sum(item["code"] == "SST-PLN032" for item in payload["diagnostics"]) == len(excluded)

    port = RecordedSnowflake(state={})
    whole = invoke_with_port(monkeypatch, port, ["plan", *common(project), "--target", "dev", "--output", "json"])
    assert whole.exit_code == 1
    planned = invoke_with_port(
        monkeypatch, port, ["plan", *common(project), "--target", "dev", "--partial", "--output", "json"]
    )
    assert planned.exit_code == 1, planned.output
    plan_payload = json.loads(planned.output)
    assert plan_payload["data"]["manifest_id"] == payload["data"]["manifest_id"]
    assert {item["artifact_key"] for item in plan_payload["data"]["changes"]} == kept
    assert set(plan_payload["data"]["partial"]["excluded"]) == excluded
    plan_path = plan_payload["data"]["plan_path"]
    assert json.loads(Path(plan_path).read_text(encoding="utf-8"))["selection"]["partial"] is True

    # Publishing a partial result is explicit, and --partial never prunes.
    apply_args = ["apply", *common(project), "--target", "dev", "--plan", plan_path, "--yes", "--output", "json"]
    assert CliRunner().invoke(cli, apply_args).exit_code == 3
    assert CliRunner().invoke(cli, ["plan", *common(project), "--partial", "--prune"]).exit_code == 3
    assert CliRunner().invoke(cli, ["apply", *common(project), "--partial", "--prune", "--yes"]).exit_code == 3

    original_execute = port.execute_script

    def execute_with_markers(statements: Sequence[Sql]) -> ExecResult:
        executed = original_execute(statements)
        for change in plan_payload["data"]["changes"]:
            if change["artifact_type"] in {"tool", "agent"}:
                port.markers[change["target"]] = OwnershipMarker(
                    plan_payload["data"]["manifest_id"], change["fingerprint"]
                )
        return executed

    port.execute_script = execute_with_markers  # type: ignore[method-assign]
    applied = invoke_with_port(monkeypatch, port, [*apply_args, "--partial"])
    assert applied.exit_code == 1, applied.output
    applied_payload = json.loads(applied.output)
    outcomes = {item["artifact_key"]: item["status"] for item in applied_payload["data"]["outcomes"]}
    assert set(outcomes) == kept and set(outcomes.values()) == {"applied"}
    assert applied_payload["data"]["state_written"] is True
    # Nothing addressed the excluded view or the agent that uses it.
    targets = {statement.split("\n", 1)[0] for script in port.scripts for statement in script}
    assert not any(".JAFFLE_MENU" in line or "JAFFLE_ANALYTICS_AGENT" in line for line in targets)


def test_apply_reuses_saved_plan_selection_without_repeated_selectors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    planned = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(state={}),
        [
            "plan",
            *common(project),
            "--target",
            "dev",
            "--select",
            "jaffle_minimal",
            "--output",
            "json",
        ],
    )
    assert planned.exit_code == 2, planned.output
    plan_path = json.loads(planned.output)["data"]["plan_path"]
    saved = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    assert saved["selection"]["selected"] == ["jaffle_minimal"]

    port = RecordedSnowflake(state={})
    applied = invoke_with_port(
        monkeypatch,
        port,
        [
            "apply",
            *common(project),
            "--target",
            "dev",
            "--plan",
            plan_path,
            "--yes",
            "--output",
            "json",
        ],
    )

    assert applied.exit_code == 0, applied.output
    assert len(port.scripts) == 1


def test_apply_refuses_stale_saved_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    port = RecordedSnowflake(state={})
    planned = invoke_with_port(
        monkeypatch,
        port,
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    path = Path(json.loads(planned.output)["data"]["plan_path"])
    document = json.loads(path.read_text(encoding="utf-8"))
    document["manifest_id"] = "b" * 64
    path.write_text(json.dumps(document), encoding="utf-8")
    refused = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(state={}),
        ["apply", *common(project), "--target", "dev", "--plan", str(path), "--yes", "--output", "json"],
    )
    assert refused.exit_code == 4


def test_apply_refuses_a_saved_plan_made_for_another_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    planned = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(state={}),
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    path = Path(json.loads(planned.output)["data"]["plan_path"])
    document = json.loads(path.read_text(encoding="utf-8"))
    document["target"]["role"] = "SOME_OTHER_ROLE"
    document["plan_id"] = content_hash({key: value for key, value in document.items() if key != "plan_id"})
    path.write_text(json.dumps(document), encoding="utf-8")
    refused = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(state={}),
        ["apply", *common(project), "--target", "dev", "--plan", str(path), "--yes", "--output", "json"],
    )
    assert refused.exit_code == 4
    [diagnostic] = json.loads(refused.output)["diagnostics"]
    assert diagnostic["code"] == "SST-APL005" and "SOME_OTHER_ROLE" in diagnostic["message"]


def test_apply_closes_its_connection_when_it_stops_before_applying(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    declined, closes = invoke_counting_closes(
        monkeypatch, RecordedSnowflake(state={}), ["apply", *common(project), "--target", "dev"]
    )
    assert declined.exit_code == 130 and "Apply this plan?" in declined.output
    assert closes == ["closed"]

    planned = invoke_with_port(
        monkeypatch, RecordedSnowflake(state={}), ["plan", *common(project), "--target", "dev", "--output", "json"]
    )
    path = Path(json.loads(planned.output)["data"]["plan_path"])
    document = json.loads(path.read_text(encoding="utf-8"))
    document["observation_fingerprint"] = "0" * 64
    document["plan_id"] = content_hash({key: value for key, value in document.items() if key != "plan_id"})
    path.write_text(json.dumps(document), encoding="utf-8")
    stale, closes = invoke_counting_closes(
        monkeypatch,
        RecordedSnowflake(state={}),
        ["apply", *common(project), "--target", "dev", "--plan", str(path), "--yes", "--output", "json"],
    )
    assert stale.exit_code == 4 and "saved plan is stale" in json.loads(stale.output)["data"]["error"]
    assert closes == ["closed"]


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


def test_profiles_publish_then_deactivate_under_prune(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = skill_project(tmp_path / "skills")
    (project / "sst_config.yml").write_text(OFFLINE_VALIDATION + PROFILE_CONFIG, encoding="utf-8")
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
