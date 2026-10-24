"""`sst apply`: confirmation, saved plans, partial publishing, its connection, and dbt-free projects."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from pathlib import Path

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, OwnershipMarker
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state import content_hash
from tests.helpers.apply_sessions import RecordingSnowflake
from tests.helpers.cli_projects import (
    OFFLINE_VALIDATION,
    PROFILE_CONFIG,
    common,
    compile_project,
    invoke_counting_closes,
    invoke_with_port,
    on_a_terminal,
    profile_with_commands_and_plugin,
    skill_project,
)
from tests.helpers.reference_project import break_menu_view, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import texts


def configure_eval_apply(port: FakeSnowflake, changes: list[dict[str, object]]) -> None:
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
    port.table_row_counts[source_table] = 4


def test_apply_requires_confirmation_and_accepts_current_saved_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    plan_port = FakeSnowflake(state={})
    planned = invoke_with_port(
        monkeypatch,
        plan_port,
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    planned_payload = json.loads(planned.output)
    plan_path = planned_payload["data"]["plan_path"]
    saved_plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    assert len(saved_plan["changes"]) == 17
    assert {item["artifact_type"] for item in saved_plan["changes"]} == {
        "semantic_view",
        "tool",
        "skill",
        "plugin",
        "coco_desktop_profile",
        "agent",
        "cowork_profile",
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
            # A CoWork profile (a Snowflake private preview) is an AGENT, marked as an agent is.
            if planned_change["artifact_type"] in {"tool", "agent", "cowork_profile"}:
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
    # Each command's envelope says how long the steps it named took.
    plan_phases = planned_payload["invocation"]["timings"]["phases"]
    assert {"load", "validate", "state_read", "observe", "classify"} <= set(plan_phases)
    assert {"lock", "publish", "state_write"} <= set(payload["invocation"]["timings"]["phases"])
    # Eight more than M4: the bundle stage, one CREATE carrying the first version for each
    # of four skills (one nested in another) and the plugin, and the profile stage; the eval dataset's own version;
    # and the agent spec stage, created once for every agent and CoWork profile that stages
    # there. One more certifies the version of jaffle-catalogue, the skill whose own config:
    # sets certified, one more creates the order_status procedure, and one more the CoWork
    # profile, whose CREATE AGENT carries its marker as each agent's does.
    assert len(apply_port.scripts) == 23
    first_lines = [statements[0].split("\n", 1)[0] for statements in apply_port.scripts]
    assert sum("CERTIFICATION_STATUS" in line and "JAFFLE_CATALOGUE" in line for line in first_lines) == 1
    assert sum("CERTIFICATION_STATUS" in line for line in first_lines) == 1
    assert sum("JAFFLE_TOOLKIT" in line for line in first_lines) == 1
    assert not any(line.startswith("ALTER CORTEX EXTENSION") and " ADD VERSION " in line for line in first_lines)
    assert sum("SKILL_BUNDLES " in line and line.startswith("CREATE STAGE") for line in first_lines) == 1
    assert sum("AGENT_SPECS_SSE " in line and line.startswith("CREATE STAGE") for line in first_lines) == 1
    assert sum("[sst:" in statement for statements in apply_port.scripts for statement in statements) == 9


def test_partial_runs_publish_what_is_healthy_and_still_exit_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    break_menu_view(project)
    manifest_file = project / "sst_target" / "manifest.json"
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

    port = FakeSnowflake(state={})
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
        FakeSnowflake(state={}),
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

    port = FakeSnowflake(state={})
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


def test_a_saved_plan_and_its_apply_agree_on_overwrite(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    paths = {}
    for flags in ((), ("--overwrite",)):
        planned = invoke_with_port(
            monkeypatch,
            FakeSnowflake(state={}),
            [
                "plan",
                *common(project),
                "--target",
                "dev",
                *flags,
                "--plan-out",
                str(tmp_path / f"plan{len(flags)}.json"),
                "--output",
                "json",
            ],
        )
        assert planned.exit_code == 2, planned.output
        paths[bool(flags)] = json.loads(planned.output)["data"]["plan_path"]
    assert json.loads(Path(paths[True]).read_text(encoding="utf-8"))["selection"]["overwrite"] is True
    assert "overwrite" not in json.loads(Path(paths[False]).read_text(encoding="utf-8"))["selection"]
    # Overwriting a hand edit is always explicit, in both directions.
    for saved, flags, said in (
        (True, (), "apply it with --overwrite"),
        (False, ("--overwrite",), "--overwrite conflicts with a saved plan made without it"),
    ):
        refused = CliRunner().invoke(
            cli, ["apply", *common(project), "--target", "dev", "--plan", paths[saved], *flags, "--yes"]
        )
        assert refused.exit_code == 3 and said in refused.output, refused.output


def test_apply_refuses_stale_saved_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    port = FakeSnowflake(state={})
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
        FakeSnowflake(state={}),
        ["apply", *common(project), "--target", "dev", "--plan", str(path), "--yes", "--output", "json"],
    )
    assert refused.exit_code == 4


def test_apply_refuses_a_saved_plan_made_for_another_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    planned = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    path = Path(json.loads(planned.output)["data"]["plan_path"])
    document = json.loads(path.read_text(encoding="utf-8"))
    document["target"]["role"] = "SOME_OTHER_ROLE"
    document["plan_id"] = content_hash({key: value for key, value in document.items() if key != "plan_id"})
    path.write_text(json.dumps(document), encoding="utf-8")
    refused = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
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
    on_a_terminal(monkeypatch)
    # One session, so the one close recorded is the command's own; a pool's siblings are the same fake.
    declined, closes = invoke_counting_closes(
        monkeypatch, FakeSnowflake(state={}), ["apply", *common(project), "--target", "dev", "--threads", "1"]
    )
    assert declined.exit_code == 130 and "Apply this plan?" in declined.output
    assert closes == ["closed"]

    planned = invoke_with_port(
        monkeypatch, FakeSnowflake(state={}), ["plan", *common(project), "--target", "dev", "--output", "json"]
    )
    path = Path(json.loads(planned.output)["data"]["plan_path"])
    document = json.loads(path.read_text(encoding="utf-8"))
    document["observation_fingerprint"] = "0" * 64
    document["plan_id"] = content_hash({key: value for key, value in document.items() if key != "plan_id"})
    path.write_text(json.dumps(document), encoding="utf-8")
    stale, closes = invoke_counting_closes(
        monkeypatch,
        FakeSnowflake(state={}),
        [
            "apply",
            *common(project),
            "--target",
            "dev",
            "--plan",
            str(path),
            "--yes",
            "--output",
            "json",
            "--threads",
            "1",
        ],
    )
    envelope = json.loads(stale.output)
    assert stale.exit_code == 4 and "changed since the plan" in envelope["data"]["error"]
    assert [item["code"] for item in envelope["diagnostics"]] == ["SST-APL012"]
    assert closes == ["closed"]


def test_skills_only_project_publishes_through_plan_and_apply(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = skill_project(tmp_path / "skills")
    compiled = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert compiled.exit_code == 0, compiled.output
    artifacts = json.loads(compiled.output)["data"]["artifacts"]
    assert [item["artifact_key"] for item in artifacts] == ["skill:month-close"]
    assert artifacts[0]["target"] == "DB.SCH.MONTH_CLOSE"

    port = FakeSnowflake(existing=())
    planned = invoke_with_port(monkeypatch, port, ["plan", "--project-dir", str(project), "--output", "json"])
    assert planned.exit_code == 2, planned.output
    change = json.loads(planned.output)["data"]["changes"][0]
    assert (change["action"], change["artifact_type"]) == ("create", "skill")
    assert change["component_fingerprints"]["alias"].startswith("SST_")
    assert (project / "sst_target" / "sql" / "skill__month-close.json").is_file()

    applied = invoke_with_port(monkeypatch, port, ["apply", "--project-dir", str(project), "--yes", "--output", "json"])
    assert applied.exit_code == 0, applied.output
    applied_data = json.loads(applied.output)["data"]
    assert applied_data["outcomes"][0]["status"] == "applied"
    assert (applied_data["applied"], applied_data["failed"], applied_data["pruned"]) == (["skill:month-close"], [], [])
    assert Path(applied_data["sql_path"]) == project / "sst_target" / "sql"
    assert "DB.SCH.MONTH_CLOSE" in port.extensions

    replanned = invoke_with_port(monkeypatch, port, ["plan", "--project-dir", str(project), "--output", "json"])
    assert replanned.exit_code == 0, replanned.output
    assert [item["action"] for item in json.loads(replanned.output)["data"]["changes"]] == ["noop"]


def test_profiles_publish_then_deactivate_under_prune(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = skill_project(tmp_path / "skills")
    (project / "sst_config.yml").write_text(OFFLINE_VALIDATION + PROFILE_CONFIG, encoding="utf-8")
    profile = project / "coco_desktop_profiles" / "analyst"
    profile.mkdir(parents=True)
    (profile / "coco_desktop_profile.yml").write_text(
        "name: analyst\ndescription: Analyst.\nowner_team: Data\nskills: [\"{{ skill('month-close') }}\"]\n",
        encoding="utf-8",
    )
    compiled = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert compiled.exit_code == 0, compiled.output
    assert [item["artifact_key"] for item in json.loads(compiled.output)["data"]["artifacts"]] == [
        "coco_desktop_profile:analyst"
    ]
    assert json.loads(compiled.output)["data"]["artifacts"][0]["target"] == "CORTEX_CODE.CONFIG.PROFILE_REGISTRY"

    port = FakeSnowflake(existing=())
    applied = invoke_with_port(monkeypatch, port, ["apply", "--project-dir", str(project), "--yes", "--output", "json"])
    assert applied.exit_code == 0, applied.output
    row = port.desktop_profile_rows(QualifiedName.parse("CORTEX_CODE.CONFIG.PROFILE_REGISTRY"))[0]
    assert (row["CONFIG_NAME"], row["VERSION"]) == ("analyst", "1")

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
    assert json.loads(pruned.output)["data"]["pruned"] == ["coco_desktop_profile:analyst"]
    assert port.desktop_profile_rows(QualifiedName.parse("CORTEX_CODE.CONFIG.PROFILE_REGISTRY")) == ()


def test_profile_commands_and_plugins_publish_and_every_pointer_resolves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = profile_with_commands_and_plugin(tmp_path / "skills")
    compiled = CliRunner().invoke(cli, ["compile", "--project-dir", str(project), "--output", "json"])
    assert compiled.exit_code == 0, compiled.output
    port = FakeSnowflake(existing=())
    applied = invoke_with_port(monkeypatch, port, ["apply", "--project-dir", str(project), "--yes", "--output", "json"])
    assert applied.exit_code == 0, applied.output
    row = port.desktop_profile_rows(QualifiedName.parse("CORTEX_CODE.CONFIG.PROFILE_REGISTRY"))[0]
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


# What a run sends Snowflake to take the run lock, hold it, and write state under it.
_LOCKED_RUN = {"acquire_run_lock", "extend_run_lock", "verify_run_lock", "write_state", "release_run_lock"}


def _applied_skill(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, RecordingSnowflake]:
    """A skills-only project, applied once, so planning it again changes nothing."""
    project = skill_project(tmp_path / "skills")
    port = RecordingSnowflake(existing=())
    first = invoke_with_port(monkeypatch, port, ["apply", "--project-dir", str(project), "--yes", "--output", "json"])
    assert first.exit_code == 0, first.output
    return project, port


def _counting_connects(monkeypatch: pytest.MonkeyPatch, port: FakeSnowflake) -> list[object]:
    """Hand out `port` for every connection the command opens, recording each one."""
    opened: list[object] = []

    def connect(params: object) -> FakeSnowflake:
        opened.append(params)
        return port

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", connect)
    port.close = lambda: None  # type: ignore[attr-defined]
    return opened


def _apply_json(project: Path, *extra: str) -> tuple[int, dict[str, object]]:
    applied = CliRunner().invoke(
        cli, ["apply", "--project-dir", str(project), "--yes", "--output", "json", "--threads", "1", *extra]
    )
    return applied.exit_code, json.loads(applied.output)


def test_an_apply_with_nothing_to_change_takes_no_lock_and_opens_no_other_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _applied_skill(tmp_path, monkeypatch)
    opened = _counting_connects(monkeypatch, port)
    port.calls.clear()

    exit_code, envelope = _apply_json(project)

    assert exit_code == 0, envelope
    data = envelope["data"]
    assert isinstance(data, dict)
    assert [item["status"] for item in data["outcomes"]] == ["skipped"]
    assert (data["applied"], data["state_written"]) == ([], False)
    # The command's own session only: no worker, no heartbeat; no lock taken, no state written.
    assert len(opened) == 1
    assert not _LOCKED_RUN & set(port.calls)
    assert port.calls.count("read_state") == 1
    assert {"lock", "publish", "state_write"}.isdisjoint(envelope["invocation"]["timings"]["phases"])  # type: ignore[index]


def test_an_apply_with_nothing_to_change_reports_what_the_locked_run_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _applied_skill(tmp_path, monkeypatch)
    _counting_connects(monkeypatch, port)
    quick_code, quick = _apply_json(project)
    quick_human = CliRunner().invoke(cli, ["apply", "--project-dir", str(project), "--yes", "--threads", "1"])

    # The run as it was: the lock taken, state read again under it, every change through the waves.
    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.apply.writes_nothing", lambda changeset: False)
    monkeypatch.setattr("snowflake_semantic_tools.app.apply.run.writes_nothing", lambda changeset: False)
    port.calls.clear()
    locked_code, locked = _apply_json(project)
    assert "acquire_run_lock" in port.calls
    locked_human = CliRunner().invoke(cli, ["apply", "--project-dir", str(project), "--yes", "--threads", "1"])

    def kept(envelope: dict[str, object]) -> dict[str, object]:
        data = dict(envelope["data"])  # type: ignore[call-overload]
        del data["run_id"]
        return {**{key: value for key, value in envelope.items() if key != "invocation"}, "data": data}

    assert quick_code == locked_code == 0
    assert kept(quick) == kept(locked)
    assert quick_human.exit_code == locked_human.exit_code == 0
    assert "applied 0 changes; 1 unchanged" in quick_human.output
    # The headline ends with how long the run took, which is all that may differ.
    assert _untimed(quick_human.output) == _untimed(locked_human.output)


def _untimed(output: str) -> str:
    return re.sub(r" · [0-9.]+s$", "", output, flags=re.MULTILINE)


def test_an_apply_with_nothing_to_change_asks_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, port = _applied_skill(tmp_path, monkeypatch)
    _counting_connects(monkeypatch, port)
    on_a_terminal(monkeypatch)
    port.calls.clear()

    asked = CliRunner().invoke(cli, ["apply", "--project-dir", str(project), "--threads", "1"])

    assert asked.exit_code == 0, asked.output
    assert "Apply this plan?" not in asked.output
    assert not _LOCKED_RUN & set(port.calls)


def test_a_saved_plan_with_nothing_to_change_applies_without_the_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, port = _applied_skill(tmp_path, monkeypatch)
    opened = _counting_connects(monkeypatch, port)
    planned = CliRunner().invoke(cli, ["plan", "--project-dir", str(project), "--output", "json", "--threads", "1"])
    assert planned.exit_code == 0, planned.output
    plan_path = json.loads(planned.output)["data"]["plan_path"]
    opened.clear()
    port.calls.clear()

    exit_code, envelope = _apply_json(project, "--plan", plan_path)

    assert exit_code == 0, envelope
    assert [item["status"] for item in envelope["data"]["outcomes"]] == ["skipped"]  # type: ignore[index]
    assert len(opened) == 1 and not _LOCKED_RUN & set(port.calls)


def test_an_apply_with_one_change_still_takes_the_lock_and_records_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = skill_project(tmp_path / "skills")
    port = RecordingSnowflake(existing=())

    applied = invoke_with_port(monkeypatch, port, ["apply", "--project-dir", str(project), "--yes", "--output", "json"])

    assert applied.exit_code == 0, applied.output
    assert json.loads(applied.output)["data"]["state_written"] is True
    assert {"acquire_run_lock", "write_state", "release_run_lock"} <= set(port.calls)
