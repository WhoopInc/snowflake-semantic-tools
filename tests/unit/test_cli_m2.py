"""M2 command, JSON, exit, saved-plan, and write-safety contracts."""

from __future__ import annotations

import dataclasses
import json
import shutil
from hashlib import md5
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.adapters.fs.local import StateFileStore
from snowflake_semantic_tools.adapters.errors import ProjectError
from tests.helpers.eval_state_store import InMemoryEvalStateStore
from tests.helpers.recorded_snowflake import PROFILE_REGISTRY_SHAPE, RecordedSnowflake
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.lifecycle.evals import EVAL_STAGE_FILE_FORMAT
from snowflake_semantic_tools.app.evals.run import EvalRunResult, EvalSuiteResult
from snowflake_semantic_tools.cli.main import _build_manifest, _compile_result, _compiled_manifest, cli
from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import (
    EvalBaselineRecord,
    EvalCostSummary,
    EvalMetricResult,
    EvalResultRow,
    EvalRunAttempt,
)
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker, QueryResult, ShowRow
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError, StagedFileMetadata
from snowflake_semantic_tools.domain.state import AppliedEntry, State, content_hash

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "reference_project"
DBT_MANIFEST = REPO_ROOT / "tests" / "fixtures" / "reference_project_manifest.json"


def target() -> TargetIdentity:
    return TargetIdentity("verify", "account", Identifier.parse("db"), Identifier.parse("schema"))


def project_copy(tmp_path: Path) -> Path:
    project = tmp_path / "project"
    shutil.copytree(FIXTURE, project)
    shutil.rmtree(project / "target", ignore_errors=True)
    config = project / "sst_config.yml"
    config.write_text(
        config.read_text(encoding="utf-8")
        .replace("snowflake_syntax_check: true", "snowflake_syntax_check: false")
        .replace("strict: true", "strict: false"),
        encoding="utf-8",
    )
    return project


def invoke_with_port(monkeypatch: pytest.MonkeyPatch, port: RecordedSnowflake, args: list[str]):
    if "--project-dir" in args and args[0] in ("plan", "apply"):
        project = Path(args[args.index("--project-dir") + 1])
        if not (project / "target" / "sst" / "manifest.json").is_file():
            compile_project(project)
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
    port.close = lambda: None  # type: ignore[attr-defined]
    return CliRunner().invoke(cli, args)


def common(project: Path) -> list[str]:
    return ["--project-dir", str(project), "--manifest", str(DBT_MANIFEST)]


def compile_project(project: Path) -> None:
    result = CliRunner().invoke(cli, ["compile", *common(project)])
    assert result.exit_code == 0, result.output


def configure_eval_as_applied(
    port: RecordedSnowflake,
    changes: list[dict[str, object]],
    *,
    config_content: str,
) -> None:
    eval_change = next(item for item in changes if item["artifact_type"] == "eval")
    resources = eval_change["physical_resources"]
    assert isinstance(resources, list)
    assert {
        (resource["object_type"], resource["qualified_name"]) for resource in resources if isinstance(resource, dict)
    } == {
        ("TABLE", "SST_REF_DEV.JAFFLE.EVAL_SRC_JAFFLE_ANALYTICS_AGENT_DCE1F8A"),
        ("DATASET", "SST_REF_DEV.JAFFLE.EVAL_JAFFLE_ANALYTICS_AGENT_DCE1F8A"),
    }
    port.existing = {str(resource["qualified_name"]) for resource in resources if isinstance(resource, dict)}
    port.existing.add("SST_REF_DEV.JAFFLE.EVAL_CONFIGS")
    port.stage_formats["SST_REF_DEV.JAFFLE.EVAL_CONFIGS"] = EVAL_STAGE_FILE_FORMAT
    components = eval_change["component_fingerprints"]
    assert isinstance(components, dict)
    config_path = "@SST_REF_DEV.JAFFLE.EVAL_CONFIGS/jaffle_analytics_agent/" f"{components['config']}.yaml"
    port.stage_files = {config_path}
    port.staged_file_metadata = {
        config_path: StagedFileMetadata(
            stage_path=config_path,
            name=config_path[1:],
            size=len(config_content.encode("utf-8")),
            md5=md5(config_content.encode("utf-8"), usedforsecurity=False).hexdigest(),
        )
    }
    port.staged_file_contents = {config_path: config_content.encode("utf-8")}
    source_table = next(
        str(resource["qualified_name"])
        for resource in resources
        if isinstance(resource, dict) and resource["object_type"] == "TABLE"
    )
    original_query = port.query

    def query(sql: str, params=None):
        if sql == f"SELECT COUNT(*) AS ROW_COUNT FROM {source_table}":
            port.queries.append((sql, params))
            return QueryResult(("ROW_COUNT",), ((2,),))
        return original_query(sql, params)

    port.query = query  # type: ignore[method-assign]


def configure_skills_as_published(port: RecordedSnowflake, changes: list[dict[str, object]], project: Path) -> None:
    """Seed the recorded port with the extension versions a previous apply published."""
    for item in changes:
        artifact_type = str(item["artifact_type"])
        if artifact_type not in ("skill", "plugin"):
            continue
        name = str(item["artifact_key"]).split(":", 1)[1]
        bundle = json.loads((project / "target" / "sst" / "sql" / f"{artifact_type}__{name}.json").read_text())
        resources = item["physical_resources"]
        assert isinstance(resources, list)
        stage = next(str(resource["qualified_name"]) for resource in resources if resource["object_type"] == "STAGE")
        target = str(item["target"])
        paths = [str(entry["path"]) for entry in bundle["files"]]
        port.stage_types[stage] = "INTERNAL NO CSE"
        port.stage_files.update(f"@{stage}/{name}/{bundle['alias']}/{path}" for path in paths)
        port.extensions[target] = {
            "type": bundle["type"],
            "comment": bundle["comment"],
            "versions": [
                {
                    "name": "VERSION$2",
                    "alias": bundle["alias"],
                    "location": f"snow://cortex_extension/{target}/versions/version$2/",
                    "files": paths,
                    "is_default": True,
                    "certification": None,
                }
            ],
            "live": None,
        }


def configure_profiles_as_published(port: RecordedSnowflake, changes: list[dict[str, object]], project: Path) -> None:
    """Seed the recorded port with the registry rows and stage trees a previous apply published."""
    for item in changes:
        if item["artifact_type"] != "profile":
            continue
        name = str(item["artifact_key"]).split(":", 1)[1]
        document = json.loads((project / "target" / "sst" / "sql" / f"profile__{name}.json").read_text())
        resources = item["physical_resources"]
        assert isinstance(resources, list)
        stage, registry = (
            next(str(resource["qualified_name"]) for resource in resources if resource["object_type"] == kind)
            for kind in ("STAGE", "TABLE")
        )
        port.stage_types[stage] = "INTERNAL NO CSE"
        port.stage_files.update(
            f"@{stage}/{tree['prefix']}{entry['path']}" for tree in document["trees"] for entry in tree["files"]
        )
        port.tables[registry] = PROFILE_REGISTRY_SHAPE
        port.profile_rows.setdefault(registry, {})[name] = {**document["row"], "ACTIVE": True}


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

    def object_exists(object_type, qualified_name):
        if object_type in {"TABLE", "DATASET", "STAGE"}:
            return qualified_name.sql in existing_eval_resources
        return original_object_exists(object_type, qualified_name)

    port.object_exists = object_exists  # type: ignore[method-assign]
    original_execute = port.execute_script

    def execute_with_eval_resources(statements):
        result = original_execute(statements)
        for statement in statements:
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

    def query_with_eval_row_count(sql, params=None):
        if sql == f"SELECT COUNT(*) AS ROW_COUNT FROM {source_table}":
            port.queries.append((sql, params))
            return QueryResult(("ROW_COUNT",), ((4,),))
        return original_query(sql, params)

    port.query = query_with_eval_row_count  # type: ignore[method-assign]


def test_usage_errors_exit_three() -> None:
    result = CliRunner().invoke(cli, ["plan", "--unknown"])
    assert result.exit_code == 3
    comma = CliRunner().invoke(cli, ["plan", "--select", "a,b"])
    assert comma.exit_code == 3
    supported = CliRunner().invoke(cli, ["plan", "--select", "type:agent"])
    assert supported.exit_code != 3
    machine = CliRunner().invoke(cli, ["--output", "json", "plan", "--unknown"])
    assert machine.exit_code == 3
    assert json.loads(machine.output)["exit_code"] == 3


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


# INT902 means SST broke an invariant; every user-caused condition has its own code.
INT902_ALLOWLIST = {
    "snowflake_semantic_tools/app/apply/errors.py": 1,  # an APL028 outcome the plan never recorded
    # compile_each: rendering a view, tool, or eval that validated (VAL762 guards eval templates)
    "snowflake_semantic_tools/app/compile/base.py": 1,
    "snowflake_semantic_tools/cli/main.py": 1,  # the catch-all for an unexpected exception
}


def test_int902_is_emitted_only_at_the_invariant_allowlist() -> None:
    package = REPO_ROOT / "snowflake_semantic_tools"
    found = {
        path.relative_to(REPO_ROOT).as_posix(): count
        for path in sorted(package.rglob("*.py"))
        if "domain/model/diagnostic/" not in path.as_posix()
        and (count := path.read_text(encoding="utf-8").count('"SST-INT902"'))
    }
    assert found == INT902_ALLOWLIST


def test_unexpected_json_failure_emits_one_error_document(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("unexpected failure")

    monkeypatch.setattr("snowflake_semantic_tools.cli.main._compile_result", fail)
    result = CliRunner().invoke(
        cli,
        ["compile", "--project-dir", str(FIXTURE), "--manifest", str(DBT_MANIFEST), "--output", "json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["diagnostics"][0]["code"] == "SST-INT902"


def test_init_debug_clean_and_list_json(tmp_path: Path) -> None:
    root = tmp_path / "empty"
    initialized = CliRunner().invoke(cli, ["init", "--project-dir", str(root), "--output", "json"])
    assert initialized.exit_code == 0
    assert json.loads(initialized.output)["data"]["created"] == ["sst_config.yml"]
    assert (root / "semantic_models" / "semantic_views").is_dir()

    project = project_copy(tmp_path)
    debugged = CliRunner().invoke(cli, ["debug", "--project-dir", str(project), "--output", "json"])
    assert debugged.exit_code == 0
    debug_data = json.loads(debugged.output)["data"]
    assert debug_data["target"] == "dev"
    # The method is shown, never a credential.
    assert isinstance(debug_data["authentication"], str) and "password" not in set(debug_data) - {"authentication"}

    compiled = CliRunner().invoke(cli, ["compile", *common(project)])
    assert compiled.exit_code == 0
    listed = CliRunner().invoke(cli, ["list", "--project-dir", str(project), "--output", "json"])
    assert listed.exit_code == 0
    assert len(json.loads(listed.output)["data"]) == 14
    cleaned = CliRunner().invoke(cli, ["clean", "--project-dir", str(project), "--output", "json"])
    assert cleaned.exit_code == 0
    assert not (project / "target" / "sst").exists()


def test_plan_live_observation_saved_plan_and_detailed_exitcode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    port = RecordedSnowflake(state={})
    result = invoke_with_port(
        monkeypatch,
        port,
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    assert result.exit_code == 2, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "changes"
    assert [item["action"] for item in payload["data"]["changes"]] == ["create"] * 14
    assert Path(payload["data"]["plan_path"]).is_file()
    assert Path(payload["data"]["sql_path"]).is_dir()

    port = RecordedSnowflake(state={})
    collapsed = invoke_with_port(
        monkeypatch,
        port,
        ["plan", *common(project), "--target", "dev", "--no-detailed-exitcode", "--no-plan-out"],
    )
    assert collapsed.exit_code == 0


def test_plan_noop_uses_remote_state_not_a_prior_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    first_port = RecordedSnowflake(state={})
    first = invoke_with_port(
        monkeypatch,
        first_port,
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    payload = json.loads(first.output)
    manifest_id = payload["data"]["manifest_id"]
    state = {
        item["artifact_key"]: AppliedEntry(
            item["fingerprint"],
            item["target"],
            "now",
            "run",
            "applied",
            item["fingerprint"],
            manifest_id,
            component_fingerprints=(
                tuple(
                    sorted(
                        (
                            *item.get("component_fingerprints", {}).items(),
                            (
                                "config_stage_md5",
                                md5(
                                    (
                                        project / "target" / "sst" / "sql" / "eval__jaffle_analytics_agent.yaml"
                                    ).read_bytes(),
                                    usedforsecurity=False,
                                ).hexdigest(),
                            ),
                        )
                    )
                )
                if item["artifact_type"] == "eval"
                else tuple(sorted(item.get("component_fingerprints", {}).items()))
            ),
            physical_resources=tuple(
                (resource["object_type"], resource["qualified_name"]) for resource in item.get("physical_resources", [])
            ),
        )
        for item in payload["data"]["changes"]
    }
    objects = {}
    for item in payload["data"]["changes"]:
        if item["artifact_type"] in ("eval", "skill", "plugin", "profile"):
            continue
        object_type = {
            "semantic_view": "SEMANTIC VIEW",
            "tool": "CORTEX SEARCH SERVICE",
            "agent": "AGENT",
        }[item["artifact_type"]]
        parts = item["target"].split(".")
        scope = ".".join(parts[:2])
        objects.setdefault((object_type, scope), []).append(
            ShowRow(
                parts[-1],
                parts[0],
                parts[1],
                "OWNER",
                "now",
                f"[sst:{manifest_id}:{item['fingerprint']}]",
                object_type=object_type,
            )
        )
    port = RecordedSnowflake(objects={key: tuple(value) for key, value in objects.items()}, state=state)
    eval_config = project / "target" / "sst" / "sql" / "eval__jaffle_analytics_agent.yaml"
    configure_eval_as_applied(
        port,
        payload["data"]["changes"],
        config_content=eval_config.read_text(encoding="utf-8"),
    )
    configure_skills_as_published(port, payload["data"]["changes"], project)
    configure_profiles_as_published(port, payload["data"]["changes"], project)
    planned = invoke_with_port(
        monkeypatch,
        port,
        ["plan", *common(project), "--target", "dev", "--output", "json", "--no-plan-out"],
    )
    assert planned.exit_code == 0, planned.output
    assert {item["action"] for item in json.loads(planned.output)["data"]["changes"]} == {"noop"}


def test_selected_plan_refuses_a_stale_canonical_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    views = project / "semantic_models" / "semantic_views" / "semantic_views.yml"
    views.write_text(
        views.read_text(encoding="utf-8").replace("Menu products only", "Changed after compile"),
        encoding="utf-8",
    )
    result = invoke_with_port(
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
    assert result.exit_code == 4
    assert "compiled SST manifest is stale" in json.loads(result.output)["data"]["error"]


def test_prune_with_exclude_requires_an_explicit_select_scope(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    compile_project(project)

    result = CliRunner().invoke(
        cli,
        [
            "plan",
            *common(project),
            "--prune",
            "--exclude",
            "jaffle_minimal",
            "--output",
            "json",
        ],
    )

    assert result.exit_code == 3
    assert "prune scope is explicit" in json.loads(result.output)["data"]["error"]


def test_type_exclusion_removes_semantic_views_from_plan_and_prune_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    result = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(state={}),
        [
            "plan",
            *common(project),
            "--target",
            "dev",
            "--exclude",
            "type:semantic_view",
            "--prune",
            "--no-plan-out",
            "--output",
            "json",
        ],
    )

    assert result.exit_code == 2, result.output
    assert {item["artifact_type"] for item in json.loads(result.output)["data"]["changes"]} == {
        "tool",
        "skill",
        "plugin",
        "profile",
        "agent",
        "eval",
    }


def test_an_agent_is_never_planned_without_the_skill_version_it_pins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    excluded = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(state={}),
        ["plan", *common(project), "--target", "dev", "--exclude", "type:skill", "--no-plan-out", "--output", "json"],
    )
    assert excluded.exit_code == 1, excluded.output
    payload = json.loads(excluded.output)
    actions = {item["artifact_key"]: (item["action"], item["reason"]) for item in payload["data"]["changes"]}
    assert actions["agent:jaffle_analytics_agent"] == ("blocked", "dependency_blocked")
    assert actions["eval:jaffle_analytics_agent"] == ("blocked", "dependency_blocked")
    assert actions["agent:jaffle_minimal_agent"] == ("create", "not_present")
    assert [item["message"] for item in payload["diagnostics"] if item["code"] == "SST-PLN030"] == [
        "agent:jaffle_analytics_agent: pins the published version of skill:jaffle-semantics; "
        "select skill:jaffle-semantics as well"
    ]

    together = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(state={}),
        [
            "plan",
            *common(project),
            "--target",
            "dev",
            "--select",
            "agent:jaffle_analytics_agent",
            "--select",
            "skill:jaffle-semantics",
            "--no-plan-out",
            "--output",
            "json",
        ],
    )
    assert together.exit_code == 2, together.output
    assert [(item["artifact_key"], item["action"]) for item in json.loads(together.output)["data"]["changes"]] == [
        ("skill:jaffle-semantics", "create"),
        ("agent:jaffle_analytics_agent", "create"),
    ]


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

    def execute_with_markers(statements):
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
    # skills and the plugin, and the profile stage.
    assert len(apply_port.scripts) == 21
    first_lines = [statements[0].split("\n", 1)[0] for statements in apply_port.scripts]
    assert sum("JAFFLE_TOOLKIT" in line for line in first_lines) == 2
    assert sum("SKILL_BUNDLES " in line and line.startswith("CREATE STAGE") for line in first_lines) == 1
    assert sum("[sst:" in statement for statements in apply_port.scripts for statement in statements) == 7


def break_menu_view(project: Path) -> None:
    path = project / "semantic_models" / "semantic_views" / "core" / "semantic_views.yml"
    text = path.read_text(encoding="utf-8")
    entry = "- \"{{ ref('products') }}\""
    assert entry in text
    path.write_text(text.replace(entry, '- "products"', 1), encoding="utf-8")


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

    def execute_with_markers(statements):
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


def test_plan_without_a_compiled_manifest_names_the_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        _compiled_manifest(tmp_path)
    assert [item.code for item in raised.value.diagnostics] == ["SST-MAN001"]
    assert str(tmp_path / "target" / "sst" / "manifest.json") in raised.value.diagnostics[0].message


def test_a_recognised_snowflake_failure_is_reported_as_its_diagnostic(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise SnowflakePortError("refused", diagnostic=D("SST-PRT001", value="acme", detail="refused"))

    monkeypatch.setattr("snowflake_semantic_tools.cli.main._compile_result", fail)
    args = ["compile", "--project-dir", str(FIXTURE), "--manifest", str(DBT_MANIFEST)]
    as_json = CliRunner().invoke(cli, [*args, "--output", "json"])
    assert as_json.exit_code == 5
    assert [item["code"] for item in json.loads(as_json.output)["diagnostics"]] == ["SST-PRT001"]
    human = CliRunner().invoke(cli, args)
    assert human.exit_code == 5 and "SST-PRT001" in human.output


def test_smoke_suite_is_separate_from_apply(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    manifest = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    manifest_id = manifest["manifest_id"]
    from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
    from snowflake_semantic_tools.domain.state import AppliedEntry

    applied_state = {
        key: AppliedEntry(
            value["fingerprint"],
            value["publish_target"]["qualified_name"],
            "now",
            "run",
            "applied",
            value["fingerprint"],
            manifest_id,
        )
        for key, value in manifest["artifacts"].items()
    }
    markers = {
        value["publish_target"]["qualified_name"]: OwnershipMarker(
            manifest_id,
            value["fingerprint"],
        )
        for value in manifest["artifacts"].values()
    }
    port = RecordedSnowflake(state=applied_state, markers=markers)
    result = invoke_with_port(
        monkeypatch,
        port,
        ["test", *common(project), "--target", "dev", "--suite", "smoke", "--output", "json"],
    )
    assert result.exit_code == 0
    attempted = json.loads(result.output)["data"]["attempted"]
    assert attempted > 3
    assert len(port.queries) == attempted

    unapplied = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(state={}),
        ["test", *common(project), "--target", "dev", "--suite", "smoke", "--output", "json"],
    )
    assert unapplied.exit_code == 1
    assert json.loads(unapplied.output)["diagnostics"][0]["code"] == "SST-APL012"

    views = project / "semantic_models" / "semantic_views" / "semantic_views.yml"
    views.write_text(
        views.read_text(encoding="utf-8").replace("Menu products only", "Changed after compile"),
        encoding="utf-8",
    )
    stale = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(),
        ["test", *common(project), "--target", "dev", "--suite", "smoke", "--output", "json"],
    )
    assert stale.exit_code == 4
    assert "compiled SST manifest is stale" in json.loads(stale.output)["data"]["error"]


def test_eval_suite_uses_common_json_envelope(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    monkeypatch.setattr("snowflake_semantic_tools.cli.main._git_sha", lambda path: "0000000")
    monkeypatch.setattr("snowflake_semantic_tools.app.evals.run._compact_timestamp", lambda: "20260928T010203Z")
    compile_project(project)
    compiled = _compile_result(project, "dev", DBT_MANIFEST)
    compiled_eval = next(item for item in compiled.compiled if isinstance(item, CompiledEval))
    manifest = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    manifest_id = manifest["manifest_id"]
    eval_manifest = manifest["artifacts"][compiled_eval.artifact_key]
    config_content = compiled_eval.rendered.config_yaml.encode("utf-8")
    config_md5 = md5(config_content, usedforsecurity=False).hexdigest()
    config_path = (
        "@SST_REF_DEV.JAFFLE.EVAL_CONFIGS/jaffle_analytics_agent/" f"{compiled_eval.rendered.config_fingerprint}.yaml"
    )
    entry = AppliedEntry(
        compiled_eval.rendered_artifact.fingerprint,
        compiled_eval.dataset_target.sql,
        "now",
        "run",
        "applied",
        compiled_eval.rendered_artifact.fingerprint,
        manifest_id,
        component_fingerprints=(
            *compiled_eval.rendered_artifact.component_fingerprints,
            ("config_stage_md5", config_md5),
        ),
        physical_resources=tuple(
            (resource["object_type"], resource["qualified_name"]) for resource in eval_manifest["physical_resources"]
        ),
    )
    existing = tuple(resource["qualified_name"] for resource in eval_manifest["physical_resources"])
    port = RecordedSnowflake(
        role="RECORDED_ROLE",
        account_locator="RECORDED_ACCOUNT",
        state={compiled_eval.artifact_key: entry},
        existing=(*existing, "SST_REF_DEV.JAFFLE.EVAL_CONFIGS"),
        stage_formats={"SST_REF_DEV.JAFFLE.EVAL_CONFIGS": EVAL_STAGE_FILE_FORMAT},
        staged_file_metadata={
            config_path: StagedFileMetadata(config_path, config_path[1:], len(config_content), config_md5)
        },
        staged_file_contents={config_path: config_content},
        agent_versions={(compiled_eval.agent_target.sql, "committed"): "VERSION$1"},
    )
    result_columns = (
        "RECORD_ID",
        "INPUT_ID",
        "REQUEST_ID",
        "TIMESTAMP",
        "DURATION_MS",
        "INPUT",
        "OUTPUT",
        "ERROR",
        "GROUND_TRUTH",
        "METRIC_NAME",
        "EVAL_AGG_SCORE",
        "METRIC_TYPE",
        "METRIC_STATUS",
        "METRIC_CALLS",
        "TOTAL_INPUT_TOKENS",
        "TOTAL_OUTPUT_TOKENS",
        "LLM_CALL_COUNT",
    )
    metric_types = {
        **{
            str(metric.name): "system"
            for metric in compiled_eval.resolved.config.system_metrics
            if metric.name is not None
        },
        **{metric.name: "custom" for metric in compiled_eval.resolved.custom_metrics},
    }
    result_rows = tuple(
        (
            f"record-{row_index}",
            f"question-{row_index}",
            f"request-{row_index}",
            "now",
            50,
            row["input_query"],
            "Answer",
            None,
            json.dumps(row["ground_truth"], sort_keys=True, separators=(",", ":")),
            metric_name,
            0.9,
            metric_type,
            {"status": 200},
            [],
            10,
            5,
            1,
        )
        for row_index, row in enumerate(json.loads(compiled_eval.rendered.dataset_payload))
        for metric_name, metric_type in metric_types.items()
    )
    response_values = []
    for attempt_number in range(1, 6):
        suffix = "" if attempt_number == 1 else f"_R{attempt_number}"
        response_values.extend(
            (
                QueryResult(
                    ("RUN_NAME", "AGENT_NAME", "AGENT_TYPE", "STATUS", "STATUS_DETAILS"),
                    (
                        (
                            f"EVAL_JAFFLE_ANALYTICS_AGENT_0000000_ci_20260928T010203Z{suffix}",
                            "JAFFLE_ANALYTICS_AGENT",
                            "CORTEX AGENT",
                            "COMPLETED",
                            [],
                        ),
                    ),
                ),
                QueryResult(result_columns, result_rows),
            )
        )
    responses = iter(response_values)

    def query(sql: str, params=None):
        port.queries.append((sql, params))
        return next(responses)

    port.query = query  # type: ignore[method-assign]
    port.query_in_context = lambda scope, sql, params=None: query(sql, params)  # type: ignore[method-assign]
    store = InMemoryEvalStateStore()
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeEvalStateStore", lambda *args: store)
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.evals.suite.capture_baseline",
        lambda item, item_result, **kwargs: EvalBaselineRecord(
            item.artifact_key,
            item.rendered.dataset_fingerprint,
            item.rendered.config_fingerprint,
            item.resolved.config.agent_version or "",
            (),
            (),
            tuple(attempt.run_name for attempt in item_result.attempts),
            "2026-09-01T00:00:00Z",
            "2026-10-01T00:00:00Z",
            kwargs["reason"],
        ),
    )
    result = invoke_with_port(
        monkeypatch,
        port,
        [
            "test",
            *common(project),
            "--target",
            "dev",
            "--suite",
            "evals",
            "--capture-baseline",
            "--reason",
            "initial",
            "--output",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["data"]["suite"] == "evals"
    assert payload["data"]["attempt_count"] == 5
    assert payload["data"]["evals"][0]["attempts"][0]["terminal_status"] == "COMPLETED"
    assert payload["data"]["cost_totals"]["total_input_tokens"] == 200
    assert payload["data"]["regression_count"] == 0
    assert payload["data"]["gate_verdict"] == "captured"
    assert payload["data"]["captured_baselines"] == ["eval:jaffle_analytics_agent"]


def test_eval_suite_refuses_unpublished_eval_before_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    port = RecordedSnowflake(state={})

    result = invoke_with_port(
        monkeypatch,
        port,
        ["test", *common(project), "--target", "dev", "--suite", "evals", "--output", "json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["diagnostics"][0]["code"] == "SST-APL012"
    assert payload["data"]["attempt_count"] == 0
    assert port.scripts == []


def test_eval_suite_reports_every_attempt_in_human_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    compiled_eval = _compile_result(project, "dev", DBT_MANIFEST)
    eval_item = next(item for item in compiled_eval.compiled if isinstance(item, CompiledEval))
    attempt = EvalRunAttempt(
        "EVAL_RUN_R2",
        2,
        "COMPLETED",
        (
            EvalResultRow(
                "question",
                "Question",
                (EvalMetricResult("question", "answer_correctness", 0.9, True),),
            ),
        ),
        EvalCostSummary(duration_ms=50, total_tokens=8, total_input_tokens=10, total_output_tokens=5, llm_call_count=1),
        agent_version="LAST",
    )
    suite = EvalSuiteResult(
        (EvalRunResult(eval_item.artifact_key, (attempt,), DiagnosticBag(), True),), DiagnosticBag()
    )
    monkeypatch.setattr("snowflake_semantic_tools.app.evals.run.RunEvalSuite.run", lambda *args, **kwargs: suite)
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.evals.suite.validate_eval_publication", lambda *args: DiagnosticBag()
    )
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.evals.suite.read_state",
        lambda *args, **kwargs: (State.empty(target()), DiagnosticBag()),
    )
    monkeypatch.setattr(
        "snowflake_semantic_tools.cli.main.ManifestFileStore.read",
        lambda self: _build_manifest(project, compiled_eval, DBT_MANIFEST),
    )
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.evals.suite.capture_baseline",
        lambda item, item_result, **kwargs: EvalBaselineRecord(
            item.artifact_key,
            item.rendered.dataset_fingerprint,
            item.rendered.config_fingerprint,
            item.resolved.config.agent_version or "",
            (),
            (),
            tuple(attempt.run_name for attempt in item_result.attempts),
            "2026-09-01T00:00:00Z",
            "2026-10-01T00:00:00Z",
            kwargs["reason"],
        ),
    )

    result = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(),
        [
            "test",
            *common(project),
            "--target",
            "dev",
            "--suite",
            "evals",
            "--capture-baseline",
            "--reason",
            "initial",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "attempt 2: EVAL_RUN_R2 COMPLETED" in result.output
    assert "answer_correctness: passed=1/1 average=0.9" in result.output
    assert "duration_ms=50 tokens=8" in result.output


def test_eval_suite_preflight_failure_keeps_json_schema_stable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)

    result = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(state={}),
        ["test", *common(project), "--target", "dev", "--suite", "evals", "--output", "json"],
    )

    payload = json.loads(result.output)
    assert result.exit_code == 1
    assert payload["data"]["cost_totals"] == {
        "duration_ms": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "llm_call_count": 0,
        "credits": None,
        "credit_attribution": "not_requested",
    }
    assert payload["data"]["regression_count"] == 0
    assert payload["data"]["gate_verdict"] == "not_evaluated"


def test_eval_baseline_capture_requires_reason(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = CliRunner().invoke(
        cli,
        ["test", *common(project), "--suite", "evals", "--capture-baseline", "--output", "json"],
    )
    assert result.exit_code == 3
    assert "requires --reason" in json.loads(result.output)["data"]["error"]


def test_human_output_and_usage_branches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    initialized = CliRunner().invoke(cli, ["init", "--project-dir", str(tmp_path / "human")])
    assert initialized.exit_code == 0 and "initialized" in initialized.output
    debugged = CliRunner().invoke(cli, ["debug", "--project-dir", str(project)])
    assert debugged.exit_code == 0 and "state_table:" in debugged.output
    compiled = CliRunner().invoke(cli, ["compile", *common(project), "--print-ddl"])
    assert compiled.exit_code == 0 and "CREATE OR REPLACE" in compiled.output

    conflict = CliRunner().invoke(
        cli,
        ["plan", *common(project), "--plan-out", str(tmp_path / "p.json"), "--no-plan-out"],
    )
    assert conflict.exit_code == 3
    prune = CliRunner().invoke(cli, ["apply", *common(project), "--prune"])
    assert prune.exit_code == 3

    port = RecordedSnowflake(state={})
    planned = invoke_with_port(monkeypatch, port, ["plan", *common(project), "--target", "dev"])
    assert planned.exit_code == 2 and "Plan:" in planned.output
    cleaned = CliRunner().invoke(cli, ["clean", "--project-dir", str(project)])
    assert cleaned.exit_code == 0 and "removed" in cleaned.output
    nothing = CliRunner().invoke(cli, ["clean", "--project-dir", str(project)])
    assert nothing.exit_code == 0 and "nothing to remove" in nothing.output


def test_debug_connection_and_human_apply_prompt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    debug_port = RecordedSnowflake(role="R", account_locator="A")
    debugged = invoke_with_port(
        monkeypatch,
        debug_port,
        ["debug", "--project-dir", str(project), "--test-connection", "--output", "json"],
    )
    assert debugged.exit_code == 0
    assert json.loads(debugged.output)["data"]["current_role"] == "R"

    apply_port = RecordedSnowflake(state={})
    applied = invoke_with_port(
        monkeypatch,
        apply_port,
        ["apply", *common(project), "--target", "dev"],
    )
    assert applied.exit_code != 0 and "Apply this plan?" in applied.output


def test_a_declined_or_interrupted_run_exits_130_without_an_internal_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    declined = invoke_with_port(
        monkeypatch, RecordedSnowflake(state={}), ["apply", *common(project), "--target", "dev"]
    )
    assert declined.exit_code == 130, declined.output
    assert "Apply this plan?" in declined.output and "Aborted." in declined.output
    assert "SST-INT902" not in declined.output

    def interrupt(params: object) -> RecordedSnowflake:
        raise KeyboardInterrupt

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", interrupt)
    human = CliRunner().invoke(cli, ["plan", *common(project), "--target", "dev"])
    assert human.exit_code == 130 and "SST-INT902" not in human.output
    as_json = CliRunner().invoke(cli, ["plan", *common(project), "--target", "dev", "--output", "json"])
    assert as_json.exit_code == 130
    envelope = json.loads(as_json.output)
    assert (envelope["exit_code"], envelope["status"], envelope["diagnostics"]) == (130, "error", [])


def invoke_counting_closes(
    monkeypatch: pytest.MonkeyPatch, port: RecordedSnowflake, args: list[str]
) -> tuple[Result, list[str]]:
    """Run `sst` against `port`, recording every close() so a test can prove the connection was released."""
    closes: list[str] = []
    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", lambda params: port)
    port.close = lambda: closes.append("closed")  # type: ignore[attr-defined]
    return CliRunner().invoke(cli, args), closes


def test_plan_closes_its_connection_when_it_fails_after_connecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    plan = ["plan", *common(project), "--target", "dev", "--output", "json"]

    class DroppedMidPlan(RecordedSnowflake):
        def read_state(self, state_table: QualifiedName, target_name: str) -> None:
            raise SnowflakePortError("connection reset while reading state")

    dropped, closes = invoke_counting_closes(monkeypatch, DroppedMidPlan(), plan)
    assert (
        dropped.exit_code == 5 and json.loads(dropped.output)["data"]["error"] == "connection reset while reading state"
    )
    assert closes == ["closed"]

    (project / "target" / "sst" / "state.dev.json").write_text("[]", encoding="utf-8")
    unreadable, closes = invoke_counting_closes(monkeypatch, RecordedSnowflake(state={}), plan)
    assert unreadable.exit_code == 4
    assert [item["code"] for item in json.loads(unreadable.output)["diagnostics"]] == ["SST-MAN022"]
    assert closes == ["closed"]


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


def test_the_eval_suite_closes_its_connection_when_it_fails_before_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    evals = ["test", *common(project), "--target", "dev", "--suite", "evals", "--output", "json"]
    StateFileStore(project / "target" / "sst" / "state.dev.json").acquire_lock("apply-run", break_stale=False)
    locked, closes = invoke_counting_closes(monkeypatch, RecordedSnowflake(state={}), evals)
    assert locked.exit_code == 4 and "apply-run holds the target lock" in json.loads(locked.output)["data"]["error"]
    assert closes == ["closed"]

    def unwritable(self: StateFileStore, run_id: str, *, break_stale: bool) -> tuple[bool, str | None, bool]:
        raise PermissionError("target/sst is read-only")

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.StateFileStore.acquire_lock", unwritable)
    failed, closes = invoke_counting_closes(monkeypatch, RecordedSnowflake(state={}), evals)
    assert failed.exit_code == 4 and json.loads(failed.output)["data"]["error"] == "target/sst is read-only"
    assert closes == ["closed"]


def test_connection_binds_plan_to_live_account_and_refuses_role_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    port = RecordedSnowflake(state={}, role="SST_REFERENCE_OFFLINE", account_locator="LIVE_ACCOUNT")
    planned = invoke_with_port(
        monkeypatch,
        port,
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    assert planned.exit_code == 2
    plan_path = Path(json.loads(planned.output)["data"]["plan_path"])
    saved = json.loads(plan_path.read_text(encoding="utf-8"))
    assert saved["target"]["account_locator"] == "LIVE_ACCOUNT"

    profiles = project / "profiles.yml"
    profiles.write_text(
        profiles.read_text(encoding="utf-8").replace(
            "warehouse: SST_REF_WH", "role: EXPECTED\n      warehouse: SST_REF_WH", 1
        ),
        encoding="utf-8",
    )
    wrong_role = RecordedSnowflake(state={}, role="WRONG", account_locator="LIVE_ACCOUNT")
    refused = invoke_with_port(
        monkeypatch,
        wrong_role,
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    assert refused.exit_code == 5


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


def test_plan_honors_configured_strict_validation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    config = project / "sst_config.yml"
    config.write_text(
        config.read_text(encoding="utf-8").replace("strict: false", "strict: true"),
        encoding="utf-8",
    )
    original = __import__("snowflake_semantic_tools.cli.main", fromlist=["_compile_result"])._compile_result

    def with_warning(*args, **kwargs):
        result = original(*args, **kwargs)
        return dataclasses.replace(result, diagnostics=DiagnosticBag((D("SST-LOD003", file="warning.yml"),)))

    monkeypatch.setattr("snowflake_semantic_tools.cli.main._compile_result", with_warning)
    refused = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(state={}),
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    assert refused.exit_code == 1
    allowed = invoke_with_port(
        monkeypatch,
        RecordedSnowflake(state={}),
        [
            "plan",
            *common(project),
            "--target",
            "dev",
            "--no-strict",
            "--output",
            "json",
        ],
    )
    assert allowed.exit_code == 2


def test_golden_json_failure_missing_file_and_smoke_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    empty_golden = tmp_path / "golden"
    empty_golden.mkdir()
    missing = CliRunner().invoke(
        cli,
        ["test", *common(project), "--suite", "golden", "--golden-dir", str(empty_golden), "--output", "json"],
    )
    assert missing.exit_code == 1
    assert len(json.loads(missing.output)["data"]["failures"]) == 15

    from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
    from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError
    from snowflake_semantic_tools.domain.state import AppliedEntry

    manifest = json.loads((project / "target" / "sst" / "manifest.json").read_text(encoding="utf-8"))
    manifest_id = manifest["manifest_id"]
    state = {
        key: AppliedEntry(
            value["fingerprint"],
            value["publish_target"]["qualified_name"],
            "now",
            "run",
            "applied",
            value["fingerprint"],
            manifest_id,
        )
        for key, value in manifest["artifacts"].items()
    }
    markers = {
        value["publish_target"]["qualified_name"]: OwnershipMarker(
            manifest_id,
            value["fingerprint"],
        )
        for value in manifest["artifacts"].values()
    }
    port = RecordedSnowflake(state=state, markers=markers)
    port.query = lambda sql, params=None: (_ for _ in ()).throw(SnowflakePortError("broken"))  # type: ignore[method-assign]
    smoke = invoke_with_port(
        monkeypatch,
        port,
        ["test", *common(project), "--target", "dev", "--suite", "smoke", "--fail-fast"],
    )
    assert smoke.exit_code == 1 and "SST-APL100" in smoke.output
