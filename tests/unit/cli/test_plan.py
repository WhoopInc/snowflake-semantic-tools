"""`sst plan`: live observation, saved plans, scopes, validation settings, and its connection."""

from __future__ import annotations

import dataclasses
import json
from hashlib import md5
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.app.lifecycle.evals import EVAL_STAGE_FILE_FORMAT
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.cli.wiring.manifest import compiled_manifest as _compiled_manifest
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ShowRow
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagedFileMetadata
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.cli_projects import common, compile_project, invoke_counting_closes, invoke_with_port, skill_project
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import PROFILE_REGISTRY_SHAPE, FakeSnowflake


def test_plan_requires_live_snowflake_observation(tmp_path: Path) -> None:
    project = project_copy(tmp_path, offline=False)
    compiled = CliRunner().invoke(
        cli,
        [
            "compile",
            "--project-dir",
            str(project),
            "--manifest",
            str(DBT_MANIFEST),
        ],
    )
    assert compiled.exit_code == 0
    result = CliRunner().invoke(
        cli,
        [
            "plan",
            "--project-dir",
            str(project),
            "--manifest",
            str(DBT_MANIFEST),
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 5
    assert json.loads(result.output)["exit_code"] == 5


def configure_eval_as_applied(
    port: FakeSnowflake,
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
    # A publish that finished left SST's version on the dataset.
    port.dataset_version_names["SST_REF_DEV.JAFFLE.EVAL_JAFFLE_ANALYTICS_AGENT_DCE1F8A"] = [
        str(components["dataset_version"])
    ]
    config_path = f"@SST_REF_DEV.JAFFLE.EVAL_CONFIGS/jaffle_analytics_agent/{components['config']}.yaml"
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
    port.table_row_counts[source_table] = 2


def configure_skills_as_published(port: FakeSnowflake, changes: list[dict[str, object]], project: Path) -> None:
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


def configure_profiles_as_published(port: FakeSnowflake, changes: list[dict[str, object]], project: Path) -> None:
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


def test_plan_live_observation_saved_plan_and_detailed_exitcode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    port = FakeSnowflake(state={})
    result = invoke_with_port(
        monkeypatch,
        port,
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    assert result.exit_code == 2, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "changes"
    # Every artifact, in publish order -- views, tools, skills, plugins, profiles, agents (a
    # delegate before the agent that calls it), evals -- and where the folder routes send it.
    assert [
        (item["artifact_key"], item["action"], item["reason"], item["target"]) for item in payload["data"]["changes"]
    ] == [
        ("semantic_view:jaffle_menu", "create", "not_present", "SST_REF_DEV.CORE.JAFFLE_MENU"),
        ("semantic_view:jaffle_minimal", "create", "not_present", "SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL"),
        ("semantic_view:jaffle_sales", "create", "not_present", "SST_REF_DEV.JAFFLE.JAFFLE_SALES"),
        ("tool:menu_docs_search", "create", "not_present", "SST_REF_DEV.JAFFLE.MENU_DOCS_SEARCH"),
        ("skill:jaffle-catalogue", "create", "not_present", "SST_REF_DEV.JAFFLE.JAFFLE_CATALOGUE"),
        ("skill:jaffle-operations", "create", "not_present", "SST_REF_DEV.JAFFLE.JAFFLE_OPERATIONS"),
        ("skill:jaffle-semantics", "create", "not_present", "SST_REF_DEV.JAFFLE.JAFFLE_SEMANTICS"),
        ("plugin:jaffle-toolkit", "create", "not_present", "SST_REF_DEV.JAFFLE.JAFFLE_TOOLKIT"),
        ("profile:jaffle-analyst", "create", "not_present", "SST_REF_DEV.JAFFLE.PROFILE_REGISTRY"),
        ("profile:jaffle-operator", "create", "not_present", "SST_REF_DEV.JAFFLE.PROFILE_REGISTRY"),
        ("agent:jaffle_delivery_agent", "create", "not_present", "SST_REF_DEV.JAFFLE.JAFFLE_DELIVERY_AGENT"),
        ("agent:jaffle_minimal_agent", "create", "not_present", "SST_REF_DEV.OPS.JAFFLE_MINIMAL_AGENT"),
        ("agent:jaffle_analytics_agent", "create", "not_present", "SST_REF_DEV.JAFFLE.JAFFLE_ANALYTICS_AGENT"),
        (
            "eval:jaffle_analytics_agent",
            "create",
            "not_present",
            "SST_REF_DEV.JAFFLE.EVAL_JAFFLE_ANALYTICS_AGENT_DCE1F8A",
        ),
    ]
    assert Path(payload["data"]["plan_path"]).is_file()
    assert Path(payload["data"]["sql_path"]).is_dir()

    port = FakeSnowflake(state={})
    collapsed = invoke_with_port(
        monkeypatch,
        port,
        ["plan", *common(project), "--target", "dev", "--no-detailed-exitcode", "--no-plan-out"],
    )
    assert collapsed.exit_code == 0


def published_target(project: Path, payload: dict[str, Any]) -> FakeSnowflake:
    """A target where apply published every change of the plan `payload`, from its manifest.

    Reads the compiled files of `project`, so call it before editing the project.
    """
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
    objects: dict[tuple[str, str], list[ShowRow]] = {}
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
    port = FakeSnowflake(objects={key: tuple(value) for key, value in objects.items()}, state=state)
    eval_config = project / "target" / "sst" / "sql" / "eval__jaffle_analytics_agent.yaml"
    configure_eval_as_applied(
        port,
        payload["data"]["changes"],
        config_content=eval_config.read_text(encoding="utf-8"),
    )
    configure_skills_as_published(port, payload["data"]["changes"], project)
    configure_profiles_as_published(port, payload["data"]["changes"], project)
    return port


def test_plan_noop_uses_remote_state_not_a_prior_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    first = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    port = published_target(project, json.loads(first.output))
    planned = invoke_with_port(
        monkeypatch,
        port,
        ["plan", *common(project), "--target", "dev", "--output", "json", "--no-plan-out"],
    )
    assert planned.exit_code == 0, planned.output
    assert {item["action"] for item in json.loads(planned.output)["data"]["changes"]} == {"noop"}


@pytest.mark.parametrize(
    ("relative", "old", "new", "updated"),
    [
        pytest.param(
            "semantic_models/metrics/metrics.yml",
            "COUNT({{ ref('order_items', 'order_item_id') }})",
            "COUNT(DISTINCT {{ ref('order_items', 'order_item_id') }})",
            # The agent's Analyst tool names the view, so the view's metrics are not in its spec.
            {"semantic_view:jaffle_menu"},
            id="one-metric-of-one-view",
        ),
        pytest.param(
            "agents/jaffle_analytics/instructions/orchestration.md",
            "## Tool selection\n",
            "## Tool selection\n\nPrefer the semantic views over the documents.\n",
            {"agent:jaffle_analytics_agent"},
            id="one-agent-s-instructions",
        ),
        pytest.param(
            "skills/jaffle-semantics/SKILL.md",
            "---\n",
            "---\n\n",
            # The skill's new version moves the pin of the agent, the plugin and the profile that carry it.
            {
                "skill:jaffle-semantics",
                "plugin:jaffle-toolkit",
                "profile:jaffle-analyst",
                "profile:jaffle-operator",
                "agent:jaffle_analytics_agent",
            },
            id="one-skill-its-dependents-pin",
        ),
    ],
)
def test_one_edit_plans_only_what_its_render_reaches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, relative: str, old: str, new: str, updated: set[str]
) -> None:
    project = project_copy(tmp_path)
    first = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    port = published_target(project, json.loads(first.output))
    path = project / relative
    text = path.read_text(encoding="utf-8")
    assert old in text
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    compile_project(project)

    planned = invoke_with_port(
        monkeypatch,
        port,
        ["plan", *common(project), "--target", "dev", "--output", "json", "--no-plan-out"],
    )

    payload = json.loads(planned.output)
    assert planned.exit_code == 2, planned.output
    assert payload["data"]["manifest_id"] != json.loads(first.output)["data"]["manifest_id"]
    actions = {item["artifact_key"]: (item["action"], item["reason"]) for item in payload["data"]["changes"]}
    assert {key for key, (action, _) in actions.items() if action != "noop"} == updated
    assert {actions[key] for key in updated} == {("update", "fingerprint_differs")}
    assert len(actions) == 14


def test_capture_prior_reads_every_published_kind_without_failing_the_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The reference project publishes views, a search service, agents, skills, a plugin, a
    # profile, and an eval; a plan capturing their priors must still exit as it would without.
    project = project_copy(tmp_path)
    first = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    payload = json.loads(first.output)
    port = published_target(project, payload)
    targets = {str(item["artifact_key"]): str(item["target"]) for item in payload["data"]["changes"]}
    for key, target in targets.items():
        if key.startswith("tool:"):
            port.definitions[target] = f"create or replace {key}"
        if key.startswith("semantic_view:"):
            # A view's GET_DDL text in the restorable form, which the capture keeps as it is.
            port.definitions[target] = f"create or replace semantic view {target}\n  COPY GRANTS"
        if key.startswith("agent:"):
            port.agent_version_rows[target] = (
                AgentVersion(None, None, specification="{}"),
                AgentVersion("VERSION$2", None, is_default=True, specification='{"models":{"orchestration":"auto"}}'),
                AgentVersion("VERSION$1", None, specification="{}"),
            )
    planned = invoke_with_port(
        monkeypatch,
        port,
        ["plan", *common(project), "--target", "dev", "--capture-prior", "--output", "json", "--no-plan-out"],
    )
    assert planned.exit_code == 0, planned.output
    result = json.loads(planned.output)
    assert not [item for item in result["diagnostics"] if item["severity"] in ("error", "warning")]
    priors = {item["artifact_key"]: item["prior_definition"] for item in result["data"]["changes"]}
    agents = [key for key in priors if key.startswith("agent:")]
    assert agents
    for key in agents:
        prior = priors[key]
        assert prior.startswith(f"-- agent {targets[key]} serves VERSION$2 (the default).\n")
        assert f"ALTER AGENT {targets[key]} SET DEFAULT_VERSION = 'VERSION$2';" in prior
        assert prior.endswith('{\n  "models": {\n    "orchestration": "auto"\n  }\n}\n')
    for key, prior in priors.items():
        if key.startswith(("semantic_view:", "tool:")):
            assert prior == port.definitions[targets[key]]
        elif not key.startswith("agent:"):
            # A skill, plugin, profile, or eval publishes content-addressed resources.
            assert prior is None, key
    # Nothing is updated, so `--full` prints no definition that an update would replace.
    full = invoke_with_port(
        monkeypatch, port, ["plan", *common(project), "--target", "dev", "--capture-prior", "--full", "--no-plan-out"]
    )
    assert full.exit_code == 0, full.output
    assert "prior definition:" not in full.output


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
        FakeSnowflake(state={}),
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

    # Nothing exists yet, so the agent that reads the excluded views cannot publish before them.
    assert result.exit_code == 1, result.output
    payload = json.loads(result.output)
    assert {item["artifact_type"] for item in payload["data"]["changes"]} == {
        "tool",
        "skill",
        "plugin",
        "profile",
        "agent",
        "eval",
    }
    assert {item["params"]["blocker"] for item in payload["diagnostics"] if item["code"] == "SST-PLN038"} == {
        "semantic_view:jaffle_sales",
        "semantic_view:jaffle_menu",
    }


def test_an_agent_is_never_planned_without_the_skill_version_it_pins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = project_copy(tmp_path)
    excluded = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
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
        FakeSnowflake(state={}),
        [
            "plan",
            *common(project),
            "--target",
            "dev",
            "--select",
            "agent:jaffle_analytics_agent",
            "--select",
            "skill:jaffle-semantics",
            "--select",
            "type:semantic_view",
            "--select",
            "tool:menu_docs_search",
            "--no-plan-out",
            "--output",
            "json",
        ],
    )
    # With every dependency it does not find in Snowflake selected too, the agent publishes
    # after them (SST-PLN038 otherwise).
    assert together.exit_code == 2, together.output
    changes = [(item["artifact_key"], item["action"]) for item in json.loads(together.output)["data"]["changes"]]
    assert ("skill:jaffle-semantics", "create") in changes
    assert changes[-1] == ("agent:jaffle_analytics_agent", "create")


def test_plan_without_a_compiled_manifest_names_the_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ProjectError) as raised:
        _compiled_manifest(tmp_path)
    assert [item.code for item in raised.value.diagnostics] == ["SST-MAN001"]
    assert str(tmp_path / "target" / "sst" / "manifest.json") in raised.value.diagnostics[0].message


def test_plan_closes_its_connection_when_it_fails_after_connecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    compile_project(project)
    plan = ["plan", *common(project), "--target", "dev", "--output", "json"]

    class DroppedMidPlan(FakeSnowflake):
        def read_state(self, state_table: QualifiedName, target_name: str) -> None:
            raise SnowflakePortError("connection reset while reading state")

    dropped, closes = invoke_counting_closes(monkeypatch, DroppedMidPlan(), plan)
    assert (
        dropped.exit_code == 5 and json.loads(dropped.output)["data"]["error"] == "connection reset while reading state"
    )
    assert closes == ["closed"]

    (project / "target" / "sst" / "state.dev.json").write_text("[]", encoding="utf-8")
    unreadable, closes = invoke_counting_closes(monkeypatch, FakeSnowflake(state={}), plan)
    assert unreadable.exit_code == 4
    assert [item["code"] for item in json.loads(unreadable.output)["diagnostics"]] == ["SST-MAN022"]
    assert closes == ["closed"]


def test_connection_binds_plan_to_live_account_and_refuses_role_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    port = FakeSnowflake(state={}, role="SST_REFERENCE_OFFLINE", account_locator="LIVE_ACCOUNT")
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
    wrong_role = FakeSnowflake(state={}, role="WRONG", account_locator="LIVE_ACCOUNT")
    refused = invoke_with_port(
        monkeypatch,
        wrong_role,
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    assert refused.exit_code == 5


def test_plan_honors_configured_strict_validation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    config = project / "sst_config.yml"
    config.write_text(
        config.read_text(encoding="utf-8").replace("strict: false", "strict: true"),
        encoding="utf-8",
    )
    original = __import__("snowflake_semantic_tools.cli.wiring.compile", fromlist=["compile_result"]).compile_result

    def with_warning(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        return dataclasses.replace(result, diagnostics=DiagnosticBag((D("SST-LOD003", file="warning.yml"),)))

    monkeypatch.setattr("snowflake_semantic_tools.cli.wiring.compile.compile_result", with_warning)
    refused = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
        ["plan", *common(project), "--target", "dev", "--output", "json"],
    )
    assert refused.exit_code == 1
    allowed = invoke_with_port(
        monkeypatch,
        FakeSnowflake(state={}),
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


def test_a_strict_plan_goes_ahead_when_the_baseline_holds_the_promoted_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = project_copy(tmp_path)
    added = CliRunner().invoke(cli, ["baseline", "add", "--all-warnings", "--yes", *common(project)])
    assert added.exit_code == 0, added.output
    args = ["plan", *common(project), "--target", "dev", "--strict", "--output", "json"]

    held = invoke_with_port(monkeypatch, FakeSnowflake(state={}), args)
    assert held.exit_code == 2, held.output
    refused = invoke_with_port(monkeypatch, FakeSnowflake(state={}), [*args, "--no-baseline"])
    assert refused.exit_code == 1
    assert json.loads(refused.stdout)["data"] == {}


def test_report_only_prunes_are_listed_but_are_not_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import shutil

    project = skill_project(tmp_path / "skills")
    port = FakeSnowflake(existing=())
    assert CliRunner().invoke(cli, ["compile", "--project-dir", str(project)]).exit_code == 0
    applied = invoke_with_port(monkeypatch, port, ["apply", "--project-dir", str(project), "--yes", "--output", "json"])
    assert applied.exit_code == 0, applied.output
    shutil.rmtree(project / "skills" / "month-close")
    assert CliRunner().invoke(cli, ["compile", "--project-dir", str(project)]).exit_code == 0
    scripts = len(port.scripts)

    # State already records the skill, so the report-only prune changes nothing to apply:
    # the plan lists it and exits 0, detailed exit code or not.
    first = invoke_with_port(monkeypatch, port, ["plan", "--project-dir", str(project), "--prune", "--output", "json"])
    assert first.exit_code == 0, first.output
    assert json.loads(first.output)["data"]["report_only"] == ["skill:month-close"]
    human = invoke_with_port(monkeypatch, port, ["plan", "--project-dir", str(project), "--prune"])
    assert human.exit_code == 0, human.output
    quiet = invoke_with_port(
        monkeypatch, port, ["plan", "--project-dir", str(project), "--prune", "--no-detailed-exitcode"]
    )
    assert quiet.exit_code == 0, quiet.output
    assert "0 to prune" in human.output and "1 report-only (SST never removes these)." in human.output
    assert "- skill:month-close PRUNE - orphaned (report only)" in human.output

    # Apply executes nothing on Snowflake's objects; it still records the run.
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
        assert [(item["code"], item["severity"]) for item in payload["diagnostics"]] == [
            ("SST-PLN034", "info"),
            ("SST-VAL805", "warning"),
            ("SST-MAN026", "info"),
            ("SST-PLN016", "info"),
        ]
