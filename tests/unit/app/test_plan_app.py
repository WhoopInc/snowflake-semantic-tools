from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.plan import PlanArtifacts, observe
from snowflake_semantic_tools.domain.model.lifecycle import Action, GrantRow, ShowRow
from snowflake_semantic_tools.domain.model.registry import (
    SEMANTIC_REGISTRY,
    ArtifactLifecycle,
    ArtifactType,
    GrantPreservation,
    Registry,
)
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError

from .conftest import InMemorySnowflake
from .helpers import change, manifest, rendered, state, target


def test_observe_collects_markers_grants_and_errors() -> None:
    port = InMemorySnowflake()
    artifact = rendered()
    port.rows = (ShowRow("V", "DB", "SCHEMA", "OWNER", "now", f"comment [sst:{'a' * 64}:{'b' * 64}]"),)
    port.grants[artifact.target.sql] = (GrantRow("SELECT", "ROLE", "R"),)
    observation, diagnostics = observe(
        port,
        SEMANTIC_REGISTRY,
        (artifact.target,),
        fetched_at="now",
    )
    assert observation.artifacts[artifact.key].marker is not None
    assert observation.artifacts[artifact.key].explicit_grants[0].grantee_name == "R"
    assert diagnostics == ()

    port.show_error = SnowflakePortError("offline")
    empty, failed = observe(port, SEMANTIC_REGISTRY, (artifact.target,), fetched_at="later")
    assert empty.artifacts == {} and failed[0].code == "SST-PLN001"


def test_plan_use_case_merges_observation_diagnostics() -> None:
    port = InMemorySnowflake()
    artifact = rendered()
    port.show_error = SnowflakePortError("no privilege")
    value = PlanArtifacts(port).run(
        {artifact.key: artifact},
        manifest({artifact.key: artifact}),
        state(),
        target(),
        fetched_at="now",
    )
    assert value.diagnostics[0].code == "SST-PLN001"
    assert value.changes[0].action.value == "create"


def test_observe_grant_failure_keeps_unknown_not_empty() -> None:
    port = InMemorySnowflake()
    artifact = rendered()
    port.rows = (ShowRow("V", "DB", "SCHEMA", "O", "now"),)
    port.grant_error = SnowflakePortError("denied")
    observation, diagnostics = observe(port, SEMANTIC_REGISTRY, (artifact.target,), fetched_at="now")
    assert observation.artifacts[artifact.key].grants is None
    assert diagnostics[0].code == "SST-PLN001"


def test_observe_skips_types_without_objects_and_merges_healthy_changes_without_extra_diagnostics() -> None:
    registry = Registry(
        MappingProxyType(
            {
                "virtual": ArtifactType(
                    "virtual",
                    "virtuals",
                    1,
                    None,
                    (),
                    "",
                    prunable=False,
                    replaces_on_update=False,
                    grant_preservation=GrantPreservation.NONE,
                    lifecycle=ArtifactLifecycle.COMPOSITE,
                ),
            }
        ),
        MappingProxyType({}),
    )
    port = InMemorySnowflake()
    port.show_error = SnowflakePortError("composite artifacts have no generic object observation")
    observation, diagnostics = observe(
        port,
        registry,
        (rendered().target,),
        fetched_at="now",
        artifact_types=frozenset(("virtual",)),
        observed_object_types={"virtual": frozenset(("",))},
    )
    assert observation.artifacts == {} and diagnostics == ()

    port = InMemorySnowflake()
    artifact = rendered()
    value = PlanArtifacts(port).run(
        {artifact.key: artifact},
        manifest({artifact.key: artifact}),
        state(),
        target(),
        fetched_at="now",
    )
    assert value.diagnostics == ()


def test_observe_queries_only_requested_artifact_types() -> None:
    port = InMemorySnowflake()
    artifact = rendered()
    observation, diagnostics = observe(
        port,
        SEMANTIC_REGISTRY,
        (artifact.target,),
        fetched_at="now",
        artifact_types=frozenset(("semantic_view",)),
    )
    assert observation.artifacts == {}
    assert diagnostics == ()


def test_observe_agent_live_version_selects_update_program() -> None:
    artifact = rendered()
    artifact = replace(
        artifact,
        key="agent:v",
        artifact_type="agent",
        object_type="AGENT",
        update_statements=("ALTER AGENT ADD VERSION",),
        update_live_statements=("ALTER AGENT COMMIT", "ALTER AGENT ADD VERSION"),
    )
    port = InMemorySnowflake()
    port.rows = (ShowRow("V", "DB", "SCHEMA", "OWNER", "now", object_type="AGENT"),)
    port.live_agents.add(artifact.target.sql)
    observation, diagnostics = observe(
        port,
        SEMANTIC_REGISTRY,
        (artifact.target,),
        fetched_at="now",
        artifact_types=frozenset(("agent",)),
        observed_object_types={"agent": frozenset(("AGENT",))},
        desired_artifacts={artifact.key: artifact},
    )
    assert diagnostics == ()
    observed = next(iter(observation.artifacts.values()))
    assert observed.has_live_version
    assert artifact.for_action(Action.UPDATE, observed).statements[0] == "ALTER AGENT COMMIT"


def test_plan_reports_composite_prune_when_generic_observation_has_no_change() -> None:
    artifact = rendered()
    prior = state()
    from snowflake_semantic_tools.domain.state.model import AppliedEntry

    entry = AppliedEntry(
        artifact.fingerprint,
        artifact.target.sql,
        "now",
        "run",
        "applied",
        artifact.fingerprint,
        "a" * 64,
    )
    prior = replace(prior, applied=MappingProxyType({"virtual:old": entry}))
    handler = _CompositeHandler()
    result = PlanArtifacts(InMemorySnowflake(), lifecycle_handlers={"virtual": handler}).run(
        {artifact.key: artifact},
        manifest({artifact.key: artifact}),
        prior,
        target(),
        fetched_at="now",
        include_prune=True,
        prune_types=frozenset(("virtual",)),
        prune_keys=frozenset(("virtual:old",)),
    )
    assert result.changes[-1].key == "virtual:old"
    assert result.changes[-1].action is Action.PRUNE


class _CompositeHandler:
    artifact_type = "virtual"

    def report_prune(self, artifact_key, state_entry):
        del state_entry
        artifact = replace(rendered("OLD"), key=artifact_key, artifact_type="virtual")
        return replace(change(artifact, Action.PRUNE), artifact_type="virtual")
