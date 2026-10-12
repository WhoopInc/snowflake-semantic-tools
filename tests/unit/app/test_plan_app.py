from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.observe import DEFAULT_OBSERVE_OPTIONS, ObserveOptions, observe
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    Change,
    ChangeSet,
    CompositePlan,
    GrantRow,
    OwnershipMarker,
    RenderedArtifact,
    ShowRow,
)
from snowflake_semantic_tools.domain.model.registry import (
    SEMANTIC_REGISTRY,
    ArtifactLifecycle,
    ArtifactType,
    GrantPreservation,
    Registry,
)
from snowflake_semantic_tools.domain.plan.properties import changed_properties
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry, AppliedResourceInput, Manifest
from tests.helpers.artifact_builders import change, manifest, rendered, state, target
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement, texts


def test_observe_collects_markers_grants_and_errors() -> None:
    port = FakeSnowflake()
    artifact = rendered()
    port.show(ShowRow("V", "DB", "SCHEMA", "OWNER", "now", f"comment [sst:{'a' * 64}:{'b' * 64}]"))
    port.grants[artifact.target.sql] = (GrantRow("SELECT", "ROLE", "R"),)
    observation, diagnostics = observe(
        port,
        SEMANTIC_REGISTRY,
        (artifact.target,),
        fetched_at="now",
        desired_artifacts={artifact.key: artifact},
    )
    assert observation.artifacts[artifact.key].marker is not None
    assert observation.artifacts[artifact.key].explicit_grants[0].grantee_name == "R"
    assert diagnostics == ()

    # An object this plan does not render -- a prune candidate or an unrelated one --
    # is never replaced, so its grants are not read (an unrelated routine could not
    # even be addressed without its signature).
    unrelated, quiet = observe(port, SEMANTIC_REGISTRY, (artifact.target,), fetched_at="now")
    assert unrelated.artifacts[artifact.key].grants is None and quiet == ()

    port.fail("show_objects", SnowflakePortError("offline"))
    empty, failed = observe(port, SEMANTIC_REGISTRY, (artifact.target,), fetched_at="later")
    assert empty.artifacts == {} and failed[0].code == "SST-PLN001"


def test_observe_options_skip_grants_and_capture_the_prior_definition() -> None:
    port = FakeSnowflake()
    artifact = rendered()
    port.show(ShowRow("V", "DB", "SCHEMA", "OWNER", "now"))
    port.grants[artifact.target.sql] = (GrantRow("SELECT", "ROLE", "R"),)
    port.definitions[artifact.target.sql] = "create or replace semantic view V copy grants"
    desired = {artifact.key: artifact}
    targets = (artifact.target,)

    skipped, quiet = observe(
        port, SEMANTIC_REGISTRY, targets, fetched_at="now", desired_artifacts=desired, options=ObserveOptions(False)
    )
    assert skipped.artifacts[artifact.key].grants is None
    assert skipped.artifacts[artifact.key].definition is None and quiet == ()

    capture = ObserveOptions(capture_prior=True)
    captured, quiet = observe(
        port, SEMANTIC_REGISTRY, targets, fetched_at="now", desired_artifacts=desired, options=capture
    )
    assert (
        captured.artifacts[artifact.key].definition == "create or replace semantic view V copy grants" and quiet == ()
    )
    assert captured.artifacts[artifact.key].grants == (GrantRow("SELECT", "ROLE", "R"),)

    # Only an object the plan renders is read; a prune candidate never is.
    unrelated, quiet = observe(port, SEMANTIC_REGISTRY, targets, fetched_at="now", options=capture)
    assert unrelated.artifacts[artifact.key].definition is None and quiet == ()

    port.definitions.clear()
    refused, failed = observe(
        port, SEMANTIC_REGISTRY, targets, fetched_at="now", desired_artifacts=desired, options=capture
    )
    assert refused.artifacts[artifact.key].definition is None
    assert {item.code for item in failed} == {"SST-PLN037"}
    assert "the prior definition of DB.SCHEMA.V" in failed[0].message


def test_plan_use_case_merges_observation_diagnostics() -> None:
    port = FakeSnowflake()
    artifact = rendered()
    port.fail("show_objects", SnowflakePortError("no privilege"))
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
    port = FakeSnowflake()
    artifact = rendered()
    port.show(ShowRow("V", "DB", "SCHEMA", "O", "now"))
    port.fail("show_grants", SnowflakePortError("denied"))
    observation, diagnostics = observe(
        port, SEMANTIC_REGISTRY, (artifact.target,), fetched_at="now", desired_artifacts={artifact.key: artifact}
    )
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
    port = FakeSnowflake()
    port.fail("show_objects", SnowflakePortError("composite artifacts have no generic object observation"))
    observation, diagnostics = observe(
        port,
        registry,
        (rendered().target,),
        fetched_at="now",
        artifact_types=frozenset(("virtual",)),
        observed_object_types={"virtual": frozenset(("",))},
    )
    assert observation.artifacts == {} and diagnostics == ()

    port = FakeSnowflake()
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
    port = FakeSnowflake()
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


def test_observe_agent_versions_selects_update_program_and_unsets_stranded_aliases() -> None:
    artifact = rendered()
    artifact = replace(
        artifact,
        key="agent:v",
        artifact_type="agent",
        object_type="AGENT",
        update_statements=(statement("ALTER AGENT ADD VERSION"),),
        update_live_statements=(statement("ALTER AGENT MODIFY LIVE VERSION"), statement("ALTER AGENT COMMIT")),
        desired_alias="candidate",
    )
    port = FakeSnowflake()
    port.show(ShowRow("V", "DB", "SCHEMA", "OWNER", "now", object_type="AGENT"))
    port.agent_version_rows[artifact.target.sql] = (
        AgentVersion(None, "CANDIDATE"),
        AgentVersion("VERSION$1", "PROMOTED", is_default=True),
    )
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
    assert observed.aliases == ("CANDIDATE", "PROMOTED")
    # The alias the agent file moved away from is unset; the one it declares is kept.
    assert texts(artifact.for_action(Action.UPDATE, observed).statements) == (
        "ALTER AGENT MODIFY LIVE VERSION",
        "ALTER AGENT COMMIT",
        'ALTER AGENT DB."SCHEMA".V MODIFY VERSION PROMOTED UNSET ALIAS',
    )


def test_plan_reports_composite_prune_when_generic_observation_has_no_change() -> None:
    artifact = rendered()
    prior = state()
    from snowflake_semantic_tools.domain.state import AppliedEntry

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
    result = PlanArtifacts(FakeSnowflake(), lifecycle_handlers={"virtual": handler}).run(
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

    def for_session(self, session: object) -> _CompositeHandler:
        del session
        return self

    # Plan reaches a handler only to report a prune, so this double answers nothing else.
    def plan(self, artifact: RenderedArtifact, state_entry: AppliedEntry | None, manifest: Manifest) -> CompositePlan:
        raise NotImplementedError

    def apply(self, change: Change, options: ApplyOptions) -> ApplyOutcome:
        raise NotImplementedError

    def merge_physical_resources(
        self, current: tuple[tuple[str, str], ...], previous: AppliedEntry | None
    ) -> tuple[AppliedResourceInput, ...]:
        raise NotImplementedError

    def report_prune(self, artifact_key: str, state_entry: AppliedEntry) -> Change:
        del state_entry
        artifact = replace(rendered("OLD"), key=artifact_key, artifact_type="virtual")
        return replace(change(artifact, Action.PRUNE), artifact_type="virtual")


def _tag_plan(
    declared: tuple[str, ...],
    options: ObserveOptions = DEFAULT_OBSERVE_OPTIONS,
    *,
    recorded: str = "f" * 64,
    port: FakeSnowflake | None = None,
) -> tuple[FakeSnowflake, RenderedArtifact, ChangeSet]:
    """Plan a view that shows the tag `GOV.T.PII`, declaring `declared`; an update unless `recorded` is current."""
    artifact = replace(rendered(), desired_tags=declared)
    planned_manifest = manifest({artifact.key: artifact})
    port = port or FakeSnowflake()
    port.show(
        ShowRow("V", "DB", "SCHEMA", "OWNER", "now", OwnershipMarker(planned_manifest.manifest_id, recorded).text)
    )
    port.tags[artifact.target.sql] = ("GOV.T.PII",)
    applied = AppliedEntry(
        recorded, artifact.target.sql, "then", "run", "applied", recorded, planned_manifest.manifest_id
    )
    planned = PlanArtifacts(port, observe_options=options).run(
        {artifact.key: artifact},
        planned_manifest,
        replace(state(), applied=MappingProxyType({artifact.key: applied})),
        target(),
        fetched_at="now",
    )
    expected = Action.NOOP if recorded == artifact.fingerprint else Action.UPDATE
    assert planned.changes[0].action is expected
    return port, artifact, planned


def test_plan_warns_of_each_tag_a_view_replace_removes() -> None:
    port, artifact, planned = _tag_plan(())
    assert port.tag_reads == [artifact.target.sql]
    [warning] = [item for item in planned.diagnostics if item.code == "SST-PLN039"]
    assert warning.message.endswith("removes tags the project does not declare: GOV.T.PII")
    properties = changed_properties(planned.changes[0], planned.manifest_id)
    assert ("tags", "GOV.T.PII", "") in [(item.name, item.before, item.after) for item in properties]


def test_plan_is_silent_on_declared_tags_and_reads_none_without_grants() -> None:
    _, _, declared = _tag_plan(("GOV.T.PII",))
    assert "SST-PLN039" not in {item.code for item in declared.diagnostics}
    assert "tags" not in [item.name for item in changed_properties(declared.changes[0], declared.manifest_id)]
    port, _, skipped = _tag_plan((), ObserveOptions(grants=False))
    assert port.tag_reads == []
    assert "SST-PLN039" not in {item.code for item in skipped.diagnostics}


def test_plan_reads_no_tags_on_a_view_it_leaves_unchanged_or_only_observes() -> None:
    port, _, unchanged = _tag_plan((), recorded=rendered().fingerprint)
    assert port.tag_reads == [] and "SST-PLN039" not in {item.code for item in unchanged.diagnostics}
    # Observation alone never reads tags, whatever the plan may replace.
    artifact = rendered()
    listed = FakeSnowflake()
    listed.show(ShowRow("V", "DB", "SCHEMA", "OWNER", "now"))
    observation, quiet = observe(
        listed, SEMANTIC_REGISTRY, (artifact.target,), fetched_at="now", desired_artifacts={artifact.key: artifact}
    )
    assert observation.artifacts[artifact.key].tags is None and quiet == () and listed.tag_reads == []


def test_a_failed_tag_read_warns_and_the_update_is_planned_all_the_same() -> None:
    refusing = FakeSnowflake()
    refusing.fail("tag_references", SnowflakePortError("denied"))
    _, _, planned = _tag_plan((), port=refusing)
    assert [(item.code, item.severity, item.message) for item in planned.diagnostics] == [
        (
            "SST-PLN040",
            Severity.WARNING,
            "the tags on DB.SCHEMA.V were not read, so the tags its replace removes are not checked: denied",
        )
    ]
    assert planned.changes[0].observed is not None and planned.changes[0].observed.tags is None
