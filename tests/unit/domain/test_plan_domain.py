"""Plan classification, prune safety, and dependency ordering."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    Change,
    ChangeReason,
    CompositeFacts,
    CompositeObservation,
    CompositePlan,
    GrantRow,
    ObservedArtifact,
    OwnershipMarker,
    PublishShape,
    RenderedArtifact,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan import build_changeset, dependency_waves, topological_order
from snowflake_semantic_tools.domain.state import STATE_SCHEMA_VERSION, AppliedEntry, ImpactIndex, Manifest, State
from tests.helpers.manifests import build_minimal_manifest
from tests.helpers.sql_values import statement


def target() -> TargetIdentity:
    return TargetIdentity("verify", "acct", Identifier.parse("db"), Identifier.parse("sch"))


def rendered(name: str, ddl: str | None = None, depends_on: tuple[str, ...] = ()) -> RenderedArtifact:
    return RenderedArtifact.create(
        key=f"semantic_view:{name.casefold()}",
        artifact_type="semantic_view",
        target=QualifiedName.from_parts("db", "sch", name),
        ddl=statement(ddl or f"create semantic view {name}"),
        depends_on=depends_on,
    )


def observed(
    artifact: RenderedArtifact,
    *,
    marker: OwnershipMarker | None = None,
    object_type: str = "SEMANTIC VIEW",
    raw_name: str | None = None,
    grants: tuple[GrantRow, ...] = (),
) -> ObservedArtifact:
    return ObservedArtifact(
        artifact.key,
        raw_name or artifact.target.name.folded,
        artifact.target,
        object_type,
        "OWNER",
        "now",
        marker.text if marker else None,
        marker,
        grants,
    )


def context(
    artifacts: dict[str, RenderedArtifact], applied: dict[str, AppliedEntry], manifest_id: str | None = None
) -> tuple[Manifest, State]:
    manifest = build_minimal_manifest(
        artifacts,
        project={},
        sources={},
        members={},
        files={},
        impact=ImpactIndex(),
        diagnostics_summary={},
    )
    state = State(
        STATE_SCHEMA_VERSION,
        target(),
        manifest_id if manifest_id is not None else manifest.manifest_id,
        "cfg",
        None,
        MappingProxyType(applied),
    )
    return manifest, state


def applied(artifact: RenderedArtifact, manifest_id: str) -> AppliedEntry:
    return AppliedEntry(
        artifact.fingerprint, artifact.target.sql, "now", "run", "applied", artifact.fingerprint, manifest_id
    )


def test_plan_classifies_create_update_noop_and_reports_grants_and_case() -> None:
    create = rendered("create")
    update = rendered("update")
    noop = rendered("noop")
    artifacts = {item.key: item for item in (create, update, noop)}
    manifest, _ = context(artifacts, {})
    state = State(
        STATE_SCHEMA_VERSION,
        target(),
        manifest.manifest_id,
        "cfg",
        None,
        MappingProxyType(
            {
                update.key: applied(rendered("update", "old"), manifest.manifest_id),
                noop.key: applied(noop, manifest.manifest_id),
            }
        ),
    )
    observation = SnowflakeObservation(
        MappingProxyType(
            {
                update.key: observed(
                    update,
                    marker=OwnershipMarker(
                        manifest.manifest_id,
                        rendered("update", "old").fingerprint,
                    ),
                    grants=(GrantRow("SELECT", "ROLE", "R"),),
                ),
                noop.key: observed(
                    noop,
                    raw_name="NoOp",
                    marker=OwnershipMarker(manifest.manifest_id, noop.fingerprint),
                ),
            }
        ),
        "then",
    )
    result = build_changeset(artifacts, observation, manifest, state, SEMANTIC_REGISTRY, target())
    assert [(item.key, item.action) for item in result.changes] == [
        (create.key, Action.CREATE),
        (noop.key, Action.NOOP),
        (update.key, Action.UPDATE),
    ]
    assert {diagnostic.code for diagnostic in result.diagnostics} == {"SST-PLN013", "SST-PLN023"}
    assert len(result.plan_id) == 64


def test_plan_conservatively_updates_without_trusted_state_and_blocks_errors() -> None:
    value = rendered("v")
    manifest, state = context({value.key: value}, {}, manifest_id="old")
    observation = SnowflakeObservation(MappingProxyType({value.key: observed(value)}), "now")
    mismatch = build_changeset({value.key: value}, observation, manifest, state, SEMANTIC_REGISTRY, target())
    assert mismatch.changes[0].reason is ChangeReason.UNMANAGED_OBJECT
    assert [item.code for item in mismatch.diagnostics] == ["SST-MAN021", "SST-PLN024"]

    old_manifest = "a" * 64
    old_entry = applied(value, old_manifest)
    fingerprint_match = State(
        STATE_SCHEMA_VERSION,
        target(),
        "old",
        "cfg",
        None,
        MappingProxyType({value.key: old_entry}),
    )
    marked_observation = SnowflakeObservation(
        MappingProxyType(
            {
                value.key: observed(
                    value,
                    marker=OwnershipMarker(old_manifest, value.fingerprint),
                )
            }
        ),
        "now",
    )
    stable = build_changeset(
        {value.key: value},
        marked_observation,
        manifest,
        fingerprint_match,
        SEMANTIC_REGISTRY,
        target(),
    )
    assert stable.changes[0].action is Action.UPDATE
    assert stable.changes[0].reason is ChangeReason.STATE_MANIFEST_MISMATCH

    current_entry = applied(value, manifest.manifest_id)
    mixed_state = State(
        STATE_SCHEMA_VERSION,
        target(),
        "older-manifest",
        "cfg",
        None,
        MappingProxyType({value.key: current_entry}),
    )
    current_observation = SnowflakeObservation(
        MappingProxyType(
            {
                value.key: observed(
                    value,
                    marker=OwnershipMarker(manifest.manifest_id, value.fingerprint),
                )
            }
        ),
        "now",
    )
    mixed = build_changeset(
        {value.key: value},
        current_observation,
        manifest,
        mixed_state,
        SEMANTIC_REGISTRY,
        target(),
    )
    assert mixed.changes[0].action is Action.NOOP

    unmarked = build_changeset(
        {value.key: value},
        observation,
        manifest,
        fingerprint_match,
        SEMANTIC_REGISTRY,
        target(),
    )
    assert unmarked.changes[0].reason is ChangeReason.VALIDATION_ERRORS
    assert unmarked.changes[0].action is Action.BLOCKED

    no_record = build_changeset(
        {value.key: value},
        observation,
        manifest,
        State.empty(target()),
        SEMANTIC_REGISTRY,
        target(),
    )
    assert no_record.changes[0].reason is ChangeReason.UNMANAGED_OBJECT
    assert no_record.changes[0].action is Action.BLOCKED
    assert no_record.diagnostics[-1].code == "SST-PLN024"

    trusted_state = State(
        STATE_SCHEMA_VERSION,
        target(),
        manifest.manifest_id,
        "cfg",
        None,
        MappingProxyType({value.key: applied(value, manifest.manifest_id)}),
    )
    out_of_band = build_changeset(
        {value.key: value},
        observation,
        manifest,
        trusted_state,
        SEMANTIC_REGISTRY,
        target(),
    )
    assert out_of_band.changes[0].action is Action.BLOCKED
    assert out_of_band.diagnostics[-1].code == "SST-PLN014"

    moved = rendered("v")
    moved = RenderedArtifact.create(
        key=moved.key,
        artifact_type=moved.artifact_type,
        target=QualifiedName.from_parts("db", "new_schema", "v"),
        ddl=statement(moved.ddl),
    )
    moved_manifest, _ = context({moved.key: moved}, {})
    prior = applied(value, moved_manifest.manifest_id)
    moved_state = State(
        STATE_SCHEMA_VERSION,
        target(),
        moved_manifest.manifest_id,
        "cfg",
        None,
        MappingProxyType({moved.key: prior}),
    )
    relocation = build_changeset(
        {moved.key: moved},
        SnowflakeObservation(
            MappingProxyType(
                {
                    moved.key: observed(
                        value,
                        marker=OwnershipMarker(moved_manifest.manifest_id, value.fingerprint),
                    )
                }
            ),
            "now",
        ),
        moved_manifest,
        moved_state,
        SEMANTIC_REGISTRY,
        target(),
    )
    assert relocation.changes[0].action is Action.BLOCKED
    assert relocation.changes[0].reason is ChangeReason.TARGET_MOVED
    assert relocation.diagnostics[-1].code == "SST-PLN025"

    blocked = build_changeset(
        {value.key: value},
        observation,
        manifest,
        state,
        SEMANTIC_REGISTRY,
        target(),
        blocked={value.key: DiagnosticBag((D("SST-REF001", model="x"),))},
    )
    assert blocked.changes[0].action is Action.BLOCKED
    wrong_type = build_changeset(
        {value.key: value},
        SnowflakeObservation(MappingProxyType({value.key: observed(value, object_type="VIEW")}), "now"),
        manifest,
        state,
        SEMANTIC_REGISTRY,
        target(),
    )
    assert wrong_type.changes[0].action is Action.BLOCKED
    assert wrong_type.diagnostics[0].code == "SST-MAN021"
    assert wrong_type.diagnostics[1].code == "SST-PLN002"


def test_prune_requires_marker_and_matching_authoritative_state() -> None:
    orphan = rendered("orphan")
    empty_manifest, empty_state = context({}, {})
    no_marker = build_changeset(
        {},
        SnowflakeObservation(MappingProxyType({orphan.key: observed(orphan)}), "now"),
        empty_manifest,
        empty_state,
        SEMANTIC_REGISTRY,
        target(),
        include_prune=True,
    )
    assert no_marker.changes == () and no_marker.diagnostics[0].code == "SST-PLN003"

    marker = OwnershipMarker("a" * 64, orphan.fingerprint)
    absent_state = build_changeset(
        {},
        SnowflakeObservation(MappingProxyType({orphan.key: observed(orphan, marker=marker)}), "now"),
        empty_manifest,
        empty_state,
        SEMANTIC_REGISTRY,
        target(),
        include_prune=True,
    )
    assert absent_state.diagnostics[0].code == "SST-PLN004"

    owned_state = State(
        STATE_SCHEMA_VERSION,
        target(),
        empty_manifest.manifest_id,
        "cfg",
        None,
        MappingProxyType({orphan.key: applied(orphan, marker.manifest_id)}),
    )
    prune = build_changeset(
        {},
        SnowflakeObservation(MappingProxyType({orphan.key: observed(orphan, marker=marker)}), "now"),
        empty_manifest,
        owned_state,
        SEMANTIC_REGISTRY,
        target(),
        include_prune=True,
    )
    assert prune.changes[0].action is Action.PRUNE


def test_dependencies_block_and_order_or_report_cycles() -> None:
    upstream = rendered("up")
    downstream = rendered("down", depends_on=(upstream.key,))
    manifest, state = context({upstream.key: upstream, downstream.key: downstream}, {})
    result = build_changeset(
        {upstream.key: upstream, downstream.key: downstream},
        SnowflakeObservation(fetched_at="now"),
        manifest,
        state,
        SEMANTIC_REGISTRY,
        target(),
        blocked={upstream.key: DiagnosticBag((D("SST-REF001", model="x"),))},
    )
    assert result.changes[0].action is Action.BLOCKED
    assert result.changes[1].reason is ChangeReason.DEPENDENCY_BLOCKED
    assert dependency_waves(result.changes) == ((result.changes[0],), (result.changes[1],))

    first = Change("a", "semantic_view", Action.CREATE, ChangeReason.NOT_PRESENT, upstream, None, ("b",), 100)
    second = Change("b", "semantic_view", Action.CREATE, ChangeReason.NOT_PRESENT, downstream, None, ("a",), 100)
    ordered, cycle = topological_order((first, second))
    assert ordered == () and cycle[0] == cycle[-1]
    try:
        dependency_waves((first, second))
    except ValueError as exc:
        assert str(exc) == "dependency cycle"
    else:
        raise AssertionError("cycle must fail")

    malformed = Change("x", "semantic_view", Action.CREATE, ChangeReason.NOT_PRESENT, upstream, None, ("missing",), 100)
    assert topological_order((malformed,))[0] == (malformed,)


def test_blocking_reaches_every_change_down_a_dependency_chain() -> None:
    top = rendered("top")
    middle = rendered("middle", depends_on=(top.key,))
    bottom = rendered("bottom", depends_on=(middle.key,))
    unrelated = rendered("unrelated")
    artifacts = {item.key: item for item in (top, middle, bottom, unrelated)}
    manifest, state = context(artifacts, {})

    result = build_changeset(
        artifacts,
        SnowflakeObservation(fetched_at="now"),
        manifest,
        state,
        SEMANTIC_REGISTRY,
        target(),
        blocked={top.key: DiagnosticBag((D("SST-REF001", model="x"),))},
    )

    by_key = {change.key: change for change in result.changes}
    assert by_key[top.key].action is Action.BLOCKED
    assert by_key[middle.key].reason is ChangeReason.DEPENDENCY_BLOCKED
    # Two links below the blocked view, so a single pass over direct dependents misses it.
    assert by_key[bottom.key].reason is ChangeReason.DEPENDENCY_BLOCKED
    assert by_key[unrelated.key].action is Action.CREATE


def test_plan_returns_cycle_diagnostic_and_skips_declared_or_unknown_prunes() -> None:
    first = rendered("a", depends_on=("semantic_view:b",))
    second = rendered("b", depends_on=("semantic_view:a",))
    manifest, state = context({first.key: first, second.key: second}, {})
    cycle = build_changeset(
        {first.key: first, second.key: second},
        SnowflakeObservation(fetched_at="now"),
        manifest,
        state,
        SEMANTIC_REGISTRY,
        target(),
    )
    assert cycle.changes == () and cycle.diagnostics[-1].code == "SST-PLN005"

    unknown = ObservedArtifact(
        "unknown:x",
        "X",
        QualifiedName.from_parts("db", "sch", "x"),
        "OTHER",
        "O",
        "now",
        None,
        None,
    )
    clean = build_changeset(
        {first.key: first},
        SnowflakeObservation(
            MappingProxyType({first.key: observed(first), "unknown:x": unknown}),
            "now",
        ),
        manifest,
        state,
        SEMANTIC_REGISTRY,
        target(),
        include_prune=True,
    )
    assert all(change.key != "unknown:x" for change in clean.changes)

    scoped_out = build_changeset(
        {},
        SnowflakeObservation(
            MappingProxyType({first.key: observed(first, marker=OwnershipMarker("a" * 64, first.fingerprint))}), "now"
        ),
        manifest,
        state,
        SEMANTIC_REGISTRY,
        target(),
        include_prune=True,
        prune_types=frozenset(),
    )
    assert scoped_out.changes == ()

    selected_out = build_changeset(
        {},
        SnowflakeObservation(
            MappingProxyType(
                {
                    first.key: observed(
                        first,
                        marker=OwnershipMarker("a" * 64, first.fingerprint),
                    )
                }
            ),
            "now",
        ),
        manifest,
        state,
        SEMANTIC_REGISTRY,
        target(),
        include_prune=True,
        prune_keys=frozenset(("semantic_view:other",)),
    )
    assert selected_out.changes == ()


def test_plan_uses_composite_lifecycle_action_observation_and_diagnostics() -> None:
    value = RenderedArtifact.create(
        key="eval:sales",
        artifact_type="eval",
        target=QualifiedName.from_parts("db", "sch", "sales_eval"),
        ddl=statement("payload"),
        shape=PublishShape(""),
        composite=CompositeFacts(),
    )
    manifest, state = context({value.key: value}, {})
    diagnostic = D("SST-PLN028", artifact=value.key, detail="stage mismatch")
    composite = CompositePlan(
        Action.BLOCKED,
        ChangeReason.VALIDATION_ERRORS,
        CompositeObservation(value.key),
        DiagnosticBag((diagnostic,)),
    )
    result = build_changeset(
        {value.key: value},
        SnowflakeObservation(fetched_at="now"),
        manifest,
        state,
        SEMANTIC_REGISTRY,
        target(),
        composite_plans={value.key: composite},
    )
    assert result.changes[0].action is Action.BLOCKED
    assert result.changes[0].composite_observation == composite.observation
    assert result.diagnostics == composite.diagnostics


def _versioned(key: str, artifact_type: str, depends_on: tuple[str, ...] = ()) -> RenderedArtifact:
    return RenderedArtifact.create(
        key=key,
        artifact_type=artifact_type,
        target=QualifiedName.from_parts("db", "sch", key.split(":", 1)[1].replace("-", "_")),
        ddl=statement(f"payload {key}"),
        shape=PublishShape("AGENT" if artifact_type == "agent" else ""),
        composite=None if artifact_type == "agent" else CompositeFacts(),
        depends_on=depends_on,
    )


def test_a_write_that_pins_a_version_outside_the_plan_is_blocked() -> None:
    agent = _versioned("agent:analyst", "agent", ("skill:guide", "plugin:kit", "semantic_view:sales"))
    evaluation = _versioned("eval:analyst", "eval", (agent.key,))
    skill = _versioned("skill:guide", "skill")
    manifest, state = context({agent.key: agent, evaluation.key: evaluation}, {})
    noop = CompositePlan(Action.NOOP, ChangeReason.UNCHANGED, CompositeObservation(skill.key))
    result = build_changeset(
        {agent.key: agent, evaluation.key: evaluation, skill.key: skill},
        SnowflakeObservation(fetched_at="now"),
        manifest,
        state,
        SEMANTIC_REGISTRY,
        target(),
        composite_plans={
            skill.key: noop,
            evaluation.key: CompositePlan(
                Action.CREATE, ChangeReason.NOT_PRESENT, CompositeObservation(evaluation.key)
            ),
        },
    )
    by_key = {change.key: change for change in result.changes}
    # The skill is planned (NOOP proves its version exists); the plugin is not,
    # and an unplanned semantic view is not a pinned version, so only the plugin blocks.
    assert by_key[agent.key].action is Action.BLOCKED
    assert by_key[agent.key].reason is ChangeReason.DEPENDENCY_BLOCKED
    assert [(item.code, item.message) for item in by_key[agent.key].diagnostics] == [
        (
            "SST-PLN030",
            "agent:analyst: pins the published version of plugin:kit; select plugin:kit as well",
        )
    ]
    assert [item.code for item in result.diagnostics] == ["SST-PLN030"]
    assert by_key[evaluation.key].reason is ChangeReason.DEPENDENCY_BLOCKED
    assert by_key[skill.key].action is Action.NOOP

    plugin = _versioned("plugin:kit", "plugin")
    planned = build_changeset(
        {agent.key: agent, skill.key: skill, plugin.key: plugin},
        SnowflakeObservation(fetched_at="now"),
        manifest,
        state,
        SEMANTIC_REGISTRY,
        target(),
        composite_plans={
            skill.key: noop,
            plugin.key: CompositePlan(Action.CREATE, ChangeReason.NOT_PRESENT, CompositeObservation(plugin.key)),
        },
    )
    assert {change.key: change.action for change in planned.changes}[agent.key] is Action.CREATE
    assert planned.diagnostics == DiagnosticBag()


def test_an_unchanged_agent_is_not_blocked_by_an_unplanned_pinned_version() -> None:
    agent = _versioned("agent:analyst", "agent", ("skill:guide",))
    manifest, _ = context({agent.key: agent}, {})
    state = State(
        STATE_SCHEMA_VERSION,
        target(),
        manifest.manifest_id,
        "cfg",
        None,
        MappingProxyType({agent.key: applied(agent, manifest.manifest_id)}),
    )
    observation = SnowflakeObservation(
        MappingProxyType(
            {
                agent.key: observed(
                    agent,
                    marker=OwnershipMarker(manifest.manifest_id, agent.fingerprint),
                    object_type="AGENT",
                )
            }
        ),
        "now",
    )
    result = build_changeset({agent.key: agent}, observation, manifest, state, SEMANTIC_REGISTRY, target())
    assert [(change.key, change.action) for change in result.changes] == [(agent.key, Action.NOOP)]
