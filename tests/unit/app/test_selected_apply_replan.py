"""A selected apply records only what it selected, and a full plan then finds the rest unchanged."""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    OwnershipMarker,
    RenderedArtifact,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
from snowflake_semantic_tools.domain.plan import build_changeset
from snowflake_semantic_tools.domain.state import STATE_SCHEMA_VERSION, AppliedEntry, State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.apply_runs import apply_plan
from tests.helpers.artifact_builders import manifest, observed, rendered, target
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement


def with_one_more_metric(artifact: RenderedArtifact) -> RenderedArtifact:
    name = artifact.target.name.value
    return RenderedArtifact.create(
        key=artifact.key,
        artifact_type=artifact.artifact_type,
        target=artifact.target,
        ddl=statement(f"CREATE OR REPLACE SEMANTIC VIEW DB.SCHEMA.{name} TABLES (T AS DB.SCHEMA.T) COPY GRANTS -- m"),
        required_relations=artifact.required_relations,
    )


def entry(artifact: RenderedArtifact, manifest_id: str) -> AppliedEntry:
    return AppliedEntry(
        artifact.fingerprint, artifact.target.sql, "now", "run", "applied", artifact.fingerprint, manifest_id
    )


def observation(port: FakeSnowflake, artifacts: Mapping[str, RenderedArtifact]) -> SnowflakeObservation:
    """What plan observes: each artifact's object, carrying the marker the port holds for it."""
    return SnowflakeObservation(
        {key: observed(artifact, ownership=port.markers[artifact.target.sql]) for key, artifact in artifacts.items()},
        "now",
    )


def test_a_selected_apply_then_a_full_plan_is_unchanged_for_what_the_selection_left_out() -> None:
    menu, sales = rendered("MENU"), rendered("SALES")
    earlier = manifest({menu.key: menu, sales.key: sales})
    prior = State(
        STATE_SCHEMA_VERSION,
        target(),
        earlier.manifest_id,
        "cfg",
        None,
        {menu.key: entry(menu, earlier.manifest_id), sales.key: entry(sales, earlier.manifest_id)},
    )
    port = FakeSnowflake()
    port.existing = {menu.target.sql, sales.target.sql, QualifiedName.parse("DB.SCHEMA.T").sql}
    port.markers = {item.target.sql: OwnershipMarker(earlier.manifest_id, item.fingerprint) for item in (menu, sales)}
    port.remote_state = prior.applied
    edited = with_one_more_metric(menu)
    project = {edited.key: edited, sales.key: sales}
    current = manifest(project)

    selected = build_changeset(
        {edited.key: edited}, observation(port, project), current, prior, ARTIFACT_REGISTRY, target(), full=False
    )
    assert [(change.key, change.action) for change in selected.changes] == [(menu.key, Action.UPDATE)]
    store = InMemoryStateStore(prior)
    result = apply_plan(selected, port, previous=prior, store=store)
    assert result.success and result.state_written and store.state is not None
    # The write left the marker the publish carried, as Snowflake keeps it in the comment.
    port.markers[menu.target.sql] = OwnershipMarker(current.manifest_id, edited.fingerprint)
    port.remote_state = store.state.applied

    after = store.state
    assert after.applied[menu.key].manifest_id == current.manifest_id
    assert after.applied[sales.key] == prior.applied[sales.key]
    full = build_changeset(project, observation(port, project), current, after, ARTIFACT_REGISTRY, target())

    assert [(change.key, change.action) for change in full.changes] == [
        (menu.key, Action.NOOP),
        (sales.key, Action.NOOP),
    ]
    assert not full.writes and [item.code for item in full.diagnostics] == []
