"""Incremental planning: each artifact is decided by its own fingerprint, whatever else changed.

The manifest id moves on every project edit, and an apply narrowed by `--select` records only
what it selected, so the entries of the other artifacts keep the manifest that last published
them. None of that may turn an artifact whose rendered form is unchanged into an update.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ChangeReason,
    ChangeSet,
    OwnershipMarker,
    RenderedArtifact,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan import build_changeset
from snowflake_semantic_tools.domain.state import STATE_SCHEMA_VERSION, AppliedEntry, Manifest, State
from tests.helpers.manifests import build_minimal_manifest
from tests.helpers.plan_codes import live
from tests.helpers.sql_values import statement


def target() -> TargetIdentity:
    return TargetIdentity("verify", "acct", Identifier.parse("db"), Identifier.parse("sch"))


def rendered(name: str, body: str = "", depends_on: tuple[str, ...] = ()) -> RenderedArtifact:
    return RenderedArtifact.create(
        key=f"semantic_view:{name.casefold()}",
        artifact_type="semantic_view",
        target=QualifiedName.from_parts("db", "sch", name),
        ddl=statement(f"create semantic view {name}{body}"),
        depends_on=depends_on,
    )


def project(*artifacts: RenderedArtifact) -> dict[str, RenderedArtifact]:
    return {artifact.key: artifact for artifact in artifacts}


def manifest_of(artifacts: Mapping[str, RenderedArtifact]) -> Manifest:
    return build_minimal_manifest(
        dict(artifacts),
        project={},
        sources={},
        members={},
        files={},
        diagnostics_summary={},
    )


def entry(artifact: RenderedArtifact, manifest_id: str, fingerprint: str | None = None) -> AppliedEntry:
    recorded = fingerprint or artifact.fingerprint
    return AppliedEntry(recorded, artifact.target.sql, "now", "run", "applied", recorded, manifest_id)


def target_after(entries: Mapping[str, AppliedEntry], manifest_id: str) -> tuple[State, SnowflakeObservation]:
    """The state an apply leaves stamped with `manifest_id`, and each object carrying its entry's marker."""
    observed = {
        key: live(
            rendered(QualifiedName.parse(item.qualified_name).name.value),
            marker=OwnershipMarker(item.manifest_id, item.fingerprint),
        )
        for key, item in entries.items()
    }
    state = State(STATE_SCHEMA_VERSION, target(), manifest_id, "cfg", None, MappingProxyType(dict(entries)))
    return state, SnowflakeObservation(MappingProxyType(observed), "now")


def published(artifacts: Mapping[str, RenderedArtifact]) -> tuple[Manifest, State, SnowflakeObservation]:
    """A target where one apply published every artifact from their manifest."""
    manifest = manifest_of(artifacts)
    entries = {key: entry(artifact, manifest.manifest_id) for key, artifact in artifacts.items()}
    return (manifest, *target_after(entries, manifest.manifest_id))


def decisions(plan: ChangeSet) -> dict[str, tuple[Action, ChangeReason]]:
    return {change.key: (change.action, change.reason) for change in plan.changes}


UNCHANGED = (Action.NOOP, ChangeReason.UNCHANGED)
CHANGED = (Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS)


def test_an_edit_to_one_artifact_updates_only_that_artifact() -> None:
    menu, sales = rendered("menu"), rendered("sales")
    # The agent names the view; editing a metric of the view does not change what it renders.
    agent = rendered("agent", depends_on=(menu.key,))
    earlier, state, observation = published(project(menu, sales, agent))
    edited = project(rendered("menu", " with one more metric"), sales, agent)
    current = manifest_of(edited)
    assert current.manifest_id != earlier.manifest_id

    plan = build_changeset(edited, observation, current, state, SEMANTIC_REGISTRY, target())

    assert decisions(plan) == {menu.key: CHANGED, sales.key: UNCHANGED, agent.key: UNCHANGED}
    assert [item.code for item in plan.diagnostics] == []


def test_a_dependent_whose_render_changes_with_its_dependency_updates_after_it() -> None:
    skill, other = rendered("skill", " v1"), rendered("other")
    # The agent pins the skill's version, which the skill's content derives: its render moves.
    agent = rendered("agent", " pinning v1", depends_on=(skill.key,))
    _, state, observation = published(project(skill, other, agent))
    edited = project(rendered("skill", " v2"), other, rendered("agent", " pinning v2", depends_on=(skill.key,)))

    plan = build_changeset(edited, observation, manifest_of(edited), state, SEMANTIC_REGISTRY, target())

    assert decisions(plan) == {skill.key: CHANGED, agent.key: CHANGED, other.key: UNCHANGED}
    order = [change.key for change in plan.changes]
    assert order.index(skill.key) < order.index(agent.key)


def test_a_selected_apply_then_a_full_plan_is_unchanged_for_everything_it_left_out() -> None:
    menu, sales, agent = rendered("menu"), rendered("sales"), rendered("agent")
    earlier, _, _ = published(project(menu, sales, agent))
    edited_menu = rendered("menu", " with one more metric")
    edited = project(edited_menu, sales, agent)
    current = manifest_of(edited)
    # What `apply --select semantic_view:menu` leaves: menu recorded and marked under the
    # current manifest, the state stamped with it, and the others as the earlier apply left them.
    state, observation = target_after(
        {
            menu.key: entry(edited_menu, current.manifest_id),
            sales.key: entry(sales, earlier.manifest_id),
            agent.key: entry(agent, earlier.manifest_id),
        },
        current.manifest_id,
    )

    plan = build_changeset(edited, observation, current, state, SEMANTIC_REGISTRY, target())

    assert set(decisions(plan).values()) == {UNCHANGED}
    assert not plan.writes and [item.code for item in plan.diagnostics] == []


def test_a_fingerprint_scheme_change_updates_every_artifact_it_reaches() -> None:
    # A release that renders or canonicalises differently changes every fingerprint it
    # reaches, so the comparison itself republishes them: no separate whole-run switch.
    menu, sales = rendered("menu"), rendered("sales")
    old_scheme = {menu.key: "1" * 64, sales.key: "2" * 64}
    earlier = manifest_of(project(menu, sales))
    state, observation = target_after(
        {key: entry(artifact, earlier.manifest_id, old_scheme[key]) for key, artifact in project(menu, sales).items()},
        earlier.manifest_id,
    )

    plan = build_changeset(project(menu, sales), observation, earlier, state, SEMANTIC_REGISTRY, target())

    assert decisions(plan) == {menu.key: CHANGED, sales.key: CHANGED}


def test_an_unchanged_project_under_an_unchanged_manifest_plans_nothing() -> None:
    artifacts = project(rendered("menu"), rendered("sales"))
    manifest, state, observation = published(artifacts)

    plan = build_changeset(artifacts, observation, manifest, state, SEMANTIC_REGISTRY, target())

    assert set(decisions(plan).values()) == {UNCHANGED}
