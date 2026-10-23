"""An object type two kinds publish, AGENT, observed only in each kind's own schemas.

Agents and CoWork profiles (a Snowflake private preview) both publish an AGENT. Observation
keys each listed object by the kind it lists it for, so listing AGENT for both kinds in every
schema would see each agent once more as a profile, and each profile as an agent; a prune
would then report a project agent as an orphan profile. With each kind's own objects named,
AGENT is listed for a kind only in that kind's schemas.
"""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.observe import observe
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts, targets_by_kind
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    OwnershipMarker,
    PublishShape,
    RenderedArtifact,
    ShowRow,
    StatementPlan,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY, GrantPreservation
from tests.helpers.artifact_builders import applied_entry, empty_state
from tests.helpers.plan_codes import manifest_of, target
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement

AGENT = QualifiedName.parse("DB.A.X")
PROFILE = QualifiedName.parse("DB.B.Y")
KIND_TARGETS = MappingProxyType({"agent": (AGENT,), "cowork_profile": (PROFILE,)})


def _catalog(*marked: RenderedArtifact) -> FakeSnowflake:
    """Agent `x` in schema A, profile `y` and an unmarked third-party agent in schema B.

    Each of `marked` carries the marker apply wrote for it, by its target's name.
    """
    port = FakeSnowflake()
    markers = {item.target.name.value: OwnershipMarker("a" * 64, item.fingerprint).text for item in marked}
    port.show(
        ShowRow("X", "DB", "A", "OWNER", "now", markers.get("X"), object_type="AGENT"),
        ShowRow("Y", "DB", "B", "OWNER", "now", markers.get("Y"), object_type="AGENT"),
        ShowRow("THIRD_PARTY", "DB", "B", "OWNER", "now", object_type="AGENT"),
    )
    return port


def _observed_keys(kind_targets: MappingProxyType[str, tuple[QualifiedName, ...]] | None) -> list[str]:
    observation, diagnostics = observe(
        _catalog(), SEMANTIC_REGISTRY, (AGENT, PROFILE), fetched_at="now", kind_targets=kind_targets
    )
    assert list(diagnostics) == []
    return sorted(observation.artifacts)


def test_each_agent_object_is_keyed_once_by_the_kind_whose_schema_holds_it() -> None:
    keys = _observed_keys(KIND_TARGETS)
    assert keys == ["agent:x", "cowork_profile:third_party", "cowork_profile:y"]
    assert "cowork_profile:x" not in keys and "agent:y" not in keys and "agent:third_party" not in keys


def test_without_kind_targets_each_agent_object_is_listed_under_both_kinds() -> None:
    assert _observed_keys(None) == [
        "agent:third_party",
        "agent:x",
        "agent:y",
        "cowork_profile:third_party",
        "cowork_profile:x",
        "cowork_profile:y",
    ]


def _agent_object(key: str, artifact_type: str, name: QualifiedName) -> RenderedArtifact:
    return RenderedArtifact.create(
        key=key,
        artifact_type=artifact_type,
        target=name,
        ddl='{"models": {"orchestration": "auto"}}',
        shape=PublishShape("AGENT", render_dialect="json", grant_preservation=GrantPreservation.NONE),
        statements=StatementPlan(default=(statement(f"CREATE AGENT {name.sql}"),)),
    )


def test_targets_by_kind_takes_each_rendered_target_then_each_state_records() -> None:
    rendered = (_agent_object("agent:x", "agent", AGENT), _agent_object("cowork_profile:y", "cowork_profile", PROFILE))
    state = empty_state(
        {
            # The agent again, and a profile whose source was deleted: still a profile's object.
            "agent:x": applied_entry(AGENT.sql, "1" * 64),
            "cowork_profile:gone": applied_entry("DB.B.GONE", "2" * 64),
            # A key naming no kind, and an entry naming no object, are left out.
            "uncategorized": applied_entry("DB.C.Z", "3" * 64),
            "agent:unnamed": applied_entry("", "4" * 64),
        }
    )
    assert dict(targets_by_kind(rendered, state)) == {
        "agent": (AGENT,),
        "cowork_profile": (PROFILE, QualifiedName.parse("DB.B.GONE")),
    }


def test_a_prune_plan_never_reports_a_project_agent_as_an_orphan_cowork_profile() -> None:
    agent = _agent_object("agent:x", "agent", AGENT)
    profile = _agent_object("cowork_profile:y", "cowork_profile", PROFILE)
    # Both were applied as they render now, so the only prune note is the third party's.
    state = empty_state(
        {
            agent.key: applied_entry(AGENT.sql, agent.fingerprint),
            profile.key: applied_entry(PROFILE.sql, profile.fingerprint),
        }
    )
    changeset = PlanArtifacts(_catalog(agent, profile)).run(
        {agent.key: agent, profile.key: profile},
        manifest_of(agent, profile),
        state,
        target(),
        fetched_at="now",
        include_prune=True,
    )
    assert {change.key: change.action for change in changeset.changes} == {
        "agent:x": Action.NOOP,
        "cowork_profile:y": Action.NOOP,
    }
    # Listed under both kinds, agent x would be a marked orphan profile (SST-PLN004), and
    # profile y and the third party unmarked agents (SST-PLN003).
    prune_notes = [item.message for item in changeset.diagnostics if item.code in ("SST-PLN003", "SST-PLN004")]
    assert prune_notes == ["DB.B.THIRD_PARTY has no SST ownership marker; skipped"]
