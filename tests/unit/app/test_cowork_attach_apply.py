"""Applying a plan that attaches a CoWork profile (a Snowflake private preview), then planning again.

The fake attaches on ALTER SNOWFLAKE INTELLIGENCE ... ADD AGENT, refusing a second ADD as Snowflake
does, so each cycle below ends with a plan that has nothing left to attach.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ChangeReason,
    ChangeSet,
    ExecResult,
    OwnershipMarker,
    RenderedArtifact,
    ShowRow,
)
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state import Manifest, State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import rendered, target
from tests.helpers.clocks import FixedClock
from tests.helpers.cowork_attach import COWORK, profile
from tests.helpers.plan_codes import manifest_of
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement, texts

ADD = f"ALTER SNOWFLAKE INTELLIGENCE {COWORK} ADD AGENT SNOWFLAKE_INTELLIGENCE.PROFILES.EVERYONE"


class _Publishing(FakeSnowflake):
    """The fake account, on which a profile is listed with the marker apply last wrote.

    CREATE AGENT and the comment an update leads with both leave the marker of the manifest
    being applied, `manifest_id`, as Snowflake keeps the comment SST writes.
    """

    def __init__(self, artifact: RenderedArtifact, manifest_id: str) -> None:
        super().__init__()
        self._artifact = artifact
        self.manifest_id = manifest_id
        self.intelligences = [COWORK]

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        result = super().execute_script(statements)
        if result.ok and texts(statements)[0] in ("create agent", "set comment"):
            name = self._artifact.target
            marker = OwnershipMarker(self.manifest_id, self._artifact.fingerprint)
            listed = ShowRow(name.name.folded, "SNOWFLAKE_INTELLIGENCE", "PROFILES", "TEST_ROLE", "now")
            self.show(replace(listed, comment=marker.text, object_type="AGENT"))
            self.object_types[name.sql] = "AGENT"
            self.markers[name.sql] = marker
        return result


def _cycle() -> tuple[RenderedArtifact, _Publishing, InMemoryStateStore]:
    artifact = profile()
    manifest = manifest_of(artifact)
    port = _Publishing(artifact, manifest.manifest_id)
    store = InMemoryStateStore(State.empty(target()))
    return artifact, port, store


def _state(store: InMemoryStateStore) -> State:
    assert store.state is not None
    return store.state


def _plan(
    artifact: RenderedArtifact, port: FakeSnowflake, store: InMemoryStateStore, manifest: Manifest | None = None
) -> ChangeSet:
    compiled = manifest or manifest_of(artifact)
    return PlanArtifacts(port).run({artifact.key: artifact}, compiled, _state(store), target(), fetched_at="now")


def _apply(changeset: ChangeSet, port: FakeSnowflake, store: InMemoryStateStore) -> None:
    use_case = ApplyArtifacts(port, store, FixedClock(), state_table=rendered("STATE").target, actor="TEST_ROLE")
    assert use_case.run(changeset, _state(store)).success


def test_a_created_profile_ends_with_its_attachment_and_the_next_plan_attaches_nothing() -> None:
    artifact, port, store = _cycle()
    created = _plan(artifact, port, store)
    [change] = created.changes
    assert change.action is Action.CREATE and change.rendered is not None
    assert texts(change.rendered.statements) == ("create agent", "commit version", ADD)
    _apply(created, port, store)
    assert port.scripts[-1][-1] == ADD
    assert port.intelligence_agents[COWORK] == [artifact.target.sql]
    [again] = _plan(artifact, port, store).changes
    assert (again.action, again.reason) == (Action.NOOP, ChangeReason.UNCHANGED)


def test_a_profile_detached_by_hand_is_attached_again_and_then_left_alone() -> None:
    artifact, port, store = _cycle()
    _apply(_plan(artifact, port, store), port, store)
    port.intelligence_agents[COWORK].clear()
    # Planned from a later compile, as in an account: the manifest id differs from the one the
    # profile's marker carries, so apply reads back the marker this plan writes.
    recompiled = replace(manifest_of(artifact), manifest_id="0" * 64)
    port.manifest_id = recompiled.manifest_id
    detached = _plan(artifact, port, store, recompiled)
    [change] = detached.changes
    assert (change.action, change.reason) == (Action.UPDATE, ChangeReason.DETACHED)
    assert [item.code for item in detached.diagnostics] == ["SST-PLN104"]
    uploads = len(port.uploads)
    _apply(detached, port, store)
    # Only the marked comment and the attachment run: nothing is staged again, no version is
    # committed.
    assert port.scripts[-1] == ("set comment", ADD) and len(port.uploads) == uploads
    marker = port.markers[artifact.target.sql]
    assert marker is not None and marker.manifest_id == recompiled.manifest_id
    [again] = _plan(artifact, port, store, recompiled).changes
    assert again.action is Action.NOOP


def test_dropping_a_profile_detaches_it_so_a_prune_needs_no_statement_of_its_own() -> None:
    artifact, port, store = _cycle()
    _apply(_plan(artifact, port, store), port, store)
    assert port.execute_script((statement(f"DROP AGENT {artifact.target.sql}"),)).ok
    assert port.intelligence_agents[COWORK] == []
