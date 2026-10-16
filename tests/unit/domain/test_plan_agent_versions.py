"""Plan an agent by the versions it lists: which program an update runs, and when its versions drifted.

An agent whose newest committed version already holds the planned specification is updated
without adding a version; one whose alias or default version moved off that version, or that
has no live version, is planned as an update for version drift (SST-PLN102).
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ChangeReason,
    DesiredMetadata,
    ObservedArtifact,
    OwnershipMarker,
    PublishShape,
    RenderedArtifact,
    StatementPlan,
    commented_spec_fingerprint,
    held_spec_fingerprint,
    spec_fingerprint,
    version_comment,
    version_drift,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.state import PARTIAL_WRITE
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, manifest_of, plan
from tests.helpers.sql_values import statement, texts

SPEC = '{\n  "models": {\n    "orchestration": "auto"\n  }\n}\n'
SPEC_FINGERPRINT = spec_fingerprint(SPEC)
RECORDED_MANIFEST = "a" * 64


def agent(*, alias: str | None = "promoted", fingerprint: str = "1" * 64, temporary: bool = False) -> RenderedArtifact:
    """An agent whose programs are named for what they do; its properties program leads with its comment."""
    artifact = RenderedArtifact.create(
        key="agent:sales",
        artifact_type="agent",
        target=QualifiedName.parse("DB.S.SALES"),
        ddl=SPEC,
        shape=PublishShape(
            "AGENT", render_dialect="json", grant_preservation=GrantPreservation.NONE, temporary=temporary
        ),
        statements=StatementPlan(
            default=(statement("create"),),
            create=(statement("create"),),
            update=(statement("set comment"), statement("add version")),
            update_live=(statement("set comment"), statement("modify live"), statement("commit")),
            update_properties=(statement("set comment"), statement("set tag")),
            properties_lead=1,
        ),
        metadata=DesiredMetadata(alias=alias, spec_fingerprint=SPEC_FINGERPRINT),
    )
    return replace(artifact, fingerprint=fingerprint)


def live(
    artifact: RenderedArtifact,
    marker: OwnershipMarker,
    *,
    newest_spec: str | None = SPEC_FINGERPRINT,
    has_live_version: bool = True,
    newest_version: str | None = "VERSION$3",
    default_version: str | None = None,
    alias_versions: tuple[tuple[str, str], ...] = (("PROMOTED", "VERSION$3"),),
) -> ObservedArtifact:
    return ObservedArtifact(
        artifact.key,
        "SALES",
        artifact.target,
        "AGENT",
        "OWNER",
        "now",
        marker.text,
        marker,
        has_live_version=has_live_version,
        newest_version=newest_version,
        newest_spec=newest_spec,
        default_version=default_version,
        alias_versions=alias_versions,
    )


def recorded(artifact: RenderedArtifact) -> OwnershipMarker:
    return OwnershipMarker(RECORDED_MANIFEST, artifact.fingerprint)


def test_the_specification_fingerprint_reads_the_json_not_its_layout() -> None:
    assert spec_fingerprint('{"models":{"orchestration":"auto"}}') == SPEC_FINGERPRINT
    assert spec_fingerprint("models:\n  orchestration: auto  \n") == spec_fingerprint("models:\n  orchestration: auto")
    assert spec_fingerprint("not json") != SPEC_FINGERPRINT


def test_a_committed_version_carries_its_commit_and_specification_fingerprint() -> None:
    comment = version_comment("abc1234", SPEC)
    assert comment == f"git:abc1234 sst-spec:{SPEC_FINGERPRINT}"
    assert commented_spec_fingerprint(comment) == SPEC_FINGERPRINT
    assert commented_spec_fingerprint(comment.upper().replace("SST-SPEC:", "sst-spec:")) == SPEC_FINGERPRINT
    assert commented_spec_fingerprint("git:abc1234") is None
    assert commented_spec_fingerprint(None) is None
    # A token glued to other text, or of the wrong length, is not one SST wrote.
    assert commented_spec_fingerprint(f"xsst-spec:{SPEC_FINGERPRINT}") is None
    assert commented_spec_fingerprint("sst-spec:abc") is None


def test_a_version_holds_the_fingerprint_its_comment_names_else_its_specifications() -> None:
    other = "f" * 64
    assert held_spec_fingerprint(f"git:x sst-spec:{other}", SPEC) == other
    assert held_spec_fingerprint("git:x", SPEC) == SPEC_FINGERPRINT
    assert held_spec_fingerprint(None, None) is None


def test_a_property_only_change_sets_properties_and_adds_no_version() -> None:
    before = agent()
    after = agent(fingerprint="2" * 64)
    [change] = plan(
        (after,),
        observed=(live(before, recorded(before)),),
        applied={after.key: entry(before, RECORDED_MANIFEST)},
    ).changes
    assert (change.action, change.reason) == (Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS)
    assert change.rendered is not None and texts(change.rendered.statements) == ("set comment", "set tag")


def test_an_agent_whose_newest_version_holds_another_specification_gets_a_version() -> None:
    before = agent()
    after = agent(fingerprint="2" * 64)
    applied = {after.key: entry(before, RECORDED_MANIFEST)}
    for newest_spec in ("f" * 64, None):
        [change] = plan(
            (after,), observed=(live(before, recorded(before), newest_spec=newest_spec),), applied=applied
        ).changes
        assert change.rendered is not None
        assert texts(change.rendered.statements) == ("set comment", "modify live", "commit")


def test_recovery_from_a_killed_publish_records_the_committed_version_without_another() -> None:
    # Killed after COMMIT: the newest version holds the planned specification. Whether the
    # marker was the recorded one or already the planned one, recovery commits nothing.
    before = agent()
    after = agent(fingerprint="2" * 64)
    planned = manifest_of(after)
    applied = {after.key: entry(before, RECORDED_MANIFEST)}
    for marker, reason in (
        (recorded(before), ChangeReason.FINGERPRINT_DIFFERS),
        (OwnershipMarker(planned.manifest_id, after.fingerprint), ChangeReason.UNRECORDED_PUBLISH),
    ):
        [change] = plan((after,), observed=(live(before, marker),), applied=applied, manifest=planned).changes
        assert (change.action, change.reason) == (Action.UPDATE, reason)
        assert change.rendered is not None
        assert not any(text in ("add version", "modify live", "commit") for text in texts(change.rendered.statements))


def test_a_recorded_partial_write_under_its_own_marker_is_finished_not_refused() -> None:
    # The marker is written first, so it names the definition state records the stopped
    # write for; the next plan finishes that write instead of calling it changed out of band.
    artifact = agent()
    partial = replace(entry(artifact, RECORDED_MANIFEST), outcome=PARTIAL_WRITE)
    planned = plan((artifact,), observed=(live(artifact, recorded(artifact)),), applied={artifact.key: partial})
    [change] = planned.changes
    assert (change.action, change.reason) == (Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS)
    assert coded(planned.diagnostics, "SST-PLN014") == []
    assert change.rendered is not None and texts(change.rendered.statements) == ("set comment", "set tag")


def drifted(
    observed: ObservedArtifact, artifact: RenderedArtifact | None = None
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Plan the unchanged agent against `observed`: its update's statements and what PLN102 reports."""
    artifact = artifact or agent()
    planned = plan((artifact,), observed=(observed,), applied={artifact.key: entry(artifact, RECORDED_MANIFEST)})
    [change] = planned.changes
    assert (change.action, change.reason) == (Action.UPDATE, ChangeReason.VERSION_DRIFT)
    assert change.rendered is not None
    return texts(change.rendered.statements), tuple(item.message for item in coded(planned.diagnostics, "SST-PLN102"))


def test_an_alias_moved_off_the_published_version_is_moved_back_without_a_version() -> None:
    artifact = agent()
    statements, messages = drifted(live(artifact, recorded(artifact), alias_versions=(("PROMOTED", "VERSION$2"),)))
    assert statements == (
        "set comment",
        "ALTER AGENT DB.S.SALES\n  MODIFY VERSION VERSION$3 SET ALIAS = PROMOTED",
        "set tag",
    )
    assert messages == ("agent:sales: alias PROMOTED is on VERSION$2, not VERSION$3; apply points it back",)


def test_an_alias_on_no_version_and_a_moved_default_are_both_pointed_back() -> None:
    artifact = agent()
    observed = live(artifact, recorded(artifact), alias_versions=(), default_version="VERSION$2")
    statements, messages = drifted(observed)
    assert statements == (
        "set comment",
        "ALTER AGENT DB.S.SALES\n  MODIFY VERSION VERSION$3 SET ALIAS = PROMOTED",
        "ALTER AGENT DB.S.SALES SET DEFAULT_VERSION = 'VERSION$3'",
        "set tag",
    )
    assert messages == (
        "agent:sales: alias PROMOTED is on no version, not VERSION$3; the default version is VERSION$2, not "
        "VERSION$3; apply points it back",
    )


def test_an_agent_without_a_live_version_opens_one_from_the_published_version() -> None:
    artifact = agent(alias=None)
    statements, messages = drifted(
        live(artifact, recorded(artifact), has_live_version=False, alias_versions=()), artifact
    )
    assert statements == ("set comment", "ALTER AGENT DB.S.SALES ADD LIVE VERSION FROM VERSION$3", "set tag")
    assert messages == ("agent:sales: it has no live version; apply points it back",)


def test_an_agent_without_a_live_version_whose_newest_holds_another_spec_gets_a_version() -> None:
    artifact = agent()
    observed = live(artifact, recorded(artifact), has_live_version=False, newest_spec="f" * 64)
    statements, messages = drifted(observed)
    assert statements == ("set comment", "add version")
    assert messages == ("agent:sales: it has no live version; apply points it back",)


def test_versions_that_serve_what_was_published_or_cannot_be_read_are_not_drift() -> None:
    artifact = agent()
    marker = recorded(artifact)
    applied = {artifact.key: entry(artifact, RECORDED_MANIFEST)}
    for observed in (
        live(artifact, marker, default_version="VERSION$3"),
        live(artifact, marker, newest_version=None, has_live_version=False),
        live(artifact, marker, newest_spec="f" * 64, alias_versions=(("PROMOTED", "VERSION$1"),)),
    ):
        [change] = plan((artifact,), observed=(observed,), applied=applied).changes
        assert (change.action, change.reason) == (Action.NOOP, ChangeReason.UNCHANGED)
    temporary = agent(temporary=True)
    assert version_drift(temporary, live(temporary, marker, has_live_version=False)) == ()


def test_an_alias_no_identifier_spells_and_a_version_no_identifier_spells_are_not_pointed_at() -> None:
    artifact = agent(alias="not an alias")
    marker = recorded(artifact)
    update = artifact.for_action(Action.UPDATE, live(artifact, marker, alias_versions=()))
    assert texts(update.statements) == ("set comment", "set tag")
    unnamed = artifact.for_action(Action.UPDATE, live(artifact, marker, newest_version="", has_live_version=False))
    assert texts(unnamed.statements) == ("set comment", "set tag")
