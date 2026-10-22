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
    drift_properties,
    held_spec_fingerprint,
    live_digest,
    pinned_default,
    spec_fingerprint,
    unpinned_default,
    version_comment,
    version_drift,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.state import PARTIAL_WRITE, AppliedEntry
from tests.helpers.diagnostic_filters import coded
from tests.helpers.plan_codes import entry, manifest_of, plan
from tests.helpers.sql_values import statement, texts

SPEC = '{\n  "models": {\n    "orchestration": "auto"\n  }\n}\n'
SPEC_FINGERPRINT = spec_fingerprint(SPEC)
RECORDED_MANIFEST = "a" * 64
LAST = "ALTER AGENT DB.S.SALES SET DEFAULT_VERSION = 'LAST'"
PINNED = "ALTER AGENT DB.S.SALES SET DEFAULT_VERSION = 'VERSION$3'"


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


def test_a_version_adding_update_leaves_the_default_following_the_version_it_adds() -> None:
    # SHOW VERSIONS marks which version is the default but not whether it was pinned by name, so
    # a default pinned to the newest version would stay behind the one the update commits.
    before = agent()
    after = agent(fingerprint="2" * 64)
    applied = {after.key: entry(before, RECORDED_MANIFEST)}
    for has_live_version, program in ((True, ("modify live", "commit")), (False, ("add version",))):
        for default in ("VERSION$2", "VERSION$3", None):
            observed = live(
                before,
                recorded(before),
                newest_spec="f" * 64,
                has_live_version=has_live_version,
                default_version=default,
            )
            [change] = plan((after,), observed=(observed,), applied=applied).changes
            assert change.rendered is not None
            assert texts(change.rendered.statements) == ("set comment", *program, LAST)
    temporary = agent(temporary=True)
    pinned = live(temporary, recorded(temporary), newest_spec="f" * 64, default_version="VERSION$2")
    assert LAST not in texts(temporary.for_action(Action.UPDATE, pinned).statements)


def test_an_agent_whose_newest_version_holds_another_specification_gets_a_version() -> None:
    before = agent()
    after = agent(fingerprint="2" * 64)
    applied = {after.key: entry(before, RECORDED_MANIFEST)}
    for newest_spec in ("f" * 64, None):
        [change] = plan(
            (after,), observed=(live(before, recorded(before), newest_spec=newest_spec),), applied=applied
        ).changes
        assert change.rendered is not None
        assert texts(change.rendered.statements) == ("set comment", "modify live", "commit", LAST)


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
        PINNED,
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
    assert statements == ("set comment", "ALTER AGENT DB.S.SALES ADD LIVE VERSION FROM LAST", "set tag")
    assert messages == ("agent:sales: it has no live version; apply points it back",)


def test_an_agent_without_a_live_version_whose_newest_holds_another_spec_gets_a_version() -> None:
    artifact = agent()
    observed = live(artifact, recorded(artifact), has_live_version=False, newest_spec="f" * 64)
    statements, messages = drifted(observed)
    assert statements == ("set comment", "add version", LAST)
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


def test_a_renamed_alias_replaces_the_old_one_on_its_version_without_unsetting_it() -> None:
    # A version carries one alias, so setting the new one on the version that held the old one
    # removes the old one: an UNSET naming it then fails, as no version carries it any more.
    artifact = agent()
    marker = recorded(artifact)
    same = replace(live(artifact, marker, alias_versions=(("RELEASED", "VERSION$3"),)), aliases=("RELEASED",))
    statements, _ = drifted(same)
    assert statements == (
        "set comment",
        "ALTER AGENT DB.S.SALES\n  MODIFY VERSION VERSION$3 SET ALIAS = PROMOTED",
        "set tag",
    )
    # An old alias another version carries is still there to unset.
    other = replace(
        live(artifact, marker, alias_versions=(("RELEASED", "VERSION$2"),)),
        aliases=("RELEASED",),
    )
    statements, _ = drifted(other)
    assert statements == (
        "set comment",
        "ALTER AGENT DB.S.SALES\n  MODIFY VERSION VERSION$3 SET ALIAS = PROMOTED",
        "set tag",
        "ALTER AGENT DB.S.SALES MODIFY VERSION RELEASED UNSET ALIAS",
    )


def test_an_agent_update_says_whether_it_commits_a_new_version() -> None:
    before = agent()
    marker = recorded(before)
    properties_only = before.for_action(Action.UPDATE, live(before, marker))
    assert properties_only.adds_version is False
    for has_live_version in (True, False):
        observed = live(before, marker, newest_spec="f" * 64, has_live_version=has_live_version)
        assert before.for_action(Action.UPDATE, observed).adds_version is True
    temporary = agent(temporary=True)
    assert temporary.for_action(Action.UPDATE, live(temporary, marker, newest_spec="f" * 64)).adds_version is None
    assert before.for_action(Action.CREATE, None).adds_version is None


def _hand_committed(**observed: object) -> tuple[RenderedArtifact, ObservedArtifact, AppliedEntry]:
    """The unchanged agent, its newest version committed by hand with another specification."""
    artifact = agent()
    hand = "f" * 64
    applied = replace(
        entry(artifact, RECORDED_MANIFEST),
        live_digest=live_digest(drift_properties("AGENT", {}, newest_spec=SPEC_FINGERPRINT)),
    )
    found = replace(
        live(artifact, recorded(artifact), newest_spec=hand, has_live_version=False, **observed),  # type: ignore[arg-type]
        live_digest=live_digest(drift_properties("AGENT", {}, newest_spec=hand)),
    )
    return artifact, found, applied


def test_a_hand_committed_specification_names_its_version_and_what_serves_it() -> None:
    for observed, said in (
        ({"default_version": "VERSION$3"}, "VERSION$3, committed outside SST, is the default and holds alias PROMOTED"),
        ({"alias_versions": ()}, "VERSION$3, committed outside SST, is the newest version"),
    ):
        artifact, found, applied = _hand_committed(**observed)
        planned = plan((artifact,), observed=(found,), applied={artifact.key: applied})
        [diagnostic] = coded(planned.diagnostics, "SST-PLN014")
        assert diagnostic.message == (
            f"agent:sales was changed out of band (its specification changed; its marker did not; {said}); "
            "apply leaves it as it is"
        )
    # With no committed version listed there is none to name.
    artifact, found, applied = _hand_committed(newest_version=None)
    [diagnostic] = coded(
        plan((artifact,), observed=(found,), applied={artifact.key: applied}).diagnostics, "SST-PLN014"
    )
    assert "committed outside SST" not in diagnostic.message


def test_overwrite_adds_a_version_over_a_hand_committed_one_and_follows_it() -> None:
    artifact, found, applied = _hand_committed()
    planned = plan((artifact,), observed=(found,), applied={artifact.key: applied}, overwrite=True)
    [change] = planned.changes
    assert (change.action, change.reason) == (Action.UPDATE, ChangeReason.OVERWRITE)
    assert change.rendered is not None and change.rendered.adds_version is True
    assert texts(change.rendered.statements) == ("set comment", "add version", LAST)
    [warning] = coded(planned.diagnostics, "SST-PLN043")
    assert "VERSION$3, committed outside SST, holds alias PROMOTED" in warning.message


def test_overwrite_of_an_edited_comment_restores_the_marker_without_a_version() -> None:
    artifact = agent()
    edited = replace(live(artifact, recorded(artifact)), marker=None, comment="edited by hand")
    planned = plan(
        (artifact,), observed=(edited,), applied={artifact.key: entry(artifact, RECORDED_MANIFEST)}, overwrite=True
    )
    [change] = planned.changes
    assert (change.action, change.reason) == (Action.UPDATE, ChangeReason.OVERWRITE)
    assert change.rendered is not None and change.rendered.adds_version is False
    assert texts(change.rendered.statements)[0] == "set comment"
    assert not any(text in ("add version", "modify live", "commit") for text in texts(change.rendered.statements))


def test_a_properties_update_pins_a_default_that_follows_last_to_the_published_version() -> None:
    # A default read as following LAST would serve the next version committed, an eval-model
    # version included, so the properties program pins it to the version by name.
    before = agent()
    after = agent(fingerprint="2" * 64)
    applied = {after.key: entry(before, RECORDED_MANIFEST)}
    for pinned, default, expected in (
        (False, "VERSION$3", ("set comment", PINNED, "set tag")),
        (True, "VERSION$3", ("set comment", "set tag")),
        (None, "VERSION$3", ("set comment", "set tag")),
        (True, "VERSION$2", ("set comment", PINNED, "set tag")),
    ):
        observed = replace(live(before, recorded(before), default_version=default), default_pinned=pinned)
        [change] = plan((after,), observed=(observed,), applied=applied).changes
        assert change.rendered is not None and change.rendered.adds_version is False
        assert texts(change.rendered.statements) == expected


def test_a_default_only_a_system_name_can_pin_is_left_alone() -> None:
    # DEFAULT_VERSION takes no alias, so a version shown under any other name is never pinned.
    artifact = agent()
    observed = replace(
        live(artifact, recorded(artifact), newest_version="V3", alias_versions=(("PROMOTED", "V3"),)),
        default_pinned=False,
    )
    assert texts(artifact.for_action(Action.UPDATE, observed).statements) == ("set comment", "set tag")
    assert unpinned_default(artifact, observed) is None
    assert pinned_default(artifact.target, "PROMOTED") is None
    assert texts((pinned_default(artifact.target, "version$3"),)) == (PINNED,)  # type: ignore[arg-type]


def test_an_unchanged_agent_whose_default_follows_last_is_pinned_once_without_a_version() -> None:
    artifact = agent()
    applied = {artifact.key: entry(artifact, RECORDED_MANIFEST)}
    for default in ("VERSION$3", None):
        observed = replace(live(artifact, recorded(artifact), default_version=default), default_pinned=False)
        planned = plan((artifact,), observed=(observed,), applied=applied)
        [change] = planned.changes
        assert (change.action, change.reason) == (Action.UPDATE, ChangeReason.DEFAULT_UNPINNED)
        assert change.rendered is not None and change.rendered.adds_version is False
        assert texts(change.rendered.statements) == ("set comment", PINNED, "set tag")
        assert [item.message for item in coded(planned.diagnostics, "SST-PLN107")] == [
            "agent:sales: the default version follows LAST; apply pins it to VERSION$3 by name"
        ]
        assert coded(planned.diagnostics, "SST-PLN102") == []


def test_a_pinned_unread_or_unpublished_default_plans_no_pin() -> None:
    artifact = agent()
    marker = recorded(artifact)
    applied = {artifact.key: entry(artifact, RECORDED_MANIFEST)}
    for pinned, newest_spec in ((True, SPEC_FINGERPRINT), (None, SPEC_FINGERPRINT), (False, "f" * 64)):
        observed = replace(
            live(artifact, marker, default_version="VERSION$3", newest_spec=newest_spec), default_pinned=pinned
        )
        [change] = plan((artifact,), observed=(observed,), applied=applied).changes
        assert (change.action, change.reason) == (Action.NOOP, ChangeReason.UNCHANGED)
        assert unpinned_default(artifact, observed) is None
    elsewhere = replace(live(artifact, marker, default_version="VERSION$2"), default_pinned=False)
    assert unpinned_default(artifact, elsewhere) is None
    temporary = agent(temporary=True)
    assert unpinned_default(temporary, replace(live(temporary, marker), default_pinned=False)) is None


def test_an_agent_with_no_live_version_and_an_eval_model_version_last_commits_a_new_release() -> None:
    # Interrupted comparison: ADD LIVE VERSION FROM LAST would open the live version on the
    # candidate's specification and serve it, so the update commits the release again instead.
    artifact = agent()
    observed = replace(live(artifact, recorded(artifact), has_live_version=False), last_is_eval_model=True)
    statements, messages = drifted(observed)
    assert statements == ("set comment", "add version", LAST)
    assert messages == ("agent:sales: it has no live version; apply points it back",)
    update = artifact.for_action(Action.UPDATE, observed)
    assert update.adds_version is True
    # With a live version, nothing opens one FROM LAST, so the properties program is safe.
    beside_live = replace(live(artifact, recorded(artifact)), last_is_eval_model=True, default_pinned=False)
    assert artifact.for_action(Action.UPDATE, beside_live).adds_version is False


def test_a_default_on_an_eval_model_version_is_drift_pointed_back_to_the_release() -> None:
    artifact = agent()
    observed = replace(
        live(artifact, recorded(artifact), default_version="VERSION$5"), last_is_eval_model=True, default_pinned=True
    )
    statements, messages = drifted(observed)
    assert statements == ("set comment", PINNED, "set tag")
    assert messages == ("agent:sales: the default version is VERSION$5, not VERSION$3; apply points it back",)


def test_eval_model_aliases_are_never_drift_and_never_unset() -> None:
    artifact = agent()
    marker = recorded(artifact)
    observed = replace(
        live(
            artifact,
            marker,
            default_version="VERSION$3",
            alias_versions=(("PROMOTED", "VERSION$3"), ("EVAL_CLAUDE_OPUS_4_8", "VERSION$5"), ("OLD", "VERSION$1")),
        ),
        aliases=("PROMOTED", "EVAL_CLAUDE_OPUS_4_8", "eval_gpt_5", "OLD"),
        last_is_eval_model=True,
        default_pinned=True,
    )
    assert version_drift(artifact, observed) == ()
    [change] = plan(
        (artifact,), observed=(observed,), applied={artifact.key: entry(artifact, RECORDED_MANIFEST)}
    ).changes
    assert (change.action, change.reason) == (Action.NOOP, ChangeReason.UNCHANGED)
    update = agent(fingerprint="2" * 64).for_action(Action.UPDATE, observed)
    assert texts(update.statements) == (
        "set comment",
        "set tag",
        "ALTER AGENT DB.S.SALES MODIFY VERSION OLD UNSET ALIAS",
    )
