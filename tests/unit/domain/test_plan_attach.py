"""Attaching CoWork profiles (a Snowflake private preview) to the account's CoWork object as plan decides it."""

from __future__ import annotations

import json
from dataclasses import replace
from hashlib import sha256
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    Change,
    ChangeReason,
    ChangeSet,
    RenderedArtifact,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan import build_changeset
from snowflake_semantic_tools.domain.plan.attach import AttachmentReading, attach_changes, attach_statement
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from snowflake_semantic_tools.domain.plan.properties import PropertyChange, changed_properties
from snowflake_semantic_tools.domain.state.saved_plan import SavedPlan
from tests.helpers.cowork_attach import COWORK, MANIFEST, live, planned, profile, reading
from tests.helpers.plan_codes import entry, manifest_of, state_of, target
from tests.helpers.sql_values import texts

ADD = f"ALTER SNOWFLAKE INTELLIGENCE {COWORK} ADD AGENT SNOWFLAKE_INTELLIGENCE.PROFILES.EVERYONE"


def _rendered(change: Change) -> RenderedArtifact:
    assert change.rendered is not None
    return change.rendered


def _decided(change: Change, found: AttachmentReading | None) -> tuple[Change, tuple[str, ...]]:
    [decided], reported = attach_changes((change,), found)
    return decided, tuple(item.code for item in reported)


def test_the_statement_names_the_cowork_object_as_shown_and_the_profile_qualified() -> None:
    assert texts((attach_statement(COWORK, profile().target),)) == (ADD,)
    quoted = attach_statement("Mine", profile("everyone").target)
    assert texts((quoted,))[0].startswith('ALTER SNOWFLAKE INTELLIGENCE "Mine" ADD AGENT')


def test_a_create_ends_with_the_attachment_whatever_is_attached() -> None:
    artifact = profile()
    for found in (reading(), reading(artifact)):
        decided, codes = _decided(planned(artifact, Action.CREATE, ChangeReason.NOT_PRESENT), found)
        assert texts(_rendered(decided).statements) == ("create agent", "commit version", ADD)
        assert (decided.action, _rendered(decided).attaches_to, codes) == (Action.CREATE, COWORK, ())


def test_an_update_ends_with_the_attachment_only_when_the_profile_is_not_attached() -> None:
    artifact = profile()
    update = planned(artifact, Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS)
    detached, _ = _decided(update, reading())
    assert texts(_rendered(detached).statements)[-1] == ADD
    assert _rendered(detached).attaches_to == COWORK
    assert _rendered(detached).adds_version is _rendered(update).adds_version
    assert _decided(update, reading(artifact)) == (update, ())


def test_an_unchanged_profile_that_was_detached_is_updated_to_attach_it_alone() -> None:
    artifact = profile()
    unchanged = planned(artifact, Action.NOOP, ChangeReason.UNCHANGED)
    decided, codes = _decided(unchanged, reading())
    rendered = decided.rendered
    assert rendered is not None
    assert (decided.action, decided.reason, codes) == (Action.UPDATE, ChangeReason.DETACHED, ("SST-PLN104",))
    assert texts(rendered.statements) == (ADD,)
    assert (rendered.attaches_to, rendered.adds_version, rendered.fingerprint) == (COWORK, False, artifact.fingerprint)
    assert (rendered.upload_path, rendered.upload_content, rendered.upload_stage) == (None, None, None)
    assert _decided(unchanged, reading(artifact)) == (unchanged, ())


def test_without_a_cowork_object_each_published_profile_is_reported_and_left_as_planned() -> None:
    artifact = profile()
    for action, reason in (
        (Action.CREATE, ChangeReason.NOT_PRESENT),
        (Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS),
        (Action.NOOP, ChangeReason.UNCHANGED),
    ):
        change = planned(artifact, action, reason)
        assert _decided(change, AttachmentReading(None)) == (change, ("SST-PLN105",))


def test_nothing_is_attached_without_a_reading_a_wish_or_a_published_change() -> None:
    artifact = profile()
    create = planned(artifact, Action.CREATE, ChangeReason.NOT_PRESENT)
    rehearsal = planned(profile(schema="PROFILES_DEV", wants=False), Action.CREATE, ChangeReason.NOT_PRESENT)
    blocked = replace(create, action=Action.BLOCKED, reason=ChangeReason.VALIDATION_ERRORS)
    prune = replace(create, action=Action.PRUNE, reason=ChangeReason.ORPHANED, rendered=None)
    assert attach_changes((create,), None) == ((create,), ())
    for change in (rehearsal, blocked, prune):
        assert _decided(change, reading()) == (change, ())


def test_an_update_that_attaches_shows_the_attachment_as_a_property() -> None:
    artifact = profile()
    detached, _ = _decided(planned(artifact, Action.NOOP, ChangeReason.UNCHANGED), reading())
    assert changed_properties(detached, MANIFEST) == (PropertyChange("attached", "no", COWORK),)
    create, _ = _decided(planned(artifact, Action.CREATE, ChangeReason.NOT_PRESENT), reading())
    assert changed_properties(create, MANIFEST) == ()


def _planned(found: AttachmentReading | None) -> ChangeSet:
    """Plan the published, unchanged profile against what was read of the CoWork object."""
    artifact = profile()
    manifest = manifest_of(artifact)
    observed = SnowflakeObservation(MappingProxyType({artifact.key: live(artifact)}), "now")
    state = state_of(manifest, {artifact.key: entry(artifact, MANIFEST)})
    return build_changeset(
        {artifact.key: artifact}, observed, manifest, state, SEMANTIC_REGISTRY, target(), attachment=found
    )


def test_a_plan_re_attaches_a_detached_profile_after_classify_and_hashes_the_update() -> None:
    artifact = profile()

    def plan(found: AttachmentReading | None) -> tuple[Change, tuple[str, ...], str]:
        changeset = _planned(found)
        [change] = changeset.changes
        return change, tuple(item.code for item in changeset.diagnostics), changeset.plan_id

    attached, quiet, attached_id = plan(reading(artifact))
    assert (attached.action, quiet) == (Action.NOOP, ())
    detached, codes, detached_id = plan(reading())
    assert (detached.action, detached.reason, codes) == (Action.UPDATE, ChangeReason.DETACHED, ("SST-PLN104",))
    assert detached_id != attached_id
    assert plan(None)[0] == attached


def test_a_saved_re_attachment_applies_only_while_the_profile_is_still_detached() -> None:
    saved = SavedPlan.from_changeset(_planned(reading()))
    [change] = saved.changes
    assert (change.action, change.reason) == ("update", "detached")
    assert change.statement_hashes == (sha256(ADD.encode("utf-8")).hexdigest(),)
    read_back = SavedPlan.from_dict(json.loads(json.dumps(saved.as_dict())))
    assert read_back.check_applicable(SavedPlan.from_changeset(_planned(reading())), source="plan.json") is None
    # Attached again by hand since: the fresh plan attaches nothing, so the saved one is refused.
    mismatch = read_back.check_applicable(SavedPlan.from_changeset(_planned(reading(profile()))), source="plan.json")
    assert mismatch is not None and mismatch.diagnostic.code == "SST-APL012"
    assert mismatch.diagnostic.context["value"] == "the ordered changes or their statement hashes"


def test_a_change_that_attaches_is_blocked_when_no_role_in_the_session_may_attach() -> None:
    lacking = Preflight("dev", "DEPLOYER", missing_cowork_privileges=MappingProxyType({COWORK: ("MODIFY",)}))
    artifact = profile()
    created = build_changeset(
        {artifact.key: artifact},
        SnowflakeObservation(MappingProxyType({}), "now"),
        manifest_of(artifact),
        state_of(manifest_of(artifact)),
        SEMANTIC_REGISTRY,
        target(),
        preflight=lacking,
        attachment=reading(),
    )
    [blocked] = created.changes
    assert (blocked.action, [item.code for item in blocked.diagnostics]) == (Action.BLOCKED, ["SST-PLN106"])
    # Without a reading nothing is attached, and what the session lacks elsewhere blocks nothing.
    granted = Preflight("dev", "DEPLOYER", missing_cowork_privileges=MappingProxyType({"ELSEWHERE": ("MODIFY",)}))
    for preflight in (lacking, granted):
        [change] = build_changeset(
            {artifact.key: artifact},
            SnowflakeObservation(MappingProxyType({}), "now"),
            manifest_of(artifact),
            state_of(manifest_of(artifact)),
            SEMANTIC_REGISTRY,
            target(),
            preflight=preflight,
            attachment=None if preflight is lacking else reading(),
        ).changes
        assert change.action is Action.CREATE
