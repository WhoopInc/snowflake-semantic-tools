"""Reading the account's CoWork object for a plan (a Snowflake private preview): once, and only when wanted."""

from __future__ import annotations

from snowflake_semantic_tools.app.observe import read_attachments
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.plan.attach import AttachmentReading
from snowflake_semantic_tools.domain.state import State
from tests.helpers.artifact_builders import target
from tests.helpers.cowork_attach import COWORK, profile, reading
from tests.helpers.snowflake_fake import FakeSnowflake

LISTED = "SNOWFLAKE_INTELLIGENCE.PROFILES.EVERYONE"


def _account(*attached: str) -> FakeSnowflake:
    port = FakeSnowflake()
    port.intelligences = [COWORK]
    port.intelligence_agents[COWORK] = list(attached)
    return port


def test_a_profile_that_wants_attaching_reads_the_cowork_object_and_what_is_attached() -> None:
    listed = profile()
    found, failures = read_attachments(_account(LISTED, "DB.S.OTHER"), {listed.key: listed})
    assert found == AttachmentReading(COWORK, frozenset({listed.target.folded, ("DB", "S", "OTHER")}))
    assert not failures


def test_a_rehearsal_reads_nothing_and_an_account_without_cowork_reads_only_the_listing() -> None:
    port = FakeSnowflake()
    port.refuse("cowork_object", "attached_agents")
    rehearsal = profile(schema="PROFILES_DEV", wants=False)
    assert read_attachments(port, {rehearsal.key: rehearsal}) == (None, ())
    port.heal("cowork_object")
    listed = profile()
    assert read_attachments(port, {listed.key: listed}) == (AttachmentReading(None), ())


def test_a_refused_read_is_reported_and_leaves_no_reading() -> None:
    port = _account()
    port.refuse("attached_agents")
    listed = profile()
    found, failures = read_attachments(port, {listed.key: listed})
    assert found is None
    [failure] = failures
    assert failure.code == "SST-PLN001"
    assert failure.message == "observation of the CoWork object failed: attached_agents refused"


def test_the_plan_reading_carries_the_attachment_with_and_without_preflight() -> None:
    listed = profile()
    port = _account(LISTED)
    for preflight in (None, port):
        planner = PlanArtifacts(port, preflight=preflight)
        found = planner.read({listed.key: listed}, State.empty(target()), target(), fetched_at="now")
        assert found.attachment == reading(listed)
        assert not found.failures.has_errors
    port.refuse("cowork_object")
    refused = PlanArtifacts(port).read({listed.key: listed}, State.empty(target()), target(), fetched_at="now")
    assert refused.attachment is None and [item.code for item in refused.failures] == ["SST-PLN001"]
