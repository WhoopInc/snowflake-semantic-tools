"""Reading the account's CoWork object for a plan (a Snowflake private preview): once, and only when wanted."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.manifest import manifest_for
from snowflake_semantic_tools.app.observe import read_attachments
from snowflake_semantic_tools.app.plan import PreparePlan
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.app.plan_values import PlanCandidates, PlanReady, PlanScope
from snowflake_semantic_tools.app.preflight import read_preflight
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, SnowflakeObservation
from snowflake_semantic_tools.domain.plan.attach import AttachmentReading
from snowflake_semantic_tools.domain.state import State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.clocks import FixedClock
from tests.helpers.cowork_attach import COWORK, profile, reading
from tests.helpers.cowork_profiles import LISTED as LISTED_TREE
from tests.helpers.cowork_profiles import compile_profiles, cowork_profile, described_agent
from tests.helpers.plan_codes import manifest_of
from tests.helpers.project_inputs import EMPTY_SOURCES, InMemoryProjectInputs, dev_target
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import texts

LISTED = "SNOWFLAKE_INTELLIGENCE.PROFILES.EVERYONE"


def empty_observation() -> SnowflakeObservation:
    return SnowflakeObservation(MappingProxyType({}), "now")


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


def test_a_plan_records_the_reading_and_a_plan_from_the_record_attaches_as_the_live_one_did() -> None:
    # Compiled where CoWork lists profiles, so the profile wants attaching.
    result = compile_profiles(cowork_profile(), agents=(described_agent(),))
    use_case = PreparePlan(InMemoryProjectInputs(tree=dict(LISTED_TREE)), FixedClock())
    candidates = use_case.select(
        result,
        manifest_for(result, EMPTY_SOURCES),
        PlanScope((), None, None, None, None, False),
        partial=False,
        strict=None,
        connected=False,
        project="p",
    )
    assert isinstance(candidates, PlanCandidates)
    port = _account()
    live = use_case.run(
        candidates,
        port,
        InMemoryStateStore(State.empty(dev_target())),
        target=dev_target(),
        state_table=QualifiedName.parse("DB.SCH.SST_STATE"),
    )
    assert isinstance(live, PlanReady) and live.recorded is not None
    assert live.recorded.attachment == AttachmentReading(COWORK)
    cached = use_case.run_recorded(candidates, live.recorded)
    assert isinstance(cached, PlanReady)
    for ready in (live, cached):
        profiles = [change for change in ready.changeset.changes if change.artifact_type == "cowork_profile"]
        [change] = profiles
        assert change.rendered is not None and change.rendered.attaches_to == COWORK
        assert texts(change.rendered.statements)[-1] == (
            f"ALTER SNOWFLAKE INTELLIGENCE {COWORK} ADD AGENT SNOWFLAKE_INTELLIGENCE.PROFILES.EVERYONE"
        )


def test_preflight_reads_the_cowork_grants_once_and_only_while_a_profile_is_unattached() -> None:
    listed, other = profile(), profile("analysts")
    rendered = {listed.key: listed, other.key: other}
    port = _account(LISTED)
    port.preflight.cowork_lacking[COWORK] = ("MODIFY",)
    lacking, refused = read_preflight(
        port,
        rendered,
        empty_observation(),
        State.empty(target()),
        target(),
        include_prune=False,
        attachment=reading(listed),
    )
    assert (lacking.missing_cowork_privileges, refused) == ({COWORK: ("MODIFY",)}, ())
    # Every profile attached, an account without CoWork, or no reading: nothing is read.
    port.refuse("missing_cowork_privileges")
    for attachment in (reading(listed, other), AttachmentReading(None), None):
        quiet, failures = read_preflight(
            port,
            rendered,
            empty_observation(),
            State.empty(target()),
            target(),
            include_prune=False,
            attachment=attachment,
        )
        assert (quiet.missing_cowork_privileges, failures) == ({}, ())
    # A refused read lacks nothing, so it never blocks a write by itself.
    unread, failures = read_preflight(
        port, rendered, empty_observation(), State.empty(target()), target(), include_prune=False, attachment=reading()
    )
    assert unread.missing_cowork_privileges == {}
    assert [item.message for item in failures] == [
        f"observation of grants on the CoWork object {COWORK} failed: missing_cowork_privileges refused"
    ]


def test_a_plan_blocks_the_attaching_create_a_session_without_modify_cannot_run() -> None:
    listed = profile()
    port = _account()
    port.preflight.cowork_lacking[COWORK] = ("MODIFY",)
    changeset = PlanArtifacts(port, preflight=port).run(
        {listed.key: listed}, manifest_of(listed), State.empty(target()), target(), fetched_at="now"
    )
    [change] = changeset.changes
    assert change.action is Action.BLOCKED
    assert "SST-PLN106" in [item.code for item in changeset.diagnostics]
