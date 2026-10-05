"""Plan's preflight reads through the port, and the plan use case's preflight, staleness, and partial manifest."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.manifest import manifest_for
from snowflake_semantic_tools.app.plan import PlanCandidates, PlanReady, PlanScope, PreparePlan
from snowflake_semantic_tools.app.preflight import read_preflight
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ChangeReason,
    ChangeSet,
    ObservedArtifact,
    OwnershipMarker,
    ShowRow,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from snowflake_semantic_tools.domain.ports.project import ValidationDefaults
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import AppliedEntry, State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import change, rendered
from tests.helpers.clocks import FixedClock
from tests.helpers.compile_builders import compiled, with_diagnostics
from tests.helpers.plan_codes import entry, live, manifest_of, plan, state_of, view
from tests.helpers.project_inputs import EMPTY_SOURCES, InMemoryProjectInputs, dev_target
from tests.helpers.snowflake_fake import FakeSnowflake

EVERYTHING = PlanScope((), None, None, None, None, False)


STATE_TABLE = QualifiedName.parse("DB.SCH.SST_STATE")


def _observation(*observed: ObservedArtifact) -> SnowflakeObservation:
    return SnowflakeObservation(MappingProxyType({item.key: item for item in observed}), "now")


def test_preflight_reads_each_container_relation_privilege_lock_reference_and_warehouse_once() -> None:
    port = FakeSnowflake()
    port.existing = {"DB.SCH.TAKEN"}
    answers = port.preflight
    answers.missing_databases.add("GONE")
    answers.missing_schemas.add("DB.EMPTY")
    answers.missing_relations.add("DB.SCH.ORDERS")
    answers.missing_warehouses.add("WH")
    answers.lacking["DB.SCH"] = ("CREATE SEMANTIC VIEW",)
    answers.locked["DB.SCH"] = (QualifiedName.parse("DB.SCH.TAKEN"),)
    orphan = view("orphan")
    answers.references[orphan.target.sql] = (QualifiedName.parse("BI.S.REPORT"), orphan.target)
    rendered_views = {
        item.key: item
        for item in (
            view("taken", relations=("DB.SCH.ORDERS",)),
            view("other", relations=("DB.SCH.ORDERS",)),
            view("lost", database="gone"),
            view("empty", schema="empty"),
        )
    }
    marked = live(orphan, marker=OwnershipMarker("a" * 64, orphan.fingerprint))
    state = state_of(manifest_of(), {orphan.key: entry(orphan, "a" * 64)})

    preflight, failures = read_preflight(
        port, rendered_views, _observation(marked), state, dev_target(), include_prune=True
    )

    assert failures == ()
    assert preflight.role == "TEST_ROLE"
    assert preflight.missing_databases == {"GONE"} and preflight.missing_schemas == {("DB", "EMPTY")}
    assert set(preflight.missing_relations) == {"semantic_view:taken", "semantic_view:other"}
    assert preflight.missing_privileges == {("DB", "SCH"): ("CREATE SEMANTIC VIEW",)}
    assert preflight.occupied == {"semantic_view:taken"}
    assert preflight.locked == {("DB", "SCH", "TAKEN")}
    assert preflight.referenced == {orphan.key: (QualifiedName.parse("BI.S.REPORT"),)}
    assert not preflight.warehouse_usable


def test_a_refused_preflight_read_is_reported_and_never_blocks_on_its_own() -> None:
    port = FakeSnowflake()
    port.refuse("database_exists", "relation_exists", "warehouse_exists", "locked_objects", "external_references")
    orphan = view("orphan")
    marked = live(orphan, marker=OwnershipMarker("a" * 64, orphan.fingerprint))
    sales = view("sales", relations=("DB.SCH.ORDERS",))
    preflight, failures = read_preflight(
        port,
        {sales.key: sales},
        _observation(marked),
        state_of(manifest_of()),
        replace(dev_target(), role=None),
        include_prune=True,
    )
    assert [item.code for item in failures] == ["SST-PLN001"] * 5
    assert failures[0].message == "observation of database DB failed: database_exists refused"
    assert preflight.role == "TEST_ROLE"
    assert preflight.missing_databases == frozenset() and preflight.missing_relations == {}
    assert preflight.warehouse_usable and preflight.locked == frozenset() and preflight.referenced == {}


# How Snowflake refuses SHOW LOCKS IN ACCOUNT to a role without MONITOR on the account.
_NO_MONITOR = SnowflakePortError(
    "003001 (42501): SQL access control error:\nInsufficient privileges to operate on account 'XY01234'. "
    "Your primary role DEPLOYER must have MONITOR granted on ACCOUNT XY01234.",
    errno=3001,
    sqlstate="42501",
)


def test_a_lock_read_the_role_may_not_make_skips_the_lock_check_and_fails_nothing() -> None:
    port = FakeSnowflake()
    port.fail("locked_objects", _NO_MONITOR)
    sales = view("sales")
    preflight, failures = read_preflight(
        port, {sales.key: sales}, _observation(), state_of(manifest_of()), dev_target(), include_prune=False
    )
    [skipped] = failures
    assert (skipped.code, skipped.severity) == ("SST-VAL020", Severity.INFO)
    assert skipped.message.startswith("SST-PLN019 skipped: the role may not read locks in DB.SCH: SQL access")
    assert "MONITOR granted on ACCOUNT" in skipped.message
    assert preflight.locked == frozenset()


def test_a_lock_read_that_fails_for_another_reason_is_still_a_failed_read() -> None:
    port = FakeSnowflake()
    port.fail("locked_objects", SnowflakePortError("251005: connection reset", errno=251005, sqlstate="08006"))
    sales = view("sales")
    _, failures = read_preflight(
        port, {sales.key: sales}, _observation(), state_of(manifest_of()), dev_target(), include_prune=False
    )
    assert [(item.code, item.severity) for item in failures] == [("SST-PLN001", Severity.ERROR)]
    assert failures[0].message == "observation of locks in DB.SCH failed: 251005: connection reset"


def test_a_plan_by_a_role_without_monitor_goes_ahead_and_may_be_reused() -> None:
    port = FakeSnowflake()
    port.fail("locked_objects", _NO_MONITOR)
    ready = PreparePlan(InMemoryProjectInputs(), FixedClock()).run(
        _select(compiled()),
        port,
        InMemoryStateStore(State.empty(dev_target())),
        target=dev_target(),
        state_table=STATE_TABLE,
        preflight=port,
    )
    assert isinstance(ready, PlanReady)
    codes = [item.code for item in ready.changeset.diagnostics]
    assert "SST-VAL020" in codes and "SST-PLN001" not in codes
    assert not ready.changeset.diagnostics.has_errors
    assert ready.recorded is not None


# How Snowflake refuses SNOWFLAKE.ACCOUNT_USAGE to a role it is not shared with.
_NO_ACCOUNT_USAGE = SnowflakePortError(
    "002003 (02000): SQL compilation error:\nSchema 'SNOWFLAKE.ACCOUNT_USAGE' does not exist or not authorized.",
    errno=2003,
    sqlstate="02000",
)


def _prune_preflight(error: SnowflakePortError) -> tuple[Preflight, tuple[Diagnostic, ...], ChangeSet]:
    """Read preflight for one marked prune candidate whose reference read fails with `error`, then plan it."""
    port = FakeSnowflake()
    port.fail("external_references", error)
    orphan = view("orphan")
    marked = live(orphan, marker=OwnershipMarker("a" * 64, orphan.fingerprint))
    applied = {orphan.key: entry(orphan, "a" * 64)}
    preflight, failures = read_preflight(
        port, {}, _observation(marked), state_of(manifest_of(), applied), dev_target(), include_prune=True
    )
    planned = plan((), observed=(marked,), applied=applied, include_prune=True, preflight=preflight)
    return preflight, failures, planned


def test_a_reference_read_the_role_may_not_make_skips_the_check_for_that_prune_candidate() -> None:
    preflight, failures, _ = _prune_preflight(_NO_ACCOUNT_USAGE)
    [skipped] = failures
    assert (skipped.code, skipped.severity) == ("SST-VAL020", Severity.INFO)
    assert skipped.message == (
        "SST-PLN017 skipped: the role may not read references to DB.SCH.ORPHAN: "
        "Schema 'SNOWFLAKE.ACCOUNT_USAGE' does not exist or not authorized."
    )
    assert preflight.referenced == {}


def test_a_prune_by_a_role_without_account_usage_goes_ahead_with_the_skip_reported() -> None:
    _, failures, planned = _prune_preflight(_NO_ACCOUNT_USAGE)
    assert [(item.action, item.prune_executable) for item in planned.changes] == [(Action.PRUNE, True)]
    assert "SST-PLN017" not in [item.code for item in planned.diagnostics]
    assert not DiagnosticBag(failures).has_errors


def test_a_reference_read_that_fails_for_another_reason_is_still_a_failed_read() -> None:
    reset = SnowflakePortError("251005: connection reset", errno=251005, sqlstate="08006")
    _, failures, _ = _prune_preflight(reset)
    assert [(item.code, item.severity) for item in failures] == [("SST-PLN001", Severity.ERROR)]
    assert failures[0].message == "observation of references to DB.SCH.ORPHAN failed: 251005: connection reset"
    unseen = SnowflakePortError("Object 'BI.S.T' does not exist or not authorized.", errno=2003, sqlstate="02000")
    _, failures, _ = _prune_preflight(unseen)
    assert [item.code for item in failures] == ["SST-PLN001"]


def test_a_schema_or_privilege_read_refused_leaves_the_write_unblocked() -> None:
    port = FakeSnowflake()
    port.refuse("schema_exists", "missing_privileges")
    sales = view("sales")
    preflight, failures = read_preflight(
        port, {sales.key: sales}, _observation(), state_of(manifest_of()), dev_target(), include_prune=False
    )
    assert [item.message.split(" failed")[0] for item in failures] == [
        "observation of schema DB.SCH",
        "observation of grants on schema DB.SCH",
    ]
    assert preflight.missing_schemas == frozenset() and preflight.missing_privileges == {}


def test_an_unparseable_warehouse_is_unusable_and_an_unnamed_one_is_not_read() -> None:
    port = FakeSnowflake()
    port.refuse("warehouse_exists")
    named, _ = read_preflight(
        port,
        {},
        _observation(),
        state_of(manifest_of()),
        replace(dev_target(), warehouse='"BROKEN'),
        include_prune=False,
    )
    unnamed, failures = read_preflight(
        port, {}, _observation(), state_of(manifest_of()), replace(dev_target(), warehouse=None), include_prune=False
    )
    assert not named.warehouse_usable and unnamed.warehouse_usable and failures == ()


def _select(
    result: CompileResult, *, partial: bool = False, manifest_of_compile: CompileResult | None = None
) -> PlanCandidates:
    candidates = PreparePlan(InMemoryProjectInputs(), FixedClock()).select(
        result,
        manifest_for(manifest_of_compile or result, EMPTY_SOURCES),
        EVERYTHING,
        partial=partial,
        strict=None,
        connected=False,
        project="the-project",
    )
    assert isinstance(candidates, PlanCandidates)
    return candidates


def test_run_preflights_through_the_port_it_is_given() -> None:
    port = FakeSnowflake()
    port.preflight.missing_schemas.add("DB.SCH")
    ready = PreparePlan(InMemoryProjectInputs(), FixedClock()).run(
        _select(compiled()),
        port,
        InMemoryStateStore(State.empty(dev_target())),
        target=dev_target(),
        state_table=STATE_TABLE,
        preflight=port,
    )
    assert isinstance(ready, PlanReady)
    assert "SST-PLN010" in [item.code for item in ready.changeset.diagnostics]
    assert ready.changeset.diagnostics[-1].code == "SST-PLN016"


class _SlowClock(FixedClock):
    def monotonic_ms(self) -> int:
        self.current += 16 * 60 * 1000
        return self.current


def test_a_plan_that_outlives_its_observation_reports_it() -> None:
    ready = PreparePlan(InMemoryProjectInputs(), _SlowClock()).run(
        _select(compiled()),
        FakeSnowflake(),
        InMemoryStateStore(State.empty(dev_target())),
        target=dev_target(),
        state_table=STATE_TABLE,
    )
    assert isinstance(ready, PlanReady)
    assert "SST-PLN018" in [item.code for item in ready.changeset.diagnostics]


def test_a_partial_plans_manifest_covers_only_what_stays_healthy_after_validation() -> None:
    # A warning on one view compiles, so the compile split leaves nothing out; strict
    # validation promotes it, so the plan's own split leaves the view out.
    warned = with_diagnostics(compiled("MENU", "SALES"), D("SST-PLN014", artifact="menu", subject="semantic_view:menu"))
    inputs = InMemoryProjectInputs(validation=ValidationDefaults(strict=True, snowflake_syntax_check=False))
    use_case = PreparePlan(inputs, FixedClock())
    candidates = use_case.select(
        warned,
        manifest_for(warned, EMPTY_SOURCES),
        EVERYTHING,
        partial=True,
        strict=None,
        connected=None,
        project="the-project",
    )
    assert isinstance(candidates, PlanCandidates) and candidates.split is None
    ready = use_case.run(
        candidates,
        FakeSnowflake(),
        InMemoryStateStore(State.empty(dev_target())),
        target=dev_target(),
        state_table=STATE_TABLE,
    )
    assert isinstance(ready, PlanReady)
    sales_only = replace(warned, compiled=tuple(item for item in warned.compiled if item.name == "SALES"))
    assert ready.manifest.manifest_id == manifest_for(sales_only, EMPTY_SOURCES).manifest_id
    assert ready.manifest.manifest_id != candidates.manifest.manifest_id
    assert {change.key for change in ready.changeset.changes} == {"semantic_view:sales"}


def test_a_report_only_prune_restamps_state_until_state_records_it_under_the_plans_manifest() -> None:
    gone = rendered("GONE")
    report_only = replace(change(gone, Action.PRUNE), prune_executable=False, reason=ChangeReason.ORPHANED)
    planned = ChangeSet("m" * 64, dev_target(), (report_only,), DiagnosticBag(), "now")
    manifest = manifest_of()
    stale = AppliedEntry("f", gone.target.sql, "now", "run", "applied", "f", "old")
    current = replace(stale, manifest_id=manifest.manifest_id)

    def ready(applied: dict[str, AppliedEntry]) -> PlanReady:
        return PlanReady(compiled(), manifest, state_of(manifest, applied), planned, MappingProxyType({}))

    assert ready({}).restamps_state and ready({gone.key: stale}).restamps_state
    assert not ready({gone.key: current}).restamps_state


def test_observation_rows_still_reach_the_plan() -> None:
    port = FakeSnowflake()
    port.show(ShowRow("OTHER", "DB", "SCH", "OWNER", "now", None))
    ready = PreparePlan(InMemoryProjectInputs(), FixedClock()).run(
        _select(compiled()),
        port,
        InMemoryStateStore(State.empty(dev_target())),
        target=dev_target(),
        state_table=STATE_TABLE,
    )
    assert isinstance(ready, PlanReady)
