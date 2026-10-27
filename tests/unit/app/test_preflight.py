"""Plan's preflight reads through the port, and the plan use case's preflight, staleness, and partial manifest."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.manifest import manifest_for
from snowflake_semantic_tools.app.plan import PreparePlan
from snowflake_semantic_tools.app.plan_values import PlanCandidates, PlanReady, PlanScope
from snowflake_semantic_tools.app.preflight import read_preflight
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
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
from snowflake_semantic_tools.domain.ports.snowflake.preflight import ROLE_HIERARCHY_DEPTH, RoleWalk
from snowflake_semantic_tools.domain.state import AppliedEntry, State
from tests.helpers.agent_stage import STAGE as AGENT_STAGE
from tests.helpers.agent_stage import plan_agents, staged_agent
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import change, rendered
from tests.helpers.clocks import FixedClock
from tests.helpers.compile_builders import compiled, with_diagnostics
from tests.helpers.plan_codes import entry, live, manifest_of, plan, state_of, view
from tests.helpers.project_inputs import EMPTY_SOURCES, InMemoryProjectInputs, dev_target
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.snowflake_fake.driver import FakeDriverConnector, FakeDriverSession
from tests.helpers.snowflake_fake.grants import GrantGraph

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


def test_an_absent_state_table_asks_for_create_table_on_its_schema_and_an_existing_one_asks_nothing() -> None:
    port = FakeSnowflake()
    state_table, lock_table = QualifiedName.parse("DB.STATE.SST_STATE"), QualifiedName.parse("DB.STATE.SST_STATE_LOCK")
    port.preflight.lacking["DB.STATE"] = ("CREATE TABLE",)
    sales = view("sales")

    def read(*missing: QualifiedName) -> Preflight:
        port.preflight.missing_relations = {table.sql for table in missing}
        found, failures = read_preflight(
            port,
            {sales.key: sales},
            _observation(),
            state_of(manifest_of()),
            dev_target(),
            include_prune=False,
            state_tables=(state_table, lock_table),
        )
        assert failures == ()
        return found

    lock_absent = read(lock_table)
    assert lock_absent.absent_state_tables == (lock_table,)
    assert lock_absent.missing_privileges[("DB", "STATE")] == ("CREATE TABLE",)
    # Both tables exist: a role that may only read and write them needs no CREATE privilege.
    present = read()
    assert present.absent_state_tables == () and ("DB", "STATE") not in present.missing_privileges


def test_the_kind_of_each_state_table_that_exists_is_read_and_an_unknown_one_left_out() -> None:
    port = FakeSnowflake()
    state_table, lock_table = QualifiedName.parse("DB.STATE.SST_STATE"), QualifiedName.parse("DB.STATE.SST_STATE_LOCK")
    gone = QualifiedName.parse("DB.STATE.GONE")
    port.preflight.missing_relations = {gone.sql}
    port.preflight.table_kinds = {state_table.sql: "standard", gone.sql: "hybrid"}
    sales = view("sales")
    found, failures = read_preflight(
        port,
        {sales.key: sales},
        _observation(),
        state_of(manifest_of()),
        dev_target(),
        include_prune=False,
        state_tables=(state_table, lock_table, gone),
        state_table_kind="hybrid",
    )
    # The lock table's kind is not known, and an absent table's is never read.
    assert failures == ()
    assert found.state_table_kind == "hybrid" and found.state_table_kinds == ((state_table, "standard"),)


def test_a_kind_read_that_raises_is_left_out_unreported() -> None:
    port = FakeSnowflake()
    state_table = QualifiedName.parse("DB.STATE.SST_STATE")
    port.preflight.table_kinds = {state_table.sql: "standard"}
    port.refuse("table_kind")
    sales = view("sales")
    found, failures = read_preflight(
        port,
        {sales.key: sales},
        _observation(),
        state_of(manifest_of()),
        dev_target(),
        include_prune=False,
        state_tables=(state_table,),
    )
    assert failures == () and found.state_table_kinds == ()


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


def test_preflight_reads_each_upload_stage_once_and_the_privileges_each_one_needs() -> None:
    port = FakeSnowflake()
    absent, usable, wrong, unread, gone, refused = (
        QualifiedName.parse(f"DB.{schema}.SPECS") for schema in ("S", "READY", "WRONG", "UNREAD", "GONE", "REFUSED")
    )
    port.stage_types.update({usable.sql: "INTERNAL NO CSE", wrong.sql: "EXTERNAL", unread.sql: "INTERNAL NO CSE"})
    port.preflight.missing_schemas.add("DB.GONE")
    port.preflight.lacking["DB.S"] = ("CREATE STAGE",)
    port.preflight.stage_lacking[usable.sql] = ("WRITE",)
    port.preflight.locked["DB.READY"] = (usable,)
    agents = {
        item.key: item
        for item in (
            staged_agent("a", stage=absent),
            staged_agent("b", stage=absent),
            staged_agent("c", stage=usable),
            staged_agent("d", stage=wrong),
            staged_agent("e", stage=gone),
            staged_agent("f", stage=refused),
            staged_agent("g", stage=unread),
        )
    }
    stage_reads = 0
    stage_type = port.stage_type

    def counted(qualified_name: QualifiedName) -> str | None:
        nonlocal stage_reads
        stage_reads += 1
        if qualified_name == refused:
            raise SnowflakePortError("stage_type refused")
        return stage_type(qualified_name)

    port.stage_type = counted  # type: ignore[method-assign]
    port.fail("missing_stage_privileges", SnowflakePortError("grants refused"), times=2)
    preflight, failures = read_preflight(
        port, agents, _observation(), state_of(manifest_of()), dev_target(), include_prune=False
    )
    assert stage_reads == 5
    assert [item.message for item in failures] == [
        "observation of stage DB.REFUSED.SPECS failed: stage_type refused",
        "observation of grants on stage DB.READY.SPECS failed: grants refused",
        "observation of grants on stage DB.UNREAD.SPECS failed: grants refused",
    ]
    assert preflight.absent_stages == {absent.folded}
    assert preflight.stage_types == {
        usable.folded: "INTERNAL NO CSE",
        wrong.folded: "EXTERNAL",
        unread.folded: "INTERNAL NO CSE",
    }
    assert preflight.missing_privileges == {("DB", "S"): ("CREATE STAGE",)}
    assert preflight.missing_stage_privileges == {}
    assert preflight.missing_schemas == {("DB", "GONE")}
    # Only a target's schema is read for locks, not a schema that holds no more than a stage.
    assert preflight.locked == frozenset()

    port.heal("missing_stage_privileges")
    preflight, failures = read_preflight(
        port, agents, _observation(), state_of(manifest_of()), dev_target(), include_prune=False
    )
    assert preflight.missing_stage_privileges == {usable.folded: ("WRITE",)}


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
    assert skipped.message.startswith("SST-PLN019 skipped: the role may not read locks in DB.SCH: Insufficient")
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
    port.refuse("schema_exists", "missing_role_privileges")
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


def _preflighted(tree: dict[str, object]) -> PlanReady:
    port = FakeSnowflake()
    port.preflight.missing_schemas.add("DB.SCH")
    ready = PreparePlan(InMemoryProjectInputs(tree=tree), FixedClock()).run(
        _select(compiled()),
        port,
        InMemoryStateStore(State.empty(dev_target())),
        target=dev_target(),
        state_table=STATE_TABLE,
        preflight=port,
    )
    assert isinstance(ready, PlanReady)
    return ready


def test_run_preflights_through_the_port_it_is_given() -> None:
    # `apply.create_schemas` is on unless set false: the absent schema is one apply creates.
    ready = _preflighted({})
    assert [scope.sql for scope in ready.changeset.schemas] == ["DB.SCH"]
    assert "SST-PLN010" not in [item.code for item in ready.changeset.diagnostics]
    off = _preflighted({"apply": {"create_schemas": False}})
    assert off.changeset.schemas == ()
    assert "SST-PLN010" in [item.code for item in off.changeset.diagnostics]
    assert off.changeset.diagnostics[-1].code == "SST-PLN016"


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
    warned = with_diagnostics(
        compiled("MENU", "SALES"),
        D(
            "SST-PLN014",
            artifact="menu",
            detail="its ownership marker is not the one state records",
            subject="semantic_view:menu",
        ),
    )
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


def test_a_partial_plan_leaves_out_an_object_it_blocks_against_the_target_and_plans_the_rest() -> None:
    # MENU compiles cleanly, but an object SST does not manage holds its name (SST-PLN024).
    result = compiled("MENU", "SALES")
    use_case = PreparePlan(InMemoryProjectInputs(), FixedClock())
    candidates = use_case.select(
        result, manifest_for(result, EMPTY_SOURCES), EVERYTHING, partial=True, strict=None, connected=None, project="p"
    )
    assert isinstance(candidates, PlanCandidates)
    port = FakeSnowflake()
    port.objects[("SEMANTIC VIEW", "DB.SCH")] = (ShowRow("MENU", "DB", "SCH", "OWNER", "now"),)
    ready = use_case.run(
        candidates, port, InMemoryStateStore(State.empty(dev_target())), target=dev_target(), state_table=STATE_TABLE
    )
    assert isinstance(ready, PlanReady)
    assert [change.key for change in ready.changeset.changes] == ["semantic_view:sales"]
    assert not ready.changeset.blocked and not ready.changeset.diagnostics.has_errors
    left_out = [
        (item.code, item.subject) for item in ready.result.diagnostics if item.code in ("SST-PLN024", "SST-PLN032")
    ]
    assert left_out == [("SST-PLN024", None), ("SST-PLN032", "semantic_view:menu")]
    assert ready.result.diagnostics.has_errors


def test_a_report_only_prune_restamps_state_only_until_state_has_an_entry_for_it() -> None:
    gone = rendered("GONE")
    report_only = replace(change(gone, Action.PRUNE), prune_executable=False, reason=ChangeReason.ORPHANED)
    planned = ChangeSet("m" * 64, dev_target(), (report_only,), DiagnosticBag(), "now")
    manifest = manifest_of()
    earlier = AppliedEntry("f", gone.target.sql, "now", "run", "applied", "f", "old")

    def ready(applied: dict[str, AppliedEntry]) -> PlanReady:
        return PlanReady(compiled(), manifest, state_of(manifest, applied), planned, MappingProxyType({}))

    # An entry another manifest recorded needs no restamp: the object is unchanged.
    assert ready({}).restamps_state
    assert not ready({gone.key: earlier}).restamps_state
    assert not ready({gone.key: replace(earlier, manifest_id=manifest.manifest_id)}).restamps_state


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


def _granted(graph: GrantGraph, session: FakeDriverSession | None = None) -> FakeSnowflake:
    """The fake, its privilege reads answered by the real connector over `graph`, on `session`."""
    connector = FakeDriverConnector(session or graph.session())

    class Granted(FakeSnowflake):
        def missing_privileges(self, scope: SchemaScope, privileges: tuple[str, ...]) -> tuple[str, ...]:
            return connector.missing_privileges(scope, privileges)

        def has_secondary_roles(self) -> bool:
            return connector.has_secondary_roles()

        def missing_role_privileges(
            self, role: str, scope: SchemaScope, privileges: tuple[str, ...]
        ) -> tuple[str, ...]:
            return connector.missing_role_privileges(role, scope, privileges)

        def walk_role_privileges(self, role: str, scope: SchemaScope, privileges: tuple[str, ...]) -> RoleWalk:
            return connector.walk_role_privileges(role, scope, privileges)

        def missing_stage_privileges(self, stage: QualifiedName, privileges: tuple[str, ...]) -> tuple[str, ...]:
            return connector.missing_stage_privileges(stage, privileges)

    port = Granted()
    port.stage_types[AGENT_STAGE.sql] = "INTERNAL NO CSE"
    return port


def _agent_grants() -> GrantGraph:
    """An agent schema whose spec stage only a secondary role in the session may read and write."""
    graph = GrantGraph(SchemaScope.from_qualified_name(AGENT_STAGE)).in_session("TEST_ROLE", "SECONDARY")
    graph.secondary("SECONDARY")
    return graph.on_stage("AGENT_SPECS", "READ", role="SECONDARY").on_stage("AGENT_SPECS", "WRITE", role="SECONDARY")


def test_a_create_privilege_counts_through_the_primary_role_s_hierarchy() -> None:
    graph = _agent_grants().on_schema("CREATE AGENT", role="BUILDER").role_to("BUILDER", role_to="TEST_ROLE")
    [created] = plan_agents(_granted(graph), staged_agent("a")).changes
    # READ and WRITE on the stage are not CREATE privileges, so the secondary role's count.
    assert created.action is Action.CREATE and created.diagnostics == ()


def test_a_create_privilege_only_a_secondary_role_holds_blocks_the_create_and_says_so() -> None:
    graph = _agent_grants().on_schema("CREATE AGENT", role="SECONDARY")
    [blocked] = plan_agents(_granted(graph), staged_agent("a")).changes
    assert blocked.action is Action.BLOCKED
    assert [(item.code, item.message) for item in blocked.diagnostics] == [
        (
            "SST-PLN008",
            "TEST_ROLE lacks CREATE AGENT (only a secondary role in the session holds it, and a CREATE uses "
            "the primary role) on DB.S",
        )
    ]
    [unheld] = plan_agents(_granted(_agent_grants()), staged_agent("a")).changes
    assert [item.message for item in unheld.diagnostics] == ["TEST_ROLE lacks CREATE AGENT on DB.S"]


def _far(graph: GrantGraph, levels: int) -> GrantGraph:
    """Grant CREATE AGENT to a role `levels` grants up the hierarchy from TEST_ROLE, active in the session."""
    graph.on_schema("CREATE AGENT", role=f"R{levels:02d}").in_session(f"R{levels:02d}")
    for level in range(levels, 0, -1):
        graph.role_to(f"R{level:02d}", role_to=f"R{level - 1:02d}" if level > 1 else "TEST_ROLE")
    return graph


def test_without_a_secondary_role_the_session_is_asked_alone_without_walking_the_hierarchy() -> None:
    graph = _far(GrantGraph(SchemaScope.from_qualified_name(AGENT_STAGE)).in_session("TEST_ROLE"), 20)
    graph.on_stage("AGENT_SPECS", "READ", role="TEST_ROLE").on_stage("AGENT_SPECS", "WRITE", role="TEST_ROLE")
    session = graph.session()
    [created] = plan_agents(_granted(graph, session), staged_agent("a")).changes
    # The session's roles are the primary role's hierarchy, which IS_ROLE_IN_SESSION answers whole.
    assert created.action is Action.CREATE and created.diagnostics == ()
    assert "SELECT CURRENT_SECONDARY_ROLES()" in session.statements
    assert not [statement for statement in session.statements if statement.startswith("SHOW GRANTS OF ROLE")]


def test_a_privilege_past_the_walk_s_depth_is_not_said_to_be_held_by_a_secondary_role() -> None:
    graph = _far(_agent_grants(), ROLE_HIERARCHY_DEPTH + 4)
    [blocked] = plan_agents(_granted(graph), staged_agent("a")).changes
    assert [(item.code, item.message) for item in blocked.diagnostics] == [
        (
            "SST-PLN008",
            "TEST_ROLE lacks CREATE AGENT (the session holds it, but no inheritance path from the primary role "
            f"was found within {ROLE_HIERARCHY_DEPTH} levels, and a CREATE uses the primary role) on DB.S",
        )
    ]


def test_a_secondary_role_read_that_is_refused_walks_the_hierarchy_as_before() -> None:
    port = FakeSnowflake()
    port.refuse("has_secondary_roles")
    port.preflight.role_lacking["DB.SCH"] = ("CREATE SEMANTIC VIEW",)
    sales = view("sales")
    preflight, failures = read_preflight(
        port, {sales.key: sales}, _observation(), state_of(manifest_of()), dev_target(), include_prune=False
    )
    assert failures == ()
    assert preflight.secondary_privileges == {("DB", "SCH"): ("CREATE SEMANTIC VIEW",)}
    port.preflight.secondary_roles = False
    port.heal("has_secondary_roles")
    alone, _ = read_preflight(
        port, {sales.key: sales}, _observation(), state_of(manifest_of()), dev_target(), include_prune=False
    )
    assert alone.missing_privileges == {}


def test_a_primary_role_that_cannot_be_read_counts_every_role_in_the_session() -> None:
    port = FakeSnowflake()
    port.refuse("current_role", "missing_role_privileges")
    port.preflight.lacking["DB.SCH"] = ("CREATE SEMANTIC VIEW",)
    sales = view("sales")
    preflight, failures = read_preflight(
        port, {sales.key: sales}, _observation(), state_of(manifest_of()), dev_target(), include_prune=False
    )
    assert [item.message.split(" failed")[0] for item in failures] == ["observation of the current role"]
    assert preflight.missing_privileges == {("DB", "SCH"): ("CREATE SEMANTIC VIEW",)}
    assert preflight.secondary_privileges == {}


def test_a_session_read_refused_leaves_what_the_primary_role_lacks_unexplained() -> None:
    port = FakeSnowflake()
    port.refuse("missing_privileges")
    port.preflight.role_lacking["DB.SCH"] = ("CREATE SEMANTIC VIEW",)
    sales = view("sales")
    preflight, failures = read_preflight(
        port,
        {sales.key: sales},
        _observation(),
        state_of(manifest_of()),
        replace(dev_target(), role="DEPLOYER"),
        include_prune=False,
    )
    assert [item.message.split(" failed")[0] for item in failures] == ["observation of grants on schema DB.SCH"]
    assert preflight.missing_privileges == {("DB", "SCH"): ("CREATE SEMANTIC VIEW",)}
    assert preflight.secondary_privileges == {}
    # The profile's role names the role in the message; CURRENT_ROLE() is what the walk climbs to.
    assert preflight.role == "DEPLOYER"


class _RefusesAbsentSchemas(FakeSnowflake):
    """Refuse to list an absent schema, as Snowflake does (002003)."""

    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]:
        if scope.sql in self.preflight.missing_schemas:
            raise SnowflakePortError(f"Schema '{scope.sql}' does not exist or not authorized.")
        return super().show_objects(object_type, scope)


def _planned_on(port: FakeSnowflake, tree: dict[str, object]) -> PlanReady:
    ready = PreparePlan(InMemoryProjectInputs(tree=tree), FixedClock()).run(
        _select(compiled()),
        port,
        InMemoryStateStore(State.empty(dev_target())),
        target=dev_target(),
        state_table=STATE_TABLE,
        preflight=port,
    )
    assert isinstance(ready, PlanReady)
    return ready


def test_a_schema_apply_creates_is_not_observed_and_one_state_records_or_unreadable_still_is() -> None:
    port = _RefusesAbsentSchemas()
    port.preflight.missing_schemas.add("DB.SCH")
    created = _planned_on(port, {})
    assert "SST-PLN001" not in [item.code for item in created.changeset.diagnostics]
    assert [scope.sql for scope in created.changeset.schemas] == ["DB.SCH"]
    # With the setting off, observing the absent schema fails as it always did.
    off = _planned_on(port, {"apply": {"create_schemas": False}})
    assert {"SST-PLN001", "SST-PLN010"} <= {item.code for item in off.changeset.diagnostics}
    # A schema state records an object in may only be one the role cannot see: it is observed.
    held = _RefusesAbsentSchemas()
    held.preflight.missing_schemas.add("DB.SCH")
    entry = AppliedEntry("f" * 64, "DB.SCH.OTHER", "now", "run", "applied", "f" * 64, "m" * 64)
    held.remote_state = MappingProxyType({"semantic_view:other": entry})
    assert "SST-PLN001" in [item.code for item in _planned_on(held, {}).changeset.diagnostics]
    # A schema whose existence cannot be read counts present, so its observation reports why.
    unreadable = _RefusesAbsentSchemas()
    unreadable.preflight.missing_schemas.add("DB.SCH")
    unreadable.fail("schema_exists", SnowflakePortError("refused"))
    assert "SST-PLN001" in [item.code for item in _planned_on(unreadable, {}).changeset.diagnostics]


def test_an_absent_hybrid_state_table_asks_for_create_hybrid_table_not_create_table() -> None:
    port = FakeSnowflake()
    state_table = QualifiedName.parse("DB.STATE.SST_STATE")
    # The role holds CREATE TABLE, which since BCR 2026_02 no longer covers a hybrid table.
    port.preflight.lacking["DB.STATE"] = ("CREATE HYBRID TABLE",)
    port.preflight.missing_relations = {state_table.sql}
    sales = view("sales")

    def read(kind: str) -> Preflight:
        found, failures = read_preflight(
            port,
            {sales.key: sales},
            _observation(),
            state_of(manifest_of()),
            dev_target(),
            include_prune=False,
            state_tables=(state_table,),
            state_table_kind=kind,
        )
        assert failures == ()
        return found

    assert read("hybrid").missing_privileges[("DB", "STATE")] == ("CREATE HYBRID TABLE",)
    # A standard table needs only CREATE TABLE, which the role holds.
    assert ("DB", "STATE") not in read("standard").missing_privileges
