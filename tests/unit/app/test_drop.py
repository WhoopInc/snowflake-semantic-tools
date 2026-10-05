"""`DropObject`: the run lease, the ownership refusal, the one DROP, and the state it forgets."""

from __future__ import annotations

import json
from types import MappingProxyType

from snowflake_semantic_tools.app.drop import DropObject, DropRequest, DropResult
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry, State
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.clocks import FixedClock
from tests.helpers.snowflake_fake import FakeSnowflake

VIEW = QualifiedName.parse("DB.S.ORDERS")
TABLE = QualifiedName.parse("DB.S.SST_STATE")
REQUEST = DropRequest(VIEW, "semantic_view", "SEMANTIC VIEW", "dev")


def _entry(target: str, manifest: str = "a" * 64) -> AppliedEntry:
    return AppliedEntry("b" * 64, target, "then", "run", APPLIED, "b" * 64, manifest)


def _port(**state: AppliedEntry) -> FakeSnowflake:
    return FakeSnowflake(existing=(VIEW.sql,), markers={VIEW.sql: OwnershipMarker("a" * 64, "b" * 64)}, state=state)


def _cache(*keys: str) -> InMemoryStateStore:
    identity = TargetIdentity("dev", "acct", VIEW.database, VIEW.schema)
    applied = MappingProxyType({key: _entry(VIEW.sql) for key in keys})
    return InMemoryStateStore(State(1, identity, "a" * 64, "sst_config.yml", None, applied))


def _drop(port: FakeSnowflake, store: InMemoryStateStore | None = None) -> object:
    return DropObject(port, store or InMemoryStateStore(), FixedClock(), state_table=TABLE, actor="R", host="h").run(
        REQUEST
    )


def test_a_drop_whose_lock_was_broken_before_it_forgets_leaves_state_to_the_new_holder() -> None:
    port = _port(**{"semantic_view:orders": _entry(VIEW.sql)})
    store = _cache("semantic_view:orders")
    execute = port.execute_script

    def broken_meanwhile(statements: object) -> object:
        port.run_locks.now = 10_000.0
        assert port.run_locks.acquire_run_lock(TABLE, "dev", LockClaim("rival"), break_stale=True).acquired
        return execute(statements)  # type: ignore[arg-type]

    port.execute_script = broken_meanwhile  # type: ignore[assignment, method-assign]
    result = DropObject(port, store, FixedClock(), state_table=TABLE).run(REQUEST)
    assert (result.outcome, result.forgotten, result.dropped) == ("dropped", (), True)
    assert [(item.code, item.message) for item in result.diagnostics] == [
        ("SST-APL011", "another run, which broke this run's lock holds the apply lock")
    ]
    assert set(port.remote_state or {}) == {"semantic_view:orders"} and store.writes == []


def test_a_drop_runs_one_statement_and_forgets_every_entry_naming_the_object() -> None:
    port = _port(**{"semantic_view:orders": _entry('"DB"."S"."ORDERS"'), "agent:orders": _entry(VIEW.sql)})
    store = _cache("semantic_view:orders", "agent:orders")
    result = DropObject(port, store, FixedClock(), state_table=TABLE).run(REQUEST)
    assert (result.outcome, result.forgotten, result.dropped) == ("dropped", ("semantic_view:orders",), True)
    assert port.scripts == [("DROP SEMANTIC VIEW DB.S.ORDERS",)]
    assert set(port.remote_state or {}) == {"agent:orders"}
    assert port.state_manifest == "a" * 64
    assert store.state is not None and set(store.state.applied) == {"agent:orders"}
    assert REQUEST.artifact == "semantic_view:orders"


def test_the_removed_entry_names_the_manifest_when_the_table_names_none() -> None:
    port = _port(**{"semantic_view:orders": _entry(VIEW.sql, "c" * 64), "semantic_view:bad": _entry("not a name")})
    port.state_manifest = None
    store = _cache("agent:other")
    result = DropObject(port, store, FixedClock(), state_table=TABLE).run(REQUEST)
    assert result.forgotten == ("semantic_view:orders",)
    assert port.state_manifest == "c" * 64
    assert store.writes == []


def test_nothing_is_forgotten_when_state_holds_no_entry_or_cannot_be_read() -> None:
    port = _port()
    assert DropObject(port, InMemoryStateStore(), FixedClock(), state_table=TABLE).run(REQUEST).forgotten == ()
    unreadable = _port()
    unreadable.read_state = lambda table, target: None  # type: ignore[method-assign,assignment]
    assert DropObject(unreadable, InMemoryStateStore(), FixedClock(), state_table=TABLE).run(REQUEST).dropped


def test_absent_unowned_rejected_and_locked_objects_are_not_dropped() -> None:
    absent = _port()
    absent.existing = set()
    assert DropObject(absent, InMemoryStateStore(), FixedClock(), state_table=TABLE).run(REQUEST).outcome == "absent"
    unowned = _port()
    unowned.markers = {}
    refused = DropObject(unowned, InMemoryStateStore(), FixedClock(), state_table=TABLE).run(REQUEST)
    assert (refused.outcome, [item.code for item in refused.diagnostics]) == ("refused", ["SST-PLN024"])
    rejected = _port()
    rejected.refused = ("DROP",)
    failed = DropObject(rejected, InMemoryStateStore(), FixedClock(), state_table=TABLE).run(REQUEST)
    assert (failed.outcome, [item.code for item in failed.diagnostics]) == ("rejected", ["SST-APL001", "SST-SNO001"])
    locked = _port()
    locked.run_locks.acquire_run_lock(TABLE, "dev", LockClaim("other"), break_stale=False)
    held = DropObject(locked, InMemoryStateStore(), FixedClock(), state_table=TABLE).run(REQUEST)
    assert (held.outcome, [item.code for item in held.diagnostics]) == ("refused", ["SST-APL011"])
    assert locked.scripts == [] and absent.scripts == [] and unowned.scripts == []


def test_a_refusal_with_no_driver_text_or_a_recognised_one_is_reported() -> None:
    from snowflake_semantic_tools.app.drop import _rejection
    from snowflake_semantic_tools.domain.model.lifecycle import ExecutionError

    assert [item.code for item in _rejection(REQUEST, None)] == ["SST-APL001", "SST-SNO001"]
    recognised = _rejection(REQUEST, ExecutionError("Insufficient privileges to operate on schema", "42501", 3001))
    assert recognised[0].code == "SST-APL001" and recognised[-1].code.startswith("SST-SNO")


AGENT = QualifiedName.parse("DB.S.ANALYST")


def _run(port: FakeSnowflake) -> DropResult:
    return DropObject(port, InMemoryStateStore(), FixedClock(), state_table=TABLE).run(REQUEST)


def _agent_spec(*names: str) -> dict[str, str]:
    """DESCRIBE AGENT's properties for an agent whose tools name `names`."""
    resources = {f"tool_{index}": {"semantic_view": name} for index, name in enumerate(names)}
    return {"agent_spec": json.dumps({"tools": [], "tool_resources": resources})}


def test_an_object_another_project_published_and_state_does_not_record_is_refused() -> None:
    foreign = _port(**{"agent:other": _entry("DB.S.OTHER")})
    foreign.markers = {VIEW.sql: OwnershipMarker("f" * 64, "b" * 64)}
    refused = _run(foreign)
    assert refused.outcome == "refused"
    assert [(item.code, item.message) for item in refused.diagnostics] == [
        ("SST-PLN024", "semantic_view:orders: DB.S.ORDERS exists without trusted SST ownership")
    ]
    assert foreign.scripts == []
    # The state table's own manifest is known as well as each entry's.
    tabled = _port(**{"agent:other": _entry("DB.S.OTHER", "c" * 64)})
    tabled.state_manifest = "c" * 64
    tabled.markers = {VIEW.sql: OwnershipMarker("f" * 64, "b" * 64)}
    assert _run(tabled).outcome == "refused"


def test_a_marker_state_vouches_for_or_cannot_contradict_is_dropped() -> None:
    same_manifest = _port(**{"agent:other": _entry("DB.S.OTHER")})
    recorded = _port(**{"semantic_view:orders": _entry(VIEW.sql, "c" * 64)})
    recorded.markers = {VIEW.sql: OwnershipMarker("f" * 64, "b" * 64)}
    no_state = _port()
    no_state.markers = {VIEW.sql: OwnershipMarker("f" * 64, "b" * 64)}
    for port in (same_manifest, recorded, no_state):
        assert _run(port).outcome == "dropped"


def test_an_agent_state_records_that_names_the_object_is_warned_of_and_the_drop_goes_ahead() -> None:
    port = _port(
        **{
            "agent:analyst": _entry(AGENT.sql),
            "agent:other": _entry("DB.S.OTHER"),
            "agent:gone": _entry("DB.S.GONE"),
            "agent:refused": _entry("DB.S.REFUSED"),
            "agent:orders": _entry(VIEW.sql),
            "agent:bad": _entry("not a name"),
            "semantic_view:orders": _entry(VIEW.sql),
        }
    )
    port.descriptions = {
        f"AGENT {AGENT.sql}": _agent_spec("db.s.orders"),
        "AGENT DB.S.OTHER": _agent_spec("DB.S.CUSTOMERS"),
        "AGENT DB.S.REFUSED": _agent_spec(VIEW.sql),
    }
    describe = port.describe_properties

    def refuse_one(object_type: str, qualified_name: QualifiedName) -> object:
        if qualified_name.sql == "DB.S.REFUSED":
            raise SnowflakePortError("DESCRIBE refused")
        return describe(object_type, qualified_name)

    port.describe_properties = refuse_one  # type: ignore[assignment, method-assign]
    result = _run(port)
    assert result.outcome == "dropped"
    assert [(item.code, item.severity.name, item.message) for item in result.diagnostics] == [
        (
            "SST-PLN017",
            "WARNING",
            "DB.S.ORDERS (named by agent DB.S.ANALYST) is referenced outside this project",
        )
    ]
    assert port.scripts == [("DROP SEMANTIC VIEW DB.S.ORDERS",)]


def test_the_warning_is_kept_when_the_drop_is_rejected_or_its_lock_was_broken() -> None:
    rejected = _port(**{"agent:analyst": _entry(AGENT.sql)})
    rejected.descriptions = {f"AGENT {AGENT.sql}": _agent_spec(VIEW.sql)}
    rejected.refused = ("DROP",)
    codes = [item.code for item in _run(rejected).diagnostics]
    assert codes == ["SST-PLN017", "SST-APL001", "SST-SNO001"]
    broken = _port(**{"agent:analyst": _entry(AGENT.sql), "semantic_view:orders": _entry(VIEW.sql)})
    broken.descriptions = {f"AGENT {AGENT.sql}": _agent_spec(VIEW.sql)}
    broken.write_state = lambda *args: False  # type: ignore[method-assign]
    assert [item.code for item in _run(broken).diagnostics] == ["SST-PLN017", "SST-APL011"]
