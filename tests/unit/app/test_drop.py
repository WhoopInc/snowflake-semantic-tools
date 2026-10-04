"""`DropObject`: the run lease, the ownership refusal, the one DROP, and the state it forgets."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.drop import DropObject, DropRequest
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
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
