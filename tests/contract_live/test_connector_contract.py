"""The real connector against the port contract the offline doubles satisfy, in a scratch schema.

`tests/contract/test_snowflake_port.py` holds `FakeSnowflake` and `FakeSnowflake` to these
same observable results. Run here against Snowflake, a divergence means the doubles promise
something the real connector does not, and every offline test that leans on them is suspect.
"""

from __future__ import annotations

from collections.abc import Callable
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.domain.diagnostics.signatures import UNRECOGNISED, match_signature
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ShowRow
from snowflake_semantic_tools.domain.sql import qname, sql
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockClaim, StateWrite
from tests.helpers.live_snowflake import LiveAccount

pytestmark = pytest.mark.live

Scratch = Callable[[str], SchemaScope]


def _name(schema: SchemaScope, object_name: str) -> QualifiedName:
    return QualifiedName(schema.database, schema.schema, Identifier.parse(object_name))


def test_the_session_runs_as_the_configured_role_and_reads_rows(
    live_connector: SnowflakeConnector, live_account: LiveAccount
) -> None:
    assert live_connector.current_role().upper() == live_account.role.upper()
    assert live_connector.current_account_locator()
    assert live_connector.query(sql("SELECT 1 AS ONE")).rows == ((1,),)


def test_an_empty_schema_lists_nothing_and_an_absent_object_has_no_marker(
    live_connector: SnowflakeConnector, scratch_schema: Scratch
) -> None:
    schema = scratch_schema("contract")
    listed = live_connector.show_objects("SEMANTIC VIEW", schema)
    assert listed == () and all(isinstance(row, ShowRow) for row in listed)
    absent = _name(schema, "SST_ABSENT_VIEW")
    assert live_connector.describe_marker(absent, "SEMANTIC VIEW") is None
    assert not live_connector.object_exists("TABLE", absent)


def test_a_created_table_exists_with_its_columns(live_connector: SnowflakeConnector, scratch_schema: Scratch) -> None:
    table = _name(scratch_schema("contract"), "SST_CONTRACT_T")
    created = live_connector.execute_script((sql("CREATE TABLE {t} (ID NUMBER, LABEL VARCHAR)", t=qname(table)),))
    assert created.ok and len(created.query_ids) == 1
    assert live_connector.object_exists("TABLE", table)
    assert [name.upper() for name, _ in live_connector.table_columns(table) or ()] == ["ID", "LABEL"]


def test_a_refused_statement_returns_a_failure_whose_text_the_signature_table_knows(
    live_connector: SnowflakeConnector, scratch_schema: Scratch
) -> None:
    absent = _name(scratch_schema("contract"), "SST_NO_SUCH_TABLE")
    result = live_connector.try_execute(sql("SELECT * FROM {t}", t=qname(absent)))
    assert not result.ok and result.query_ids == ()
    assert result.error is not None
    found = match_signature(result.error.message, errno=result.error.errno, sqlstate=result.error.sqlstate)
    assert found is not UNRECOGNISED, f"unrecognised Snowflake failure text: {result.error.message!r}"


def test_state_round_trips_and_the_run_lock_admits_one_run(
    live_connector: SnowflakeConnector, scratch_schema: Scratch
) -> None:
    state_table = _name(scratch_schema("contract"), "SST_STATE")
    entry = AppliedEntry(
        "b" * 64, _name(scratch_schema("contract"), "V").sql, "now", "run", "applied", "b" * 64, "a" * 64
    )
    live_connector.ensure_state_table(state_table)
    assert live_connector.read_state(state_table, "live") in (None, {})
    claim = LockClaim("run-a", "ROLE", "host", 60)
    taken = live_connector.acquire_run_lock(state_table, "live", claim, break_stale=False)
    assert taken.acquired and taken.fence is not None
    write = StateWrite(MappingProxyType({"semantic_view:v": entry}))
    assert live_connector.write_state(state_table, "live", "m" * 64, write, taken.fence)
    assert live_connector.read_state(state_table, "live") == {"semantic_view:v": entry}
    assert live_connector.read_state_manifest(state_table, "live") == "m" * 64

    refused = live_connector.acquire_run_lock(state_table, "live", LockClaim("run-b"), break_stale=False)
    assert not refused.acquired and refused.holder is not None and refused.holder.run_id == "run-a"
    assert live_connector.extend_run_lock(state_table, "live", taken.fence, 60)
    live_connector.release_run_lock(state_table, "live", taken.fence)
    assert not live_connector.write_state(state_table, "live", "n" * 64, write, taken.fence)
    second = live_connector.acquire_run_lock(state_table, "live", LockClaim("run-b"), break_stale=False)
    assert second.acquired and second.fence is not None and second.fence.generation > taken.fence.generation
    live_connector.release_run_lock(state_table, "live", second.fence)
