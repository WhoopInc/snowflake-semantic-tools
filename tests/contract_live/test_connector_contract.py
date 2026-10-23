"""The real connector against the port contract the offline doubles satisfy, in a scratch schema.

`tests/contract/test_snowflake_port.py` holds `FakeSnowflake` and `ReadOnlySnowflake` to these
same observable results. Run here against Snowflake, a divergence means the doubles promise
something the real connector does not, and every offline test that leans on them is suspect.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.app.drift import written_digest
from snowflake_semantic_tools.domain.diagnostics.signatures import UNRECOGNIZED, match_signature
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import PublishShape, RenderedArtifact, ShowRow, drifted_properties
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import ident, qname, sql
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import (
    HYBRID_TABLES,
    STANDARD_TABLES,
    TABLE_KIND_SETTING,
    LockClaim,
    StateWrite,
)
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
    assert found is not UNRECOGNIZED, f"unrecognized Snowflake failure text: {result.error.message!r}"


@pytest.fixture(params=[STANDARD_TABLES, HYBRID_TABLES])
def state_connector(
    request: pytest.FixtureRequest, live_account: LiveAccount, live_connector: SnowflakeConnector
) -> Iterator[tuple[str, SnowflakeConnector]]:
    """The live connector, and one configured for hybrid state tables, as `state.+table_kind` sets."""
    if request.param == STANDARD_TABLES:
        yield request.param, live_connector
        return
    connector = SnowflakeConnector({**live_account.connection_params(), TABLE_KIND_SETTING: HYBRID_TABLES})
    try:
        yield request.param, connector
    finally:
        connector.close()


def test_state_round_trips_and_the_run_lock_admits_one_run(
    state_connector: tuple[str, SnowflakeConnector], scratch_schema: Scratch
) -> None:
    kind, live_connector = state_connector
    state_table = _name(scratch_schema("contract"), f"SST_STATE_{kind.upper()}")
    entry = AppliedEntry(
        "b" * 64,
        _name(scratch_schema("contract"), "V").sql,
        "2026-10-17T00:00:00Z",
        "run",
        "applied",
        "b" * 64,
        "a" * 64,
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
    # The tables are of the kind configured, and a connector configured for the other refuses them.
    other = HYBRID_TABLES if kind == STANDARD_TABLES else STANDARD_TABLES
    kinds = live_connector.query(
        sql(
            "SELECT TABLE_NAME, IS_HYBRID FROM {database}.INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME LIKE %s ORDER BY 1",
            database=ident(state_table.database),
        ),
        (state_table.schema.folded, f"{state_table.name.folded}%"),
    ).rows
    assert [str(row[1]) for row in kinds] == ["YES" if kind == HYBRID_TABLES else "NO"] * 2
    mismatched = live_connector.sibling()
    try:
        mismatched._state_table_kind = other
        with pytest.raises(SnowflakePortError) as refused_kind:
            mismatched.ensure_state_table(state_table)
        assert refused_kind.value.diagnostic is not None and refused_kind.value.diagnostic.code == "SST-APL031"
    finally:
        mismatched.close()


def test_a_procedure_s_drift_digest_is_stable_across_reads_and_a_comment_but_not_execute_as(
    live_connector: SnowflakeConnector, scratch_schema: Scratch
) -> None:
    procedure = _name(scratch_schema("contract"), "SST_CONTRACT_DRIFT")
    created = live_connector.execute_script(
        (
            sql(
                "CREATE OR REPLACE PROCEDURE {p}() RETURNS NUMBER LANGUAGE SQL COMMENT = 'x' "
                "EXECUTE AS CALLER AS 'BEGIN RETURN 1; END;'",
                p=qname(procedure),
            ),
        )
    )
    assert created.ok
    rendered = RenderedArtifact.create(
        key="tool:sst_contract_drift",
        artifact_type="tool",
        target=procedure,
        ddl=sql("CREATE PROCEDURE"),
        shape=PublishShape("PROCEDURE"),
    )
    first = written_digest(live_connector, rendered)
    assert first is not None and first == written_digest(live_connector, rendered)
    commented = live_connector.execute_script((sql("ALTER PROCEDURE {p}() SET COMMENT = 'y'", p=qname(procedure)),))
    assert commented.ok and written_digest(live_connector, rendered) == first
    owner = live_connector.execute_script((sql("ALTER PROCEDURE {p}() EXECUTE AS OWNER", p=qname(procedure)),))
    assert owner.ok
    assert drifted_properties(first, written_digest(live_connector, rendered)) == ("execute_as",)


def test_state_tables_convert_to_hybrid_and_back_keeping_their_rows_and_backups(
    live_account: LiveAccount, scratch_schema: Scratch
) -> None:
    state_table = _name(scratch_schema("contract"), "SST_STATE_CONVERT")
    entry = AppliedEntry(
        "c" * 64,
        _name(scratch_schema("contract"), "V").sql,
        "2026-10-17T00:00:00Z",
        "run",
        "applied",
        "c" * 64,
        "a" * 64,
    )
    standard = SnowflakeConnector(live_account.connection_params())
    try:
        standard.ensure_state_table(state_table)
        taken = standard.acquire_run_lock(
            state_table, "live", LockClaim("run-a", "ROLE", "host", 60), break_stale=False
        )
        assert taken.fence is not None
        with pytest.raises(SnowflakePortError) as live_lock:
            standard.convert_state_tables(state_table, HYBRID_TABLES)
        assert live_lock.value.diagnostic is not None and live_lock.value.diagnostic.code == "SST-APL034"
        write = StateWrite(MappingProxyType({"semantic_view:v": entry}))
        assert standard.write_state(state_table, "live", "m" * 64, write, taken.fence)
        standard.release_run_lock(state_table, "live", taken.fence)

        to_hybrid = standard.convert_state_tables(state_table, HYBRID_TABLES)
        assert (to_hybrid.from_kind, to_hybrid.rows) == (STANDARD_TABLES, 1)
        assert to_hybrid.state_backup.name.value == "SST_STATE_CONVERT_STANDARD_BAK"
    finally:
        standard.close()
    hybrid = SnowflakeConnector({**live_account.connection_params(), TABLE_KIND_SETTING: HYBRID_TABLES})
    try:
        assert hybrid.read_state(state_table, "live") == {"semantic_view:v": entry}
        again = hybrid.acquire_run_lock(state_table, "live", LockClaim("run-b", "ROLE", "host", 60), break_stale=False)
        assert again.acquired and again.fence is not None
        hybrid.release_run_lock(state_table, "live", again.fence)
        back = hybrid.convert_state_tables(state_table, STANDARD_TABLES)
        assert (back.from_kind, back.to_kind, back.rows) == (HYBRID_TABLES, STANDARD_TABLES, 1)
        assert back.state_backup.name.value == "SST_STATE_CONVERT_HYBRID_BAK"
    finally:
        hybrid.close()


def test_a_state_table_written_before_member_fingerprints_gains_the_column_and_keeps_its_rows(
    live_connector: SnowflakeConnector, scratch_schema: Scratch
) -> None:
    state_table = _name(scratch_schema("members"), "SST_STATE_MEMBERS")
    older = AppliedEntry("b" * 64, "DB.S.V", "2026-10-17T00:00:00Z", "run", "applied", "b" * 64, "a" * 64)
    live_connector.ensure_state_table(state_table)
    dropped = live_connector.execute_script(
        (sql("ALTER TABLE {table} DROP COLUMN MEMBER_FINGERPRINTS", table=qname(state_table)),)
    )
    assert dropped.ok, dropped.error
    taken = live_connector.acquire_run_lock(state_table, "live", LockClaim("run-a"), break_stale=False)
    assert taken.acquired and taken.fence is not None
    # A table without the column reads it as none, and keeps the row a write lands.
    assert live_connector.write_state(
        state_table, "live", "m" * 64, StateWrite(MappingProxyType({"semantic_view:v": older})), taken.fence
    )
    assert live_connector.read_state(state_table, "live") == {"semantic_view:v": older}
    # The next run's ensure adds the column, and a fingerprinted entry reads back whole.
    live_connector.ensure_state_table(state_table)
    fingerprinted = replace(older, member_fingerprints=(("metric:orders.total_revenue", "f" * 64),))
    assert live_connector.write_state(
        state_table, "live", "m" * 64, StateWrite(MappingProxyType({"semantic_view:w": fingerprinted})), taken.fence
    )
    assert live_connector.read_state(state_table, "live") == {
        "semantic_view:v": older,
        "semantic_view:w": fingerprinted,
    }
    live_connector.release_run_lock(state_table, "live", taken.fence)
