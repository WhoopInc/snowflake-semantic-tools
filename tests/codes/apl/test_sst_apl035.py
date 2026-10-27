"""SST-APL035: the account cannot create the hybrid state or lock table `state.+table_kind` names."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state.lock import STANDARD_TABLES, LockClaim
from tests.helpers.hybrid_lock_tables import HybridTables, HybridTablesConnector

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
CLAIM = LockClaim("run-a", "ROLE", "laptop", 60)
# Illustrative: the error of an account without hybrid tables is not measured, only that it names them.
UNAVAILABLE = (1234, "SQL compilation error:\nHybrid tables are not available for this account.\nmore detail")


def _refusing(errno: int, message: str) -> HybridTables:
    tables = HybridTables()
    tables.lock_table_exists = False
    tables.create_fails, tables.create_message = errno, message
    return tables


def test_sst_apl035_fires() -> None:
    with pytest.raises(SnowflakePortError) as raised:
        HybridTablesConnector(_refusing(*UNAVAILABLE)).acquire_run_lock(STATE_TABLE, "dev", CLAIM, break_stale=False)
    diagnostic = raised.value.diagnostic
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-APL035", Severity.ERROR)
    assert diagnostic.message == (
        "DB.S.SST_STATE_LOCK: this account cannot create hybrid tables: "
        "Hybrid tables are not available for this account."
    )
    assert raised.value.errno == 1234


def test_sst_apl035_silent() -> None:
    # A refusal that does not name hybrid tables is reported as Snowflake gave it, and a standard
    # table is never one an account cannot create.
    with pytest.raises(SnowflakePortError) as raised:
        HybridTablesConnector(_refusing(3001, "Insufficient privileges to operate on schema 'S'")).acquire_run_lock(
            STATE_TABLE, "dev", CLAIM, break_stale=False
        )
    assert raised.value.diagnostic is None
    with pytest.raises(SnowflakePortError) as standard:
        HybridTablesConnector(_refusing(*UNAVAILABLE), kind=STANDARD_TABLES).acquire_run_lock(
            STATE_TABLE, "dev", CLAIM, break_stale=False
        )
    assert standard.value.diagnostic is None
