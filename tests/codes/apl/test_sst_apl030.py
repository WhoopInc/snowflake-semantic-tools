"""SST-APL030: a hybrid state or lock table is configured in a transient database or schema."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.hybrid_lock_tables import HybridTables, HybridTablesConnector

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
CLAIM = LockClaim("run-a", "ROLE", "laptop", 60)


def test_sst_apl030_fires() -> None:
    tables = HybridTables()
    tables.lock_table_exists = False
    tables.create_fails = 391412
    with pytest.raises(SnowflakePortError) as raised:
        HybridTablesConnector(tables).acquire_run_lock(STATE_TABLE, "dev", CLAIM, break_stale=False)
    diagnostic = raised.value.diagnostic
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-APL030", Severity.ERROR)


def test_sst_apl030_silent() -> None:
    tables = HybridTables()
    tables.lock_table_exists = False
    acquisition = HybridTablesConnector(tables).acquire_run_lock(STATE_TABLE, "dev", CLAIM, break_stale=False)
    assert acquisition.acquired
