"""SST-APL031: the state or lock table is of the other kind than `state.+table_kind` names."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.hybrid_lock_tables import HybridTables, HybridTablesConnector

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")


def test_sst_apl031_fires() -> None:
    with pytest.raises(SnowflakePortError) as raised:
        HybridTablesConnector(HybridTables(hybrid=False)).ensure_state_table(STATE_TABLE)
    diagnostic = raised.value.diagnostic
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-APL031", Severity.ERROR)
    assert diagnostic.message == "DB.S.SST_STATE is a standard table, and state.+table_kind says hybrid"


def test_sst_apl031_silent() -> None:
    HybridTablesConnector(HybridTables()).ensure_state_table(STATE_TABLE)
