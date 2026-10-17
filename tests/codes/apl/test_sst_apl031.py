"""SST-APL031: the state or lock table is of the other kind than `state.+table_kind` names."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.state.lock import HYBRID_TABLES, STANDARD_TABLES, lock_table_for
from tests.helpers.diagnostic_filters import coded
from tests.helpers.hybrid_lock_tables import HybridTables, HybridTablesConnector
from tests.helpers.plan_codes import plan, view

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
LOCK_TABLE = lock_table_for(STATE_TABLE)


def _read_as(*kinds: str, configured: str = HYBRID_TABLES) -> Preflight:
    """A preflight that read the state and lock tables as `kinds`, with `configured` set."""
    found = tuple(zip((STATE_TABLE, LOCK_TABLE), kinds, strict=False))
    return Preflight("dev", "DEPLOYER", state_table_kind=configured, state_table_kinds=found)


def test_sst_apl031_fires() -> None:
    with pytest.raises(SnowflakePortError) as raised:
        HybridTablesConnector(HybridTables(hybrid=False)).ensure_state_table(STATE_TABLE)
    diagnostic = raised.value.diagnostic
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-APL031", Severity.ERROR)
    assert diagnostic.message == "DB.S.SST_STATE is a standard table, and state.+table_kind says hybrid"


def test_sst_apl031_silent() -> None:
    HybridTablesConnector(HybridTables()).ensure_state_table(STATE_TABLE)


def test_sst_apl031_fires_at_plan_time_for_each_table_of_the_other_kind() -> None:
    planned = plan((view("sales"),), preflight=_read_as(STANDARD_TABLES, STANDARD_TABLES))
    assert [(item.severity, item.message) for item in coded(planned.diagnostics, "SST-APL031")] == [
        (Severity.ERROR, "DB.S.SST_STATE is a standard table, and state.+table_kind says hybrid"),
        (Severity.ERROR, "DB.S.SST_STATE_LOCK is a standard table, and state.+table_kind says hybrid"),
    ]


def test_sst_apl031_silent_at_plan_time() -> None:
    # Tables of the configured kind, a kind that could not be read, and a plan that writes nothing.
    assert coded(plan((view("sales"),), preflight=_read_as(HYBRID_TABLES)).diagnostics, "SST-APL031") == []
    unread = Preflight("dev", "DEPLOYER", state_table_kind=HYBRID_TABLES)
    assert coded(plan((view("sales"),), preflight=unread).diagnostics, "SST-APL031") == []
    assert coded(plan((), preflight=_read_as(STANDARD_TABLES)).diagnostics, "SST-APL031") == []
