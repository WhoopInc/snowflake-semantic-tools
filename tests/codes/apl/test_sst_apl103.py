"""SST-APL103: Snowflake ended a session apply was running on, after the run took its lock."""

from __future__ import annotations

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ApplyResult
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.apply_runs import STATE_TABLE
from tests.helpers.artifact_builders import change, changeset, rendered, state
from tests.helpers.clocks import FixedClock
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.dropped_sessions import DroppedSession, Reconnects
from tests.helpers.snowflake_fake import FakeSnowflake


def _apply(port: FakeSnowflake) -> tuple[ApplyResult, Reconnects]:
    reconnect = Reconnects(port)
    plan = changeset(change(rendered("A")), change(rendered("B")))
    applied = ApplyArtifacts(port, InMemoryStateStore(), FixedClock(), state_table=STATE_TABLE, reconnect=reconnect)
    return applied.run(plan, state()), reconnect


def test_sst_apl103_fires() -> None:
    port = DroppedSession(scripts=1)
    result, reconnect = _apply(port)
    diagnostic = only(result.diagnostics, "SST-APL103")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "the connection to Snowflake was lost mid-run; finished before it: semantic_view:a"
    assert reconnect.opened == 1 and port.run_locks.rows == {} and result.state_written


def test_sst_apl103_silent() -> None:
    port = FakeSnowflake()
    result, reconnect = _apply(port)
    assert "SST-APL103" not in codes(result.diagnostics) and reconnect.opened == 0
