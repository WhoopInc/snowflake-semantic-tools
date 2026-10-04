"""SST-APL011: another run holds the target's apply lock."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.apply_runs import STATE_TABLE, apply_plan
from tests.helpers.artifact_builders import change, changeset, rendered
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_apl011_fires() -> None:
    port = FakeSnowflake()
    port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim("other", "ROLE", "laptop", 60), break_stale=False)
    result = apply_plan(changeset(change(rendered())), port)
    diagnostic = only(result.diagnostics, "SST-APL011")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "run other (ROLE on laptop), expires 60.0 holds the apply lock"
    assert port.scripts == [] and not result.state_written


def test_sst_apl011_silent() -> None:
    port = FakeSnowflake()
    assert "SST-APL011" not in codes(apply_plan(changeset(change(rendered())), port).diagnostics)
    assert port.run_locks.rows == {}
