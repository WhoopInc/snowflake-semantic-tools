"""SST-APL010: apply took over a run lock its holder left expired."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.apply_runs import STATE_TABLE, apply_plan, codes, only
from tests.helpers.artifact_builders import change, changeset, rendered
from tests.helpers.snowflake_fake import FakeSnowflake


def expired_lock() -> FakeSnowflake:
    port = FakeSnowflake()
    port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim("crashed", "ROLE", "ci", 10), break_stale=False)
    port.run_locks.now = 60.0
    return port


def test_sst_apl010_fires() -> None:
    result = apply_plan(changeset(change(rendered())), expired_lock(), options=ApplyOptions(break_stale_lock=True))
    diagnostic = only(result, "SST-APL010")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "broke a stale lock held by run crashed (ROLE on ci), expired 10.0"
    assert result.success


def test_sst_apl010_silent() -> None:
    assert "SST-APL010" not in codes(apply_plan(changeset(change(rendered()))))
