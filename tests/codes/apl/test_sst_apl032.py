"""SST-APL032: what `sst state recover` recorded from ownership markers and content digests."""

from __future__ import annotations

from tests.helpers.recover_changes import proved, recover
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_apl032_fires() -> None:
    [summary] = [item for item in recover(FakeSnowflake(), proved("A")).diagnostics if item.code == "SST-APL032"]
    assert summary.message == (
        "state entries recovered from ownership markers and content digests: 1; unrecorded artifacts left: 0"
    )


def test_sst_apl032_silent() -> None:
    # A recover another run's lock refuses reports the lock, not a summary.
    port = FakeSnowflake()
    from snowflake_semantic_tools.domain.state.lock import LockClaim
    from tests.helpers.recover_changes import TABLE

    assert port.run_locks.acquire_run_lock(
        TABLE, "verify", LockClaim("rival", ttl_seconds=600), break_stale=False
    ).acquired
    assert "SST-APL032" not in [item.code for item in recover(port, proved("A")).diagnostics]
