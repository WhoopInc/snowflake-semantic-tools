"""SST-APL033: an artifact `sst state recover` could not record from a marker."""

from __future__ import annotations

from tests.helpers.recover_changes import proved, recover, unmanaged
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_apl033_fires() -> None:
    [item] = [item for item in recover(FakeSnowflake(), unmanaged("B", None)).diagnostics if item.code == "SST-APL033"]
    assert item.message == (
        "semantic_view:b: not recovered (no marker); the next apply is blocked on it until it is dropped or renamed"
    )


def test_sst_apl033_silent() -> None:
    assert "SST-APL033" not in [item.code for item in recover(FakeSnowflake(), proved("A")).diagnostics]
