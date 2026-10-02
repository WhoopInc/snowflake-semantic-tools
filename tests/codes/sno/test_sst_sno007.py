"""SST-SNO007: Snowflake reported, in words only, that the warehouse does not exist."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno007_fires() -> None:
    [diagnostic] = refusal(driver_error("Warehouse 'REPORTING_WH' does not exist or not authorized."))
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO007", Severity.ERROR)
    assert diagnostic.message == "warehouse REPORTING_WH does not exist or is not authorised"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno007_silent() -> None:
    assert [item.code for item in refusal(driver_error("Database 'DB' does not exist or not authorized."))] == [
        "SST-SNO006"
    ]
