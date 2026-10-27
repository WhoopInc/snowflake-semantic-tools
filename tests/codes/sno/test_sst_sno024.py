"""SST-SNO024: Snowflake queued the statement past the wait cap."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno024_fires() -> None:
    [diagnostic] = refusal(
        driver_error("Statement queued beyond the STATEMENT_QUEUED_TIMEOUT_IN_SECONDS of 300 seconds and was canceled.")
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO024", Severity.ERROR)
    assert diagnostic.message == "query queued beyond 300 seconds"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno024_silent() -> None:
    assert [item.code for item in refusal(driver_error("Statement is queued."))] == ["SST-SNO001"]
