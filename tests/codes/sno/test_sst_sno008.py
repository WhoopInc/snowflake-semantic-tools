"""SST-SNO008: Snowflake reported the session has no active warehouse."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno008_fires() -> None:
    [diagnostic] = refusal(
        driver_error(
            "No active warehouse selected in the current session.  "
            "Select an active warehouse with the 'use warehouse' command."
        )
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO008", Severity.ERROR)
    assert diagnostic.message == "no active warehouse in the session"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno008_silent() -> None:
    assert [
        item.code for item in refusal(driver_error("Warehouse 'REPORTING_WH' does not exist or not authorized."))
    ] == ["SST-SNO007"]
