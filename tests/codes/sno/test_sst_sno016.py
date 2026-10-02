"""SST-SNO016: Snowflake reported semantic views as an unavailable feature."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno016_fires() -> None:
    [diagnostic] = refusal(driver_error("Unsupported feature 'SEMANTIC VIEW'."))
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO016", Severity.ERROR)
    assert diagnostic.message == "semantic views are not enabled on this account"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno016_silent() -> None:
    assert [item.code for item in refusal(driver_error("Unsupported feature 'MATERIALIZED VIEW'."))] == ["SST-SNO001"]
