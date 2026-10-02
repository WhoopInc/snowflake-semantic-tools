"""SST-SNO015: Snowflake rejected the max_staleness value."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno015_fires() -> None:
    [diagnostic] = refusal(driver_error("invalid value for MAX_STALENESS: '60 seconds'"))
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO015", Severity.ERROR)
    assert diagnostic.message == "max_staleness 60 seconds rejected"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno015_silent() -> None:
    assert [item.code for item in refusal(driver_error("invalid value for TARGET_LAG: '60 seconds'"))] == ["SST-SNO001"]
