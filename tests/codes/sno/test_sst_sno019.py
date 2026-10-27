"""SST-SNO019: Snowflake rejected a repeated synonym."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno019_fires() -> None:
    [diagnostic] = refusal(driver_error("duplicate synonym 'REVENUE' in semantic view"))
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO019", Severity.ERROR)
    assert diagnostic.message == "duplicate synonym REVENUE"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno019_silent() -> None:
    assert [item.code for item in refusal(driver_error("duplicate column name 'REVENUE'"))] == ["SST-SNO001"]
