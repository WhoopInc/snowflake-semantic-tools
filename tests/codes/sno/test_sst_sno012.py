"""SST-SNO012: Snowflake reported 093932: a secure-object share restriction."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno012_fires() -> None:
    [diagnostic] = refusal(
        driver_error("A view or function being shared cannot be non-secure: 'DB.SCHEMA.V'", errno=93932)
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO012", Severity.ERROR)
    assert diagnostic.message == "DB.SCHEMA.V: share restriction"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno012_silent() -> None:
    assert [
        item.code
        for item in refusal(driver_error("A view or function being shared cannot be non-secure: 'DB.SCHEMA.V'"))
    ] == ["SST-SNO001"]
