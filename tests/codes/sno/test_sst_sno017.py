"""SST-SNO017: Snowflake required CREATE AGENT."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno017_fires() -> None:
    [diagnostic] = refusal(
        driver_error("Insufficient privileges: CREATE AGENT privilege required on schema 'DB.SCHEMA'")
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO017", Severity.ERROR)
    assert diagnostic.message == "CREATE AGENT required on DB.SCHEMA"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno017_silent() -> None:
    assert [
        item.code
        for item in refusal(
            driver_error(
                "Insufficient privileges: CREATE AGENT privilege required on schema 'DB.SCHEMA'",
                errno=3001,
                sqlstate="42501",
            )
        )
    ] == ["SST-SNO004"]
