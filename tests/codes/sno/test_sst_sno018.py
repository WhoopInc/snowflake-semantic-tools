"""SST-SNO018: Snowflake required CREATE DATASET."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno018_fires() -> None:
    [diagnostic] = refusal(
        driver_error("Insufficient privileges: CREATE DATASET privilege required on schema 'DB.SCHEMA'")
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO018", Severity.ERROR)
    assert diagnostic.message == "CREATE DATASET required on DB.SCHEMA"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno018_silent() -> None:
    assert [
        item.code
        for item in refusal(
            driver_error("Insufficient privileges: CREATE AGENT privilege required on schema 'DB.SCHEMA'")
        )
    ] == ["SST-SNO017"]
