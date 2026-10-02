"""SST-SNO003: Snowflake reported 002003: an object does not exist or is not authorised."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno003_fires() -> None:
    [diagnostic] = refusal(
        driver_error(
            "SQL compilation error:\nObject 'DB.SCHEMA.T' does not exist or not authorized.",
            errno=2003,
            sqlstate="02000",
        )
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO003", Severity.ERROR)
    assert diagnostic.message == "DB.SCHEMA.T does not exist or is not authorised"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno003_silent() -> None:
    assert [
        item.code
        for item in refusal(
            driver_error(
                "SQL compilation error:\nSchema 'DB.SCHEMA' does not exist or not authorized.",
                errno=2043,
                sqlstate="02000",
            )
        )
    ] == ["SST-SNO005"]
