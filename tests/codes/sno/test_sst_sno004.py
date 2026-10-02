"""SST-SNO004: Snowflake reported 003001: insufficient privileges."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno004_fires() -> None:
    [diagnostic] = refusal(
        driver_error(
            "SQL access control error:\nInsufficient privileges to operate on schema 'SCHEMA'",
            errno=3001,
            sqlstate="42501",
        )
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO004", Severity.ERROR)
    assert diagnostic.message == "insufficient privileges for SCHEMA"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno004_silent() -> None:
    assert [
        item.code
        for item in refusal(
            driver_error(
                "SQL compilation error:\nObject 'DB.SCHEMA.T' does not exist or not authorized.",
                errno=2003,
                sqlstate="02000",
            )
        )
    ] == ["SST-SNO003"]
