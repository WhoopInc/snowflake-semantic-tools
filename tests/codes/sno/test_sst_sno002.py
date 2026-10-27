"""SST-SNO002: Snowflake reported 002002: the object already exists."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno002_fires() -> None:
    [diagnostic] = refusal(
        driver_error("SQL compilation error:\nObject 'DB.SCHEMA.V' already exists.", errno=2002, sqlstate="42710")
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO002", Severity.ERROR)
    assert diagnostic.message == "DB.SCHEMA.V already exists"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno002_silent() -> None:
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
