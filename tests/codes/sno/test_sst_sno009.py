"""SST-SNO009: Snowflake reported 001003: a SQL compilation error."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno009_fires() -> None:
    [diagnostic] = refusal(
        driver_error(
            "SQL compilation error:\nsyntax error line 1 at position 7 unexpected 'VIEW'.", errno=1003, sqlstate="42000"
        )
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO009", Severity.ERROR)
    assert diagnostic.message == "SQL compilation error: syntax error line 1 at position 7 unexpected 'VIEW'."
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno009_silent() -> None:
    assert [
        item.code
        for item in refusal(
            driver_error("SQL compilation error:\nObject 'DB.SCHEMA.V' already exists.", errno=2002, sqlstate="42710")
        )
    ] == ["SST-SNO002"]
