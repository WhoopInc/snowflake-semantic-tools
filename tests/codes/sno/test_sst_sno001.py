"""SST-SNO001: Snowflake refused and no signature matched the driver error."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno001_fires() -> None:
    [diagnostic] = refusal(driver_error("The service is in an unexpected state"))
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO001", Severity.ERROR)
    assert diagnostic.message == "Snowflake refused: The service is in an unexpected state"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno001_silent() -> None:
    assert [
        item.code
        for item in refusal(
            driver_error("SQL compilation error:\nObject 'DB.SCHEMA.V' already exists.", errno=2002, sqlstate="42710")
        )
    ] == ["SST-SNO002"]
