"""SST-SNO022: Snowflake reported a lock or concurrency failure."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno022_fires() -> None:
    [diagnostic] = refusal(
        driver_error(
            "Your statement was aborted because the number of waiters for this lock "
            "exceeds the 20 statements limit on 'DB.SCHEMA.V'."
        )
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO022", Severity.ERROR)
    assert diagnostic.message == "lock timeout on DB.SCHEMA.V"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno022_silent() -> None:
    assert [item.code for item in refusal(driver_error("Object 'DB.SCHEMA.V' is locked."))] == ["SST-SNO001"]
