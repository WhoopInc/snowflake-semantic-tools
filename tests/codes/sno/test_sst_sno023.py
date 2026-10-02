"""SST-SNO023: Snowflake refused to return a result."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno023_fires() -> None:
    [diagnostic] = refusal(driver_error("Result set too large for 'DB.SCHEMA.V'."))
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO023", Severity.ERROR)
    assert diagnostic.message == "result set too large for DB.SCHEMA.V"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno023_silent() -> None:
    assert [item.code for item in refusal(driver_error("Result set is empty for 'DB.SCHEMA.V'."))] == ["SST-SNO001"]
