"""SST-SNO020: Snowflake rejected an over-length identifier."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno020_fires() -> None:
    [diagnostic] = refusal(driver_error("identifier 'ORDERS_BY_REGION_AND_MONTH' is too long"))
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO020", Severity.ERROR)
    assert diagnostic.message == "identifier ORDERS_BY_REGION_AND_MONTH is too long"
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno020_silent() -> None:
    assert [item.code for item in refusal(driver_error("identifier 'ORDERS_BY_REGION_AND_MONTH' is invalid"))] == [
        "SST-SNO001"
    ]
