"""SST-SNO014: the driver could not complete a round trip."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno014_fires() -> None:
    [diagnostic] = refusal(driver_error("Failed to execute request: Read timed out.", sqlstate="08001"))
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO014", Severity.ERROR)
    assert diagnostic.message == "network failure: Failed to execute request: Read timed out."
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno014_silent() -> None:
    assert [
        item.code
        for item in refusal(
            driver_error(
                "Statement reached its statement or warehouse timeout of 600 second(s) and was canceled.",
                errno=630,
                sqlstate="57014",
            )
        )
    ] == ["SST-SNO011"]
