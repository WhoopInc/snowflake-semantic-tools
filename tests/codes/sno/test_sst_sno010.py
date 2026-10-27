"""SST-SNO010: Snowflake reported an internal error, with an incident number."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.sno_codes import driver_error, refusal


def test_sst_sno010_fires() -> None:
    [diagnostic] = refusal(
        driver_error(
            "SQL execution internal error:\nProcessing aborted due to error 300002:2523941447; incident 5381865."
        )
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO010", Severity.ERROR)
    assert (
        diagnostic.message
        == "SQL execution internal error: Processing aborted due to error 300002:2523941447; incident 5381865."
    )
    assert diagnostic.subject == "semantic_view:v"


def test_sst_sno010_silent() -> None:
    assert [
        item.code
        for item in refusal(
            driver_error(
                "SQL compilation error:\nsyntax error line 1 at position 7 unexpected 'VIEW'.",
                errno=1003,
                sqlstate="42000",
            )
        )
    ] == ["SST-SNO009"]
