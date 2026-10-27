"""SST-VAL775: a comparison candidate is not generally available, so no verdict names it.

Fires for a preview candidate; a generally available one stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.evaluate.compare import CANDIDATE, PINNED
from snowflake_semantic_tools.domain.evaluate.compare_notices import comparison_notices
from tests.helpers.compare_reports import compared, credits, model, run


def notices(lifecycle: str) -> list[str]:
    evaluation = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("opus", CANDIDATE, run("o1", 1.0), lifecycle=lifecycle),
        costs={"pinned": credits(1.0), "opus": credits(2.0)},
    )
    return [item.message for item in comparison_notices(evaluation, path="c.json") if item.code == "SST-VAL775"]


def test_sst_val775_fires() -> None:
    evaluation = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("opus", CANDIDATE, run("o1", 1.0), lifecycle="preview"),
        costs={"pinned": credits(1.0), "opus": credits(2.0)},
    )
    [diagnostic] = comparison_notices(evaluation, path="c.json")
    assert (diagnostic.code, diagnostic.severity) == ("SST-VAL775", Severity.INFO)
    assert (
        diagnostic.message == "eval 'sales': candidate 'opus' is preview, so the verdict reports it but never names it"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val775_silent() -> None:
    assert notices("ga") == []
