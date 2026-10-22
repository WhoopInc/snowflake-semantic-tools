"""SST-VAL774: some model's comparison cost is unavailable.

Fires for a comparison whose usage was not read; one whose cost is all known stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.evaluate.compare import CANDIDATE, PINNED, UNAVAILABLE, Amount, ModelCost
from snowflake_semantic_tools.domain.evaluate.compare_notices import comparison_notices
from tests.helpers.compare_reports import compared, credits, model, run


def test_sst_val774_fires() -> None:
    refused = Amount(UNAVAILABLE, reason="Insufficient privileges to operate on ACCOUNT_USAGE")
    evaluation = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("opus", CANDIDATE, run("o1", 1.0)),
        costs={"pinned": ModelCost(refused, refused, refused), "opus": ModelCost(refused, refused, refused)},
    )
    [diagnostic] = comparison_notices(evaluation, path="c.json")
    assert (diagnostic.code, diagnostic.severity) == ("SST-VAL774", Severity.INFO)
    assert diagnostic.message == (
        "eval 'sales': the cost of 2 model(s)' runs is unavailable: Insufficient privileges to operate on ACCOUNT_USAGE"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val774_silent() -> None:
    evaluation = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("opus", CANDIDATE, run("o1", 1.0)),
        costs={"pinned": credits(1.0), "opus": credits(2.0)},
    )
    assert "SST-VAL774" not in [item.code for item in comparison_notices(evaluation, path="c.json")]
