"""SST-VAL773: some model's comparison cost has not landed in the usage views yet.

Fires for a comparison one of whose amounts is pending; one whose cost is all known stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.evaluate.compare import CANDIDATE, PENDING, PINNED
from snowflake_semantic_tools.domain.evaluate.compare_notices import comparison_notices
from tests.helpers.compare_reports import compared, credits, model, run


def test_sst_val773_fires() -> None:
    evaluation = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("opus", CANDIDATE, run("o1", 1.0)),
        costs={"pinned": credits(1.0), "opus": credits(None, PENDING)},
    )
    [diagnostic] = [item for item in comparison_notices(evaluation, path="c.json") if item.code == "SST-VAL773"]
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "eval 'sales': the usage of 1 model(s)' runs has not landed in Snowflake's usage views yet, "
        "so their cost is pending"
    )
    assert diagnostic.suggestion == "run `sst eval --refresh-cost c.json` in about an hour to fill it in"
    assert diagnostic.subject == "eval:sales"


def test_sst_val773_silent() -> None:
    evaluation = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("opus", CANDIDATE, run("o1", 1.0)),
        costs={"pinned": credits(1.0), "opus": credits(2.0)},
    )
    assert comparison_notices(evaluation, path="c.json") == ()
