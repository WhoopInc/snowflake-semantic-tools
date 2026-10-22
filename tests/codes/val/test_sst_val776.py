"""SST-VAL776: a CoWork profile's comparison cost is its router's alone.

Fires once for a profile's comparison; an agent's stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.evaluate.compare import CANDIDATE, PINNED
from snowflake_semantic_tools.domain.evaluate.compare_notices import comparison_notices
from tests.helpers.compare_reports import compared, credits, model, run


def notices(kind: str) -> list[str]:
    evaluation = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("opus", CANDIDATE, run("o1", 1.0)),
        kind=kind,
        costs={"pinned": credits(1.0), "opus": credits(2.0)},
    )
    return [item.code for item in comparison_notices(evaluation, path="c.json")]


def test_sst_val776_fires() -> None:
    evaluation = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("opus", CANDIDATE, run("o1", 1.0)),
        kind="cowork_profile",
        costs={"pinned": credits(1.0), "opus": credits(2.0)},
    )
    [diagnostic] = comparison_notices(evaluation, path="c.json")
    assert (diagnostic.code, diagnostic.severity) == ("SST-VAL776", Severity.INFO)
    assert diagnostic.message == (
        "eval 'sales': the CoWork profile's cost is its router's requests alone; the subagent runs "
        "they routed to are not attributed"
    )
    assert diagnostic.subject == "eval:cowork_profile:sales"


def test_sst_val776_silent() -> None:
    assert notices("agent") == []
