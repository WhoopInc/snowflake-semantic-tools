"""What a comparison notices beyond its report: pending and unavailable cost, excluded candidates, router-only cost."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.evaluate.compare import (
    CANDIDATE,
    PENDING,
    PINNED,
    UNAVAILABLE,
    Amount,
    ModelCost,
    unavailable_reason,
)
from snowflake_semantic_tools.domain.evaluate.compare_notices import comparison_notices
from tests.helpers.compare_reports import compared, credits, model, run


def test_each_notice_is_reported_once_per_eval_but_excluded_candidates_one_each() -> None:
    evaluation = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("preview", CANDIDATE, run("x1", 1.0), lifecycle="preview"),
        model("legacy", CANDIDATE, run("l1", 1.0), lifecycle="deprecated"),
        kind="cowork_profile",
        costs={"pinned": credits(None, PENDING), "preview": credits(None, PENDING)},
    )
    found = comparison_notices(evaluation, path="sst_target/compare/c.json")
    assert [item.code for item in found] == ["SST-VAL773", "SST-VAL774", "SST-VAL775", "SST-VAL775", "SST-VAL776"]
    assert found[0].message.startswith("eval 'sales': the usage of 2 model(s)' runs")
    assert {item.subject for item in found} == {"eval:cowork_profile:sales"}
    assert found[1].message.endswith("is unavailable: no usage was read")


def test_an_unavailable_amount_read_back_from_a_report_has_a_reason_of_its_own() -> None:
    evaluation = compared(model("pinned", PINNED, run("p1", 1.0)), model("opus", CANDIDATE, run("o1", 1.0)))
    unreasoned = Amount(UNAVAILABLE)
    bare = replace(
        evaluation,
        models=tuple(
            replace(item, cost=ModelCost(unreasoned, unreasoned, unreasoned, unreasoned)) for item in evaluation.models
        ),
    )
    assert unavailable_reason(bare) == "nothing attributes the usage"


def test_an_agents_comparison_whose_cost_is_known_notices_nothing() -> None:
    evaluation = compared(
        model("pinned", PINNED, run("p1", 1.0)),
        model("opus", CANDIDATE, run("o1", 1.0)),
        costs={"pinned": credits(1.0), "opus": credits(2.0)},
    )
    assert comparison_notices(evaluation, path="c.json") == ()
