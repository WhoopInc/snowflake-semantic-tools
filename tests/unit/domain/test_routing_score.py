"""`routing_accuracy`, as SST scores a CoWork profile's routing (a Snowflake private preview).

The score is the share of the router's routes that go to a subagent the question accepts; an
empty expectation expects a decline. A question with no expectation, or no trace, fails closed.
"""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.evaluate.routing import (
    expected_subagents,
    resolved_name,
    routing_metric_version,
    routing_result,
    routing_score,
)
from snowflake_semantic_tools.domain.model.eval import ThresholdRange
from tests.helpers.eval_inputs import config, routing_metric

SALES = "DB.AGENTS.SALES"
PARTNER = "PARTNER.AGENTS.DELIVERY"


@pytest.mark.parametrize(
    ("expected", "routed", "score"),
    [
        ((), (), 1.0),
        ((), (SALES,), 0.0),
        ((SALES,), (), 0.0),
        ((SALES,), (SALES,), 1.0),
        ((SALES,), (PARTNER,), 0.0),
        ((SALES,), (SALES, PARTNER), 0.5),
        ((SALES, PARTNER), (PARTNER,), 1.0),
        ((SALES,), (SALES, SALES, PARTNER, PARTNER), 0.5),
    ],
    ids=[
        "decline-declined",
        "decline-routed",
        "route-declined",
        "right-route",
        "wrong-route",
        "one-of-two-wrong",
        "any-of-several",
        "repeats-count",
    ],
)
def test_the_score_is_the_share_of_routes_to_an_accepted_subagent(
    expected: tuple[str, ...], routed: tuple[str, ...], score: float
) -> None:
    assert routing_score(frozenset(expected), routed) == score


def test_names_compare_as_snowflake_resolves_them() -> None:
    assert routing_score(frozenset({"db.agents.sales"}), ("DB.AGENTS.SALES",)) == 1.0
    assert routing_score(frozenset({'DB.AGENTS."sales"'}), ("DB.AGENTS.SALES",)) == 0.0
    assert resolved_name("jaffle_cx.agents.guest") == "JAFFLE_CX.AGENTS.GUEST"
    # A name that is not a qualified name is compared as written.
    assert resolved_name("router") == "router"
    assert routing_score(frozenset({"router"}), ("router",)) == 1.0


def test_the_expected_subagents_are_read_from_the_rendered_ground_truth() -> None:
    assert expected_subagents({"ground_truth_subagents": [SALES, 3, PARTNER]}) == (SALES, PARTNER)
    assert expected_subagents({"ground_truth_subagents": []}) == ()
    assert expected_subagents({"ground_truth_output": "x"}) is None
    assert expected_subagents({"ground_truth_subagents": SALES}) is None
    assert expected_subagents("not a mapping") is None


def test_a_result_passes_within_its_threshold_and_says_what_was_expected_and_chosen() -> None:
    result = routing_result("q", routing_metric(), (SALES, PARTNER), (SALES, PARTNER))
    assert (result.metric_name, result.score, result.passed) == ("routing_accuracy", 1.0, True)
    assert result.threshold == ThresholdRange(min=0.9)
    assert result.explanation == f"expected one of {SALES}, {PARTNER}; the router chose {SALES}, {PARTNER}"

    wrong = routing_result("q", routing_metric(), (SALES,), (PARTNER,))
    assert (wrong.score, wrong.passed) == (0.0, False)
    assert wrong.explanation == f"expected {SALES}; the router chose {PARTNER}"

    declined = routing_result("q", routing_metric(), (), ())
    assert (declined.score, declined.passed) == (1.0, True)
    assert declined.explanation == "expected a decline; the router chose no subagent"


@pytest.mark.parametrize(
    ("threshold", "passed"),
    [(None, True), (ThresholdRange(max=0.4), False), (ThresholdRange(min=0.4, max=0.6), True)],
)
def test_a_threshold_bounds_the_pass_on_either_side(threshold: ThresholdRange | None, passed: bool) -> None:
    metric = routing_metric(gate=threshold is not None, threshold=threshold)
    assert routing_result("q", metric, (SALES,), (SALES, PARTNER)).passed is passed


def test_a_question_with_no_expectation_or_no_trace_fails_closed_with_no_score() -> None:
    unstated = routing_result("q", routing_metric(threshold=None), None, (SALES,))
    assert (unstated.score, unstated.passed) == (None, False)
    assert unstated.explanation == "the question names no ground_truth_subagents, so its route is not scored"
    untraced = routing_result("q", routing_metric(name=None), (SALES,), None)
    assert (untraced.metric_name, untraced.score, untraced.passed) == ("routing_accuracy", None, False)
    assert untraced.explanation == "the run's router spans hold no trace of the question, so its route is unknown"


def test_the_rule_has_a_version_a_baseline_records() -> None:
    assert routing_metric_version("routing_accuracy") == "sst:routing_accuracy:v1"


def test_a_config_finds_its_routing_metric_by_casefolded_name() -> None:
    metric = routing_metric()
    found = config(routing_metrics=(routing_metric(name=None), metric))
    assert found.routing_metric("ROUTING_ACCURACY") is metric
    assert found.routing_metric("answer_correctness") is None
