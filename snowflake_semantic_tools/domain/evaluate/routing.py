"""Score `routing_accuracy`: whether a CoWork profile's router chose a subagent the question accepts.

CoWork profiles are a Snowflake private preview (PrPr) feature; a profile's eval follows the
preview's contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

SST scores this metric itself, with no judge, from the subagents the run's router spans name.
Snowflake's own tool metrics never see a router call. For one question, E is the set of
subagents its ground truth accepts and A the subagents the router chose, one per routing call:

1. An empty E expects a decline: 1.0 when A is empty, else 0.0.
2. Otherwise 0.0 when A is empty, else the routes in A that are in E over all the routes in A.

Names compare as Snowflake resolves them, so the uppercase name a router span gives and the
name the dataset rendered match whatever their spelling. A name that does not parse as a
qualified name is compared as written.

A question that names no `ground_truth_subagents`, or whose trace the run's spans do not
hold, is not scored, and fails: the metric fails closed, as a Snowflake metric with no score
does.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.domain.model.eval.model import ROUTING_ACCURACY, EvalRoutingMetric, ThresholdRange
from snowflake_semantic_tools.domain.model.eval.results import EvalMetricResult
from snowflake_semantic_tools.domain.model.identifier import QualifiedName

# The ground-truth key a row's accepted subagents render under.
GROUND_TRUTH_SUBAGENTS = "ground_truth_subagents"
# The version of the module's rule; changing the rule moves it, so a baseline scored under one
# rule never gates a run scored under another.
ROUTING_RULE_VERSION = "v1"


def routing_metric_version(name: str) -> str:
    """Name the version a baseline records for a routing metric: `sst:<name>:<rule version>`."""
    return f"sst:{name}:{ROUTING_RULE_VERSION}"


def expected_subagents(ground_truth: object) -> tuple[str, ...] | None:
    """Return the qualified names a question's `ground_truth_subagents` accepts, in order.

    Returns:
        The names; `()` for an empty list, which expects a decline; None when the ground truth
        has no `ground_truth_subagents` list, which states no expectation. An entry that is not
        text names nothing.
    """
    listed = ground_truth.get(GROUND_TRUTH_SUBAGENTS) if isinstance(ground_truth, Mapping) else None
    if not isinstance(listed, list):
        return None
    return tuple(name for name in listed if isinstance(name, str))


def resolved_name(name: str) -> str:
    """Spell a subagent's name as Snowflake resolves it: `DB.SCHEMA.NAME`, else as written.

    Example:
        `jaffle_cx.agents.guest` and `JAFFLE_CX.AGENTS.GUEST` both read `JAFFLE_CX.AGENTS.GUEST`.
    """
    try:
        return ".".join(QualifiedName.parse(name).folded)
    except ValueError:
        return name


def routing_score(expected: frozenset[str], routed: tuple[str, ...]) -> float:
    """Score one question's routes against the subagents it accepts, by the module's rule.

    Args:
        expected: The qualified names the question accepts; empty expects a decline.
        routed: The subagent of each routing call the router made, repeats included.

    Returns:
        A score in [0, 1].
    """
    if not routed:
        return 0.0 if expected else 1.0
    if not expected:
        return 0.0
    accepted = {resolved_name(name) for name in expected}
    return sum(resolved_name(name) in accepted for name in routed) / len(routed)


def routing_result(
    question_key: str,
    metric: EvalRoutingMetric,
    expected: tuple[str, ...] | None,
    routed: tuple[str, ...] | None,
) -> EvalMetricResult:
    """Score one question's routing, and say what was expected and chosen.

    It passes when its score falls within the metric's threshold, or has one when the metric
    has none. A question that states no expectation, or whose routes are unknown, has no score
    and fails.

    Args:
        expected: The names `expected_subagents` returns for the question.
        routed: The subagents the router chose, as the run's spans name them; None when the
            spans hold no trace of the question.
    """
    name = metric.name or ROUTING_ACCURACY
    if expected is None:
        why = "the question names no ground_truth_subagents, so its route is not scored"
        return EvalMetricResult(question_key, name, None, False, why, metric.threshold)
    if routed is None:
        why = "the run's router spans hold no trace of the question, so its route is unknown"
        return EvalMetricResult(question_key, name, None, False, why, metric.threshold)
    score = routing_score(frozenset(expected), routed)
    explanation = f"expected {_expected_text(expected)}; the router chose {_routed_text(routed)}"
    return EvalMetricResult(question_key, name, score, _within(score, metric.threshold), explanation, metric.threshold)


def _within(score: float, threshold: ThresholdRange | None) -> bool:
    """Report whether a score falls within a threshold; any score does without one."""
    if threshold is None:
        return True
    return (threshold.min is None or score >= threshold.min) and (threshold.max is None or score <= threshold.max)


def _expected_text(expected: tuple[str, ...]) -> str:
    if not expected:
        return "a decline"
    return expected[0] if len(expected) == 1 else "one of " + ", ".join(expected)


def _routed_text(routed: tuple[str, ...]) -> str:
    return ", ".join(routed) if routed else "no subagent"
