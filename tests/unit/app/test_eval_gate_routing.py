"""`routing_accuracy` in the gate and its baselines: a third metric kind beside system and custom.

CoWork profiles are a Snowflake private preview. SST scores routing itself, so its version is
the rule's (`sst:routing_accuracy:v1`), its gate and threshold come from `metrics.routing`, and
a change to either is a change to the eval's metric identity, though the config Snowflake runs
does not carry them.
"""

from __future__ import annotations

import ast
import inspect
from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals import gate, retrieve
from snowflake_semantic_tools.app.evals.gate import capture_baseline, evaluate_gate, gate_refusal
from snowflake_semantic_tools.app.evals.run import ResolvedAgent
from snowflake_semantic_tools.domain.model.eval import EvalBaselineRecord, ThresholdRange
from tests.helpers.eval_builders import attempt, profile_eval_of, run_result
from tests.helpers.eval_inputs import concierge_eval, routing_metric, settled

ROUTING = "routing_accuracy"
NOW = "2026-09-10T00:00:00Z"
# The functions of the gate and of retrieval that read a config's system metrics but rightly
# leave routing out, and why.
_NOT_ROUTING = {
    "recorded_judges": "SST-VAL746 reports each system metric's judge version; routing has no judge",
    "_expected_metrics": "the metrics Snowflake's results carry; SST scores routing itself",
}


def _profile_eval(**metric_changes: object) -> CompiledEval:
    """The profile eval, blocking with two baseline runs, its routing metric changed as given."""
    compiled = profile_eval_of(concierge_eval(routing_metrics=(routing_metric(**metric_changes),)))
    return replace(compiled, resolved=settled(compiled.resolved, baseline_runs=2, eval_tier="blocking"))


def _baseline(compiled: CompiledEval) -> EvalBaselineRecord:
    flags = (("q", "answer_correctness", True), ("q", ROUTING, True))
    return capture_baseline(
        compiled,
        run_result(attempt("run-1", *flags), attempt("run-2", *flags)),
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
    )


def _helpers() -> dict[str, str]:
    """Every function of the gate and of retrieval that reads a config's system metrics, by name."""
    found: dict[str, str] = {}
    for module in (gate, retrieve):
        source = inspect.getsource(module)
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.FunctionDef):
                text = ast.get_source_segment(source, node) or ""
                if "system_metrics" in text:
                    found[node.name] = text
    return found


def test_every_helper_that_reads_system_metrics_reads_routing_metrics_too() -> None:
    helpers = _helpers()
    assert set(_NOT_ROUTING) <= set(helpers)
    missing = sorted(
        name for name, text in helpers.items() if name not in _NOT_ROUTING and "routing_metric" not in text
    )
    assert missing == []


def test_every_gate_and_baseline_helper_covers_the_routing_metric() -> None:
    compiled = _profile_eval()

    assert dict(gate._metric_versions(compiled))[ROUTING] == "sst:routing_accuracy:v1"
    assert (ROUTING, (0.9, None), True) in gate._metric_policy(compiled)
    assert (ROUTING, True) in gate._gate_policy(compiled)
    assert gate._score_range(compiled, "ROUTING_ACCURACY") == (0.9, None)
    assert gate._is_gated_metric(compiled, ROUTING)
    assert not gate._is_gated_metric(_profile_eval(gate=False, threshold=None), ROUTING)
    assert gate._score_range(_profile_eval(gate=False, threshold=None), ROUTING) is None
    assert retrieve._metric_threshold(compiled, ROUTING) == ThresholdRange(min=0.9)
    assert set(_NOT_ROUTING) == {"recorded_judges", "_expected_metrics"}
    assert ROUTING not in retrieve._expected_metrics(compiled)
    assert [item.context["artifact"] for item in gate.recorded_judges(compiled)] == ["answer_correctness"]


def test_a_baseline_records_the_routing_rule_its_threshold_and_its_gate() -> None:
    baseline = _baseline(_profile_eval())

    assert (ROUTING, "sst:routing_accuracy:v1") in baseline.metric_versions
    assert (ROUTING, True) in baseline.gate_policy
    [routing] = [item for item in baseline.metrics if item.metric_name == ROUTING]
    assert (routing.passed_attempts, routing.score_range) == ((True, True), (0.9, None))


def test_a_gated_routing_metric_that_fails_a_question_regresses() -> None:
    compiled = _profile_eval()
    current = run_result(attempt("run-3", ("q", "answer_correctness", True), ("q", ROUTING, False)))

    verdict, diagnostics = evaluate_gate(compiled, current, _baseline(compiled), now=NOW)

    assert not verdict.passed
    assert [(item.question_key, item.metric_name) for item in verdict.regressions] == [("q", ROUTING)]
    assert [(item.code, item.message) for item in diagnostics] == [
        ("SST-VAL763", "eval 'eval:cowork_profile:concierge' regressed on 1 question/metric pair(s): routing_accuracy")
    ]


def test_an_ungated_routing_metric_never_regresses() -> None:
    compiled = _profile_eval(gate=False, threshold=None)
    current = run_result(attempt("run-3", ("q", "answer_correctness", True), ("q", ROUTING, False)))

    verdict, _ = evaluate_gate(compiled, current, _baseline(compiled), now=NOW)

    assert verdict.passed and verdict.regressions == ()


@pytest.mark.parametrize(
    ("changes", "detail"),
    [
        ({"threshold": ThresholdRange(min=0.8)}, "metric 'routing_accuracy' changed its threshold"),
        ({"gate": False, "threshold": None}, "metric 'routing_accuracy' changed its threshold"),
    ],
    ids=["threshold", "gate"],
)
def test_a_routing_policy_change_makes_the_baseline_incompatible(changes: dict[str, object], detail: str) -> None:
    baseline = _baseline(_profile_eval())
    edited = _profile_eval(**changes)
    # The config Snowflake runs does not carry routing, so the policy alone tells the change.
    assert edited.rendered.config_fingerprint == _profile_eval().rendered.config_fingerprint

    refusal = gate_refusal(edited, baseline, ResolvedAgent("VERSION$1"), now=NOW)

    assert refusal is not None
    verdict, diagnostics = refusal
    assert verdict.reason == "baseline_incompatible"
    [incompatible] = diagnostics
    assert incompatible.code == "SST-VAL759" and detail in incompatible.message


def test_a_gating_change_alone_is_named() -> None:
    baseline = _baseline(_profile_eval())
    ungated = _profile_eval(gate=False)

    refusal = gate_refusal(ungated, baseline, ResolvedAgent("VERSION$1"), now=NOW)

    assert refusal is not None
    assert "metric 'routing_accuracy' changed its gating" in refusal[1][0].message


def test_a_baseline_scored_under_another_routing_rule_is_incompatible_not_an_edited_judge() -> None:
    compiled = _profile_eval()
    baseline = _baseline(compiled)
    versions = tuple(
        (name, "sst:routing_accuracy:v0" if name == ROUTING else version) for name, version in baseline.metric_versions
    )

    refusal = gate_refusal(compiled, replace(baseline, metric_versions=versions), ResolvedAgent("VERSION$1"), now=NOW)

    assert refusal is not None
    [incompatible] = refusal[1]
    assert incompatible.code == "SST-VAL759"
    assert "metric 'routing_accuracy' moved from version sst:routing_accuracy:v0 to sst:routing_accuracy:v1" in (
        incompatible.message
    )


def test_a_routing_metric_added_to_an_eval_makes_its_baseline_incompatible() -> None:
    without = profile_eval_of(concierge_eval(routing_metrics=()))
    without = replace(without, resolved=settled(without.resolved, baseline_runs=2, eval_tier="blocking"))
    flags = (("q", "answer_correctness", True),)
    baseline = capture_baseline(
        without,
        run_result(attempt("run-1", *flags), attempt("run-2", *flags)),
        reason="initial",
        captured_at="2026-09-01T00:00:00Z",
    )

    refusal = gate_refusal(_profile_eval(), baseline, ResolvedAgent("VERSION$1"), now=NOW)

    assert refusal is not None
    assert "metric 'routing_accuracy' was added" in refusal[1][0].message
