"""SST-VAL888: a CoWork profile's eval leaves Snowflake no metric.

A CoWork profile's eval (a Snowflake private preview) whose only system metrics were the
inherited tool metrics left out is refused, saying what to add; one that keeps
answer_correctness is not.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import concierge_eval, system_metric, validate


def test_sst_val888_fires() -> None:
    blind = tuple(
        system_metric(name, gate=False, threshold=None)
        for name in ("tool_selection_accuracy", "tool_execution_accuracy")
    )
    found = validate(concierge_eval(system_metrics=(), inherited_blind_metrics=blind))
    diagnostic = only(found, "SST-VAL888")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "eval config for 'concierge': with tool_selection_accuracy and tool_execution_accuracy left out, "
        "Snowflake has no metric to score"
    )
    assert (
        diagnostic.suggestion
        == "add answer_correctness or logical_consistency under metrics.system, or a custom metric"
    )
    assert diagnostic.subject == "eval:cowork_profile:concierge"
    # It says why the config is empty, in place of the generic refusal.
    assert "SST-PRS101" not in codes(found)


def test_sst_val888_silent() -> None:
    blind = (system_metric("tool_selection_accuracy", gate=False, threshold=None),)
    found = validate(concierge_eval(inherited_blind_metrics=blind))
    assert "SST-VAL888" not in codes(found)
    assert "SST-PRS101" in codes(validate(concierge_eval(system_metrics=())))
