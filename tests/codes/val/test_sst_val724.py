"""SST-VAL724: judge_model set for a system metric.

Fires when a system metric sets judge_model; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import (
    JUDGE,
    codes,
    config,
    only,
    sales_eval,
    system_metric,
    validate,
)


def test_sst_val724_fires() -> None:
    diagnostic = only(
        validate(sales_eval(eval_config=config(system_metrics=(system_metric(judge_model=JUDGE),)))), "SST-VAL724"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "eval config for 'sales': metric 'tool_selection_accuracy' is a system metric and declares judge_model"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val724_silent() -> None:
    assert "SST-VAL724" not in codes(validate())
