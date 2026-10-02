"""SST-VAL733: blocking threshold declares no usable bound.

Fires when a gated system metric has inverted bounds; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import (
    ThresholdRange,
)
from tests.helpers.eval_inputs import (
    codes,
    config,
    only,
    sales_eval,
    system_metric,
    validate,
)


def test_sst_val733_fires() -> None:
    diagnostic = only(
        validate(
            sales_eval(eval_config=config(system_metrics=(system_metric(threshold=ThresholdRange(min=0.9, max=0.2)),)))
        ),
        "SST-VAL733",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "eval config for 'sales': metric 'tool_selection_accuracy' threshold has inverted bounds"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val733_silent() -> None:
    assert "SST-VAL733" not in codes(
        validate(
            sales_eval(eval_config=config(system_metrics=(system_metric(threshold=ThresholdRange(min=0.2, max=0.9)),)))
        )
    )
