"""SST-VAL722: metric_version is not pinned.

Fires when a system metric pins no version; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalDefaults,
)
from tests.helpers.eval_inputs import (
    codes,
    config,
    only,
    sales_eval,
    system_metric,
    validate,
)


def test_sst_val722_fires() -> None:
    diagnostic = only(
        validate(sales_eval(eval_config=config(system_metrics=(system_metric(version=None),)))), "SST-VAL722"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "eval config for 'sales': metric 'tool_selection_accuracy' declares no metric_version"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val722_silent() -> None:
    assert "SST-VAL722" not in codes(
        validate(
            sales_eval(eval_config=config(system_metrics=(system_metric(version=None),))),
            defaults=EvalDefaults(metric_version="v3"),
        )
    )
