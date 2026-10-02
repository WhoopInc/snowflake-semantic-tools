"""SST-VAL726: logical_consistency used as a gate.

Fires when logical_consistency gates; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import (
    codes,
    config,
    only,
    sales_eval,
    system_metric,
    validate,
)


def test_sst_val726_fires() -> None:
    diagnostic = only(
        validate(sales_eval(eval_config=config(system_metrics=(system_metric("logical_consistency"),)))), "SST-VAL726"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "eval config for 'sales': logical_consistency is a blocking metric"
    assert diagnostic.subject == "eval:sales"


def test_sst_val726_silent() -> None:
    assert "SST-VAL726" not in codes(
        validate(
            sales_eval(
                eval_config=config(system_metrics=(system_metric("logical_consistency", gate=False, threshold=None),))
            )
        )
    )
