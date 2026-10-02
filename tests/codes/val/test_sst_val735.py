"""SST-VAL735: threshold set with no baseline.

Fires when a threshold is set with no baseline runs; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import (
    codes,
    config,
    only,
    run_config,
    sales_eval,
    validate,
)


def test_sst_val735_fires() -> None:
    diagnostic = only(validate(sales_eval(eval_config=config(run=run_config(baseline_runs=0)))), "SST-VAL735")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "eval config for 'sales': metric 'tool_selection_accuracy' has a threshold and no baseline"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val735_silent() -> None:
    assert "SST-VAL735" not in codes(validate())
