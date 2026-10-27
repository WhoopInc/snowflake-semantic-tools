"""SST-VAL725: metric evaluation mechanics.

Fires when tool_selection_accuracy uses no LLM judge; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import config, sales_eval, system_metric, validate


def test_sst_val725_fires() -> None:
    diagnostic = only(validate(), "SST-VAL725")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "eval config for 'sales': metric 'tool_selection_accuracy' uses no LLM judge"
    assert diagnostic.subject == "eval:sales"


def test_sst_val725_silent() -> None:
    assert "SST-VAL725" not in codes(
        validate(sales_eval(eval_config=config(system_metrics=(system_metric("answer_correctness"),))))
    )
