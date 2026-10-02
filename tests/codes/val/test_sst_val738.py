"""SST-VAL738: custom metric declares no explicit judge model.

Fires when a custom metric's judge model is auto; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import (
    codes,
    judge,
    only,
    sales_eval,
    validate,
)


def test_sst_val738_fires() -> None:
    diagnostic = only(validate(sales_eval(metrics=(judge(model="auto"),))), "SST-VAL738")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval metric 'grounding': judge_model is 'auto'"
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_val738_silent() -> None:
    assert "SST-VAL738" not in codes(validate())
