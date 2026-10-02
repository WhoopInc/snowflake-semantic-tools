"""SST-VAL747: scoring instruction produces a number outside the declared ranges.

Fires when a judge prompt's scale reaches outside the declared bands; the nearest legitimate input
stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import (
    JUDGE_PROMPT,
    codes,
    judge,
    only,
    sales_eval,
    validate,
)


def test_sst_val747_fires() -> None:
    diagnostic = only(
        validate(sales_eval(metrics=(judge(prompt=JUDGE_PROMPT.replace("0 to 5", "0 to 9")),))), "SST-VAL747"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval metric 'grounding': the prompt can produce 0..9, outside 0..5"
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_val747_silent() -> None:
    assert "SST-VAL747" not in codes(validate())
