"""SST-VAL740: custom metric duplicates a system metric's intent.

Fires when a judge prompt restates a system metric; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import JUDGE_PROMPT, judge, sales_eval, validate


def test_sst_val740_fires() -> None:
    diagnostic = only(
        validate(sales_eval(metrics=(judge(prompt=JUDGE_PROMPT + " Judge the answer correctness."),))), "SST-VAL740"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "eval metric 'grounding' duplicates 'answer_correctness'"
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_val740_silent() -> None:
    assert "SST-VAL740" not in codes(validate())
