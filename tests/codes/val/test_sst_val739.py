"""SST-VAL739: judge_model is not in the allowlist.

Fires when a judge model is outside the allowlist; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import judge, sales_eval, validate


def test_sst_val739_fires() -> None:
    diagnostic = only(validate(sales_eval(metrics=(judge(model="mistral-large2"),))), "SST-VAL739")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval metric 'grounding': judge_model 'mistral-large2' is not in the allowlist"
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_val739_silent() -> None:
    assert "SST-VAL739" not in codes(validate())
