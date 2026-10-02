"""SST-VAL743: rubric has no tie-break or insufficient-information branch.

Fires when a rubric has no tie-break or insufficient-information branch; the nearest legitimate
input stays quiet.
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


def test_sst_val743_fires() -> None:
    diagnostic = only(
        validate(sales_eval(metrics=(judge(prompt="Score from 0 to 5. Return only the numeric score."),))), "SST-VAL743"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "eval metric 'grounding': the rubric has no tie-break or insufficient-information branch"
    )
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_val743_silent() -> None:
    assert "SST-VAL743" not in codes(validate())
