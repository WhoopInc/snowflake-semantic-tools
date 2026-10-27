"""SST-VAL748: judged range is wider than three bands for an invariant.

Fires when an invariant is judged on a wide scale; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalScoreRanges,
    ThresholdRange,
)
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import JUDGE_PROMPT, judge, sales_eval, validate


def test_sst_val748_fires() -> None:
    diagnostic = only(validate(sales_eval(metrics=(judge(description="Checks an invariant"),))), "SST-VAL748")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "eval metric 'grounding' declares 6 bands for an invariant"
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_val748_silent() -> None:
    assert "SST-VAL748" not in codes(
        validate(
            sales_eval(
                metrics=(
                    judge(
                        description="Checks an invariant",
                        score_ranges=EvalScoreRanges((0, 0), (1, 1), (2, 2)),
                        prompt=JUDGE_PROMPT.replace("0 to 5", "0 to 2"),
                        threshold_default=ThresholdRange(min=1),
                    ),
                )
            )
        )
    )
