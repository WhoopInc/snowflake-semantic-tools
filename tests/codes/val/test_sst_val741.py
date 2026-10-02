"""SST-VAL741: judge prompt declares no output contract.

Fires when a judge prompt declares no output contract; the nearest legitimate input stays quiet.
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


def test_sst_val741_fires() -> None:
    diagnostic = only(
        validate(
            sales_eval(
                metrics=(
                    judge(
                        prompt="Decide whether the answer is grounded. If information is insufficient or tied, say so."
                    ),
                )
            )
        ),
        "SST-VAL741",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval metric 'grounding': the prompt declares no scale or allowed values"
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_val741_silent() -> None:
    assert "SST-VAL741" not in codes(validate())
