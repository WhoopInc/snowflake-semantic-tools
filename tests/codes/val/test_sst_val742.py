"""SST-VAL742: judge prompt asks for reasoning with no parseable score.

Fires when a judge prompt asks for reasoning with no parseable score; the nearest legitimate input
stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import judge, sales_eval, validate


def test_sst_val742_fires() -> None:
    diagnostic = only(
        validate(
            sales_eval(
                metrics=(
                    judge(prompt="Explain your reasoning on a scale from 0 to 5. If tied or insufficient, say so."),
                )
            )
        ),
        "SST-VAL742",
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "eval metric 'grounding': the prompt has no parseable score instruction"
    assert diagnostic.subject == "eval_metric:grounding"


def test_sst_val742_silent() -> None:
    assert "SST-VAL742" not in codes(validate())
