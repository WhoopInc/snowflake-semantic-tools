"""SST-VAL737: custom metric shadows a system metric name.

Fires when a custom metric takes a system metric's name; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import config, judge, sales_eval, validate


def test_sst_val737_fires() -> None:
    diagnostic = only(
        validate(
            sales_eval(
                eval_config=config(custom_metric_names=("answer_correctness",)), metrics=(judge("answer_correctness"),)
            )
        ),
        "SST-VAL737",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval metric 'answer_correctness' shadows system metric 'answer_correctness'"
    assert diagnostic.subject == "eval_metric:answer_correctness"


def test_sst_val737_silent() -> None:
    assert "SST-VAL737" not in codes(validate())
