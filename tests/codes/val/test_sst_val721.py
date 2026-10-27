"""SST-VAL721: metric is neither a system metric nor a declared custom metric.

Fires when the config names a custom metric that does not resolve; the nearest legitimate input
stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import config, judge, sales_eval, validate


def test_sst_val721_fires() -> None:
    diagnostic = only(
        validate(sales_eval(eval_config=config(custom_metric_names=("grounding", "tone")), metrics=(judge(),))),
        "SST-VAL721",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval config for 'sales': metric 'tone' is unknown"
    assert diagnostic.subject == "eval:sales"


def test_sst_val721_silent() -> None:
    assert "SST-VAL721" not in codes(
        validate(sales_eval(eval_config=config(custom_metric_names=("grounding",)), metrics=(judge(),)))
    )
