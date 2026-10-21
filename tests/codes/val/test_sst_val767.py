"""SST-VAL767: routing metric on an agent's eval.

An agent's eval config that lists `metrics.routing` is refused, since an agent has no router;
the same metric on a CoWork profile's eval (a Snowflake private preview) is what it scores.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import concierge_eval, config, routing_metric, sales_eval, validate


def test_sst_val767_fires() -> None:
    diagnostic = only(validate(sales_eval(eval_config=config(routing_metrics=(routing_metric(),)))), "SST-VAL767")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "eval config for 'sales': routing metric 'routing_accuracy' scores a CoWork profile's router, "
        "and an agent has none"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val767_silent() -> None:
    assert "SST-VAL767" not in codes(validate(sales_eval(), concierge_eval()))
