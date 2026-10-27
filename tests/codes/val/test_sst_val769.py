"""SST-VAL769: routing threshold is outside the score range.

A routing score lies in [0, 1], so a bound outside it is refused, once per bound; a bound inside
it stays quiet. CoWork profiles are a Snowflake private preview.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import ThresholdRange
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import concierge_eval, routing_metric, validate


def test_sst_val769_fires() -> None:
    metric = routing_metric(threshold=ThresholdRange(min=90))
    diagnostic = only(validate(concierge_eval(routing_metrics=(metric,))), "SST-VAL769")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "eval config for 'concierge': routing metric 'routing_accuracy' threshold min 90 is outside [0, 1]"
    )
    assert diagnostic.subject == "eval:cowork_profile:concierge"


def test_sst_val769_silent() -> None:
    metric = routing_metric(threshold=ThresholdRange(min=0.0, max=1.0))
    assert "SST-VAL769" not in codes(validate(concierge_eval(routing_metrics=(metric,))))
