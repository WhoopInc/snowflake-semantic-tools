"""SST-VAL723: metric_version pins a legacy judge.

Fires when a system metric pins a legacy version; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import config, sales_eval, system_metric, validate


def test_sst_val723_fires() -> None:
    diagnostic = only(
        validate(sales_eval(eval_config=config(system_metrics=(system_metric(version="v2"),)))), "SST-VAL723"
    )
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == ("eval config for 'sales': metric 'tool_selection_accuracy' pins legacy version 'v2'")
    assert diagnostic.subject == "eval:sales"


def test_sst_val723_silent() -> None:
    assert "SST-VAL723" not in codes(validate())
