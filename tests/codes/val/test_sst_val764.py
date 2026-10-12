"""SST-VAL764: a system metric pins version v1, which Snowflake refuses."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.diagnostics.policy import apply_overrides
from snowflake_semantic_tools.domain.model.eval import EvalDefaults
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import config, sales_eval, system_metric, validate

DEMOTED = {"SST-PRS013": Severity.WARNING, "SST-VAL764": Severity.WARNING}


def test_sst_val764_fires() -> None:
    found = validate(sales_eval(eval_config=config(system_metrics=(system_metric(version="V1"),))))
    diagnostic = only(apply_overrides(found, DEMOTED), "SST-VAL764")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "eval config for 'sales': metric 'tool_selection_accuracy' pins version 'V1', which Snowflake refuses"
    )
    assert diagnostic.subject == "eval:sales"
    assert only(apply_overrides(found, DEMOTED), "SST-PRS013").severity is Severity.WARNING


def test_sst_val764_fires_for_the_default_metric_version() -> None:
    unpinned = sales_eval(eval_config=config(system_metrics=(system_metric(version=None),)))
    diagnostic = only(validate(unpinned, defaults=EvalDefaults(metric_version="v1")), "SST-VAL764")
    assert diagnostic.severity is Severity.ERROR


def test_sst_val764_silent() -> None:
    assert "SST-VAL764" not in codes(validate())
    legacy = sales_eval(eval_config=config(system_metrics=(system_metric(version="v2"),)))
    assert "SST-VAL764" not in codes(validate(legacy))
