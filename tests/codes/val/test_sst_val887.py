"""SST-VAL887: an inherited tool metric is left out of a CoWork profile's eval.

A CoWork profile's eval (a Snowflake private preview) that inherits tool selection accuracy
from `evals.+metrics` runs without it, noted as info; an eval inheriting no such metric is quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.setting import ROOT_SOURCE
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import concierge_eval, settled, system_metric, validate


def test_sst_val887_fires() -> None:
    blind = system_metric("tool_selection_accuracy", gate=False, threshold=None)
    resolved = settled(
        concierge_eval(inherited_blind_metrics=(blind,)),
        ROOT_SOURCE,
        metrics=["tool_selection_accuracy", "answer_correctness"],
    )
    diagnostic = only(validate(resolved), "SST-VAL887")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "eval config for 'concierge': metric 'tool_selection_accuracy', which evals.+metrics enables, is "
        "left out: it does not see the router's calls of a CoWork profile"
    )
    assert diagnostic.subject == "eval:cowork_profile:concierge"
    assert "SST-VAL882" not in codes(validate(resolved))


def test_sst_val887_silent() -> None:
    assert "SST-VAL887" not in codes(validate(concierge_eval()))
