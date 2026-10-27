"""SST-VAL882: tool metric cannot see a CoWork profile's routing.

Tool selection accuracy on a CoWork profile's eval (a Snowflake private preview) is refused,
since it never sees the router's calls; a judged metric stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import concierge_eval, system_metric, validate


def test_sst_val882_fires() -> None:
    resolved = concierge_eval(system_metrics=(system_metric("tool_selection_accuracy"),))
    diagnostic = only(validate(resolved), "SST-VAL882")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "eval config for 'concierge': metric 'tool_selection_accuracy' does not see the router's calls "
        "of a CoWork profile"
    )
    assert diagnostic.subject == "eval:cowork_profile:concierge"


def test_sst_val882_silent() -> None:
    assert "SST-VAL882" not in codes(validate(concierge_eval()))
