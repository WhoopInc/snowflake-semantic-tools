"""SST-VAL884: CoWork profile eval expects no route.

A CoWork profile's dataset (a Snowflake private preview) whose rows name no subagents gives
`routing_accuracy` nothing to score; one row naming them is enough.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import concierge_eval, routed, validate


def test_sst_val884_fires() -> None:
    diagnostic = only(validate(concierge_eval(routed("Which region sold the most?", subagents=None))), "SST-VAL884")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "dataset 'cowork_profiles/concierge/evals/dataset.yml': no row sets ground_truth_subagents, "
        "so routing_accuracy scores nothing on cowork profile 'concierge'"
    )
    assert diagnostic.subject == "eval:cowork_profile:concierge"


def test_sst_val884_silent() -> None:
    rows = (routed("Which region sold the most?", subagents=None), routed("Which supplier is late?"))
    assert "SST-VAL884" not in codes(validate(concierge_eval(*rows)))
