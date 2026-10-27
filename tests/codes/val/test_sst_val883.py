"""SST-VAL883: expected tool calls in a CoWork profile's eval dataset.

A profile's row that expects tool calls, even none, is refused: no metric scores them on a
CoWork profile (a Snowflake private preview). A row expecting subagents stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import concierge_eval, routed, validate


def test_sst_val883_fires() -> None:
    rows = (routed("Which region sold the most?", "agent:sales", invocations=()), routed("Which supplier?"))
    diagnostic = only(validate(concierge_eval(*rows)), "SST-VAL883")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "dataset 'cowork_profiles/concierge/evals/dataset.yml': row 0 sets ground_truth_invocations, "
        "which no metric scores on cowork profile 'concierge'"
    )
    assert diagnostic.subject == "eval:cowork_profile:concierge"


def test_sst_val883_silent() -> None:
    assert "SST-VAL883" not in codes(validate(concierge_eval()))
