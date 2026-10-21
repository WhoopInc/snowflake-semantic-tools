"""SST-VAL881: expected subagent is not one of the profile's.

A row expecting an agent the CoWork profile (a Snowflake private preview) does not route to is
refused, naming the profile's subagents; a row naming one of them, in any case, stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import concierge_eval, routed, validate


def test_sst_val881_fires() -> None:
    rows = (routed("Which region sold the most?", "agent:partner_agent"), routed("Which supplier is late?"))
    diagnostic = only(validate(concierge_eval(*rows)), "SST-VAL881")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "dataset 'cowork_profiles/concierge/evals/dataset.yml': row 0 expects agent('partner_agent'), "
        "not a subagent of cowork profile 'concierge'"
    )
    assert diagnostic.suggestion == "name one of the profile's subagents: agent('sales'), tool('partner_agent')"
    assert diagnostic.subject == "eval:cowork_profile:concierge"


def test_sst_val881_silent() -> None:
    rows = (routed("Which region sold the most?", "agent:SALES", "tool:partner_agent"), routed("Which supplier?"))
    assert "SST-VAL881" not in codes(validate(concierge_eval(*rows)))
