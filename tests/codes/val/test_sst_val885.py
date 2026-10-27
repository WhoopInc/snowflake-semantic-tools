"""SST-VAL885: CoWork profile eval expects no decline.

A CoWork profile's dataset (a Snowflake private preview) with routed rows but none expecting
`[]` is noted; a decline row among them stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import concierge_eval, routed, validate


def test_sst_val885_fires() -> None:
    diagnostic = only(validate(concierge_eval(routed("Which region sold the most?", "agent:sales"))), "SST-VAL885")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "dataset 'cowork_profiles/concierge/evals/dataset.yml': no row expects cowork profile 'concierge' "
        "to decline a question"
    )
    assert diagnostic.subject == "eval:cowork_profile:concierge"


def test_sst_val885_silent() -> None:
    assert "SST-VAL885" not in codes(validate(concierge_eval()))
