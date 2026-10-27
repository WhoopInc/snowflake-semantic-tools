"""SST-VAL711: question-set tool coverage.

Fires when the coverage note counts tools no question exercises; the nearest legitimate input stays
quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import validate


def test_sst_val711_fires() -> None:
    diagnostic = only(validate(), "SST-VAL711")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "dataset 'agents/sales/evals/dataset.yml': 1 of the agent's tools are exercised by no question"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val711_silent() -> None:
    assert "SST-VAL711" not in codes(validate(tool_names=()))
