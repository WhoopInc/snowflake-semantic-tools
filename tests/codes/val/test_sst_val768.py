"""SST-VAL768: expected subagents in an agent's eval dataset.

A row of an agent's dataset that sets `ground_truth_subagents` is refused; the same row in a
CoWork profile's dataset (a Snowflake private preview) is an expectation it scores.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import concierge_eval, dataset, question, sales_eval, validate


def test_sst_val768_fires() -> None:
    row = question(subagents=())
    diagnostic = only(validate(sales_eval(eval_dataset=dataset(row))), "SST-VAL768")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "dataset 'agents/sales/evals/dataset.yml': row 0 sets ground_truth_subagents, "
        "which only a CoWork profile's eval scores"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val768_silent() -> None:
    assert "SST-VAL768" not in codes(validate(sales_eval(), concierge_eval()))
