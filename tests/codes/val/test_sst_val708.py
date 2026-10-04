"""SST-VAL708: ground_truth_invocations names a tool the agent does not have.

Fires when an expected tool is absent from the agent; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalInvocation,
)
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import DATASET_FILE, dataset, question, sales_eval, validate


def test_sst_val708_fires() -> None:
    diagnostic = only(
        validate(
            sales_eval(
                eval_dataset=dataset(question(invocations=(EvalInvocation(Origin(DATASET_FILE, 7), "ORDERS_VIEW"),)))
            )
        ),
        "SST-VAL708",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "dataset 'agents/sales/evals/dataset.yml': row 0 expects tool 'ORDERS_VIEW', absent from agent 'sales'"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val708_silent() -> None:
    assert "SST-VAL708" not in codes(
        validate(
            sales_eval(
                eval_dataset=dataset(question(invocations=(EvalInvocation(Origin(DATASET_FILE, 7), "SALES_VIEW"),)))
            )
        )
    )
