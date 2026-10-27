"""SST-VAL709: web_search expectation uses a non-canonical name.

Fires when a web expectation is not named web_search; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalInvocation,
)
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import DATASET_FILE, dataset, question, sales_eval, validate


def test_sst_val709_fires() -> None:
    diagnostic = only(
        validate(
            sales_eval(
                eval_dataset=dataset(
                    question(invocations=(EvalInvocation(Origin(DATASET_FILE, 7), "web_search_tool"),))
                )
            )
        ),
        "SST-VAL709",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "dataset 'agents/sales/evals/dataset.yml': row 0 expects 'web_search_tool'"
    assert diagnostic.subject == "eval:sales"


def test_sst_val709_silent() -> None:
    assert "SST-VAL709" not in codes(
        validate(
            sales_eval(
                eval_dataset=dataset(question(invocations=(EvalInvocation(Origin(DATASET_FILE, 7), "web_search"),)))
            )
        )
    )
