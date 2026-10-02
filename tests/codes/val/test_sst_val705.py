"""SST-VAL705: dataset row is incomplete.

Fires when a dataset row has no expectation; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalGroundTruth,
    EvalQuestion,
)
from tests.helpers.eval_inputs import (
    DATASET_FILE,
    codes,
    dataset,
    only,
    question,
    sales_eval,
    validate,
)


def test_sst_val705_fires() -> None:
    diagnostic = only(
        validate(
            sales_eval(
                eval_dataset=dataset(
                    EvalQuestion(
                        Origin(DATASET_FILE, 4), "Show revenue in 2025.", EvalGroundTruth(Origin(DATASET_FILE, 6))
                    )
                )
            )
        ),
        "SST-VAL705",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "dataset 'agents/sales/evals/dataset.yml': row 0 no expected field"
    assert diagnostic.subject == "eval:sales"


def test_sst_val705_silent() -> None:
    assert "SST-VAL705" not in codes(validate(sales_eval(eval_dataset=dataset(question(invocations=None)))))
