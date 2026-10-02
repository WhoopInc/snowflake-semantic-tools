"""SST-VAL707: eval question duplicates an agent sample question.

Fires when a question repeats a sample question; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import (
    codes,
    dataset,
    only,
    question,
    sales_eval,
    validate,
)


def test_sst_val707_fires() -> None:
    diagnostic = only(validate(sales_eval(eval_dataset=dataset(question("Show revenue by region.")))), "SST-VAL707")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "dataset 'agents/sales/evals/dataset.yml': row 0 is byte-identical to a sample_question"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val707_silent() -> None:
    assert "SST-VAL707" not in codes(
        validate(sales_eval(eval_dataset=dataset(question("Show revenue by region and year."))))
    )
