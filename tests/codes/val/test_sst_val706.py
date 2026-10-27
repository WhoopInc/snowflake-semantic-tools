"""SST-VAL706: relative date in an eval question or expected answer.

Fires when a question carries a relative date; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import dataset, question, sales_eval, validate


def test_sst_val706_fires() -> None:
    diagnostic = only(
        validate(sales_eval(eval_dataset=dataset(question("Show revenue for last month.")))), "SST-VAL706"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == ("dataset 'agents/sales/evals/dataset.yml': row 0 contains relative date 'last month'")
    assert diagnostic.subject == "eval:sales"


def test_sst_val706_silent() -> None:
    assert "SST-VAL706" not in codes(
        validate(sales_eval(eval_dataset=dataset(question("Show revenue for March 2025."))))
    )
