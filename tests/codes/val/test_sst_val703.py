"""SST-VAL703: dataset declares its own database or schema.

Fires when an eval file sets its own location; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import (
    codes,
    dataset,
    only,
    sales_eval,
    validate,
)


def test_sst_val703_fires() -> None:
    diagnostic = only(validate(sales_eval(eval_dataset=dataset(forbidden_keys=("schema",)))), "SST-VAL703")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "dataset 'agents/sales/evals/dataset.yml' declares +schema"
    assert diagnostic.subject == "eval:sales"


def test_sst_val703_silent() -> None:
    assert "SST-VAL703" not in codes(validate())
