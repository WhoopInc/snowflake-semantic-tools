"""SST-VAL710: dataset has fewer rows than the configured floor.

Fires when a dataset is under the configured row floor; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalDefaults,
)
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import validate


def test_sst_val710_fires() -> None:
    diagnostic = only(validate(defaults=EvalDefaults(min_dataset_rows=2)), "SST-VAL710")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "dataset 'agents/sales/evals/dataset.yml' has 1 rows, under 2"
    assert diagnostic.subject == "eval:sales"


def test_sst_val710_silent() -> None:
    assert "SST-VAL710" not in codes(validate(defaults=EvalDefaults(min_dataset_rows=1)))
