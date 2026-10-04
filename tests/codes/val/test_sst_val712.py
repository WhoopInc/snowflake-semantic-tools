"""SST-VAL712: cREATE DATASET takes no properties.

Fires when every dataset reports that CREATE DATASET takes no properties; the nearest legitimate
input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalCatalog,
)
from snowflake_semantic_tools.domain.validate.eval import validate_eval_catalog
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import JUDGE, judge, validate


def test_sst_val712_fires() -> None:
    diagnostic = only(validate(), "SST-VAL712")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "dataset 'agents/sales/evals/dataset.yml': versions and provenance are added by ALTER, not CREATE"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val712_silent() -> None:
    assert "SST-VAL712" not in codes(validate_eval_catalog(EvalCatalog((), (judge(),)), allowed_models=(JUDGE,)))
