"""SST-VAL762: eval dataset template is missing.

Fires when the dataset name template is not set; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalDatasetConfig,
)
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import config, sales_eval, validate


def test_sst_val762_fires() -> None:
    diagnostic = only(
        validate(sales_eval(eval_config=config(dataset=EvalDatasetConfig("auto", None, "SRC_{{ agent }}_{{ sha7 }}")))),
        "SST-VAL762",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval 'sales': dataset.name_template is not set"
    assert diagnostic.subject == "eval:sales"


def test_sst_val762_silent() -> None:
    assert "SST-VAL762" not in codes(validate())
