"""SST-VAL702: dataset name exceeds the object limit.

Fires when a rendered dataset name is over 128 characters; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalDatasetConfig,
)
from tests.helpers.eval_inputs import (
    codes,
    config,
    only,
    sales_eval,
    validate,
)


def test_sst_val702_fires() -> None:
    diagnostic = only(
        validate(
            sales_eval(
                eval_config=config(
                    dataset=EvalDatasetConfig("auto", "EVAL_{{ agent }}_" + "X" * 120, "SRC_{{ agent }}")
                )
            )
        ),
        "SST-VAL702",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "dataset 'EVAL_sales_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX' is 131 chars, over 128"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val702_silent() -> None:
    assert "SST-VAL702" not in codes(
        validate(
            sales_eval(
                eval_config=config(
                    dataset=EvalDatasetConfig("auto", "EVAL_{{ agent }}_" + "X" * 100, "SRC_{{ agent }}")
                )
            )
        )
    )
