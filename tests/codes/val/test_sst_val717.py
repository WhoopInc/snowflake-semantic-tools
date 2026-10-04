"""SST-VAL717: run_name is not unique for the agent.

Fires when two evals of one agent render one run name; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalDatasetConfig,
)
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import config, dataset, run_config, sales_eval, validate


def test_sst_val717_fires() -> None:
    diagnostic = only(
        validate(
            sales_eval(),
            sales_eval(
                eval_dataset=dataset(),
                eval_config=config(
                    dataset=EvalDatasetConfig("auto", "EVAL2_{{ agent }}_{{ sha7 }}", "SRC2_{{ agent }}_{{ sha7 }}")
                ),
            ),
        ),
        "SST-VAL717",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "run_name 'EVAL_SALES_0000000_ci_20000101T000000Z' is already used for sales"
    assert diagnostic.subject == "eval:sales"


def test_sst_val717_silent() -> None:
    assert "SST-VAL717" not in codes(
        validate(
            sales_eval(),
            sales_eval(
                eval_config=config(
                    dataset=EvalDatasetConfig("auto", "EVAL2_{{ agent }}_{{ sha7 }}", "SRC2_{{ agent }}_{{ sha7 }}"),
                    run=run_config(name_template="EVAL2_{{ agent }}_{{ sha7 }}_{{ ts }}"),
                )
            ),
        )
    )
