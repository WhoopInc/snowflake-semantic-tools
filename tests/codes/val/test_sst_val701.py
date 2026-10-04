"""SST-VAL701: dataset name is not unique within the agent schema.

Fires when two evals render one dataset name; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalDatasetConfig,
)
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import config, dataset, sales_agent, sales_eval, validate


def test_sst_val701_fires() -> None:
    diagnostic = only(
        validate(
            sales_eval(
                eval_config=config(dataset=EvalDatasetConfig("auto", "EVAL_{{ sha7 }}", "SRC_{{ agent }}_{{ sha7 }}"))
            ),
            sales_eval(
                agent=sales_agent(name="orders"),
                eval_dataset=dataset(agent="orders"),
                eval_config=config(
                    agent="orders", dataset=EvalDatasetConfig("auto", "EVAL_{{ sha7 }}", "SRC_{{ agent }}_{{ sha7 }}")
                ),
            ),
        ),
        "SST-VAL701",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "dataset 'EVAL_0000000' is declared twice in sales"
    assert diagnostic.subject == "eval:orders"


def test_sst_val701_silent() -> None:
    assert "SST-VAL701" not in codes(
        validate(
            sales_eval(),
            sales_eval(
                agent=sales_agent(name="orders"),
                eval_dataset=dataset(agent="orders"),
                eval_config=config(agent="orders"),
            ),
        )
    )
