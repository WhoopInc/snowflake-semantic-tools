"""SST-VAL718: run_name carries no commit SHA.

Fires when a run name carries no commit SHA; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import config, run_config, sales_eval, validate


def test_sst_val718_fires() -> None:
    diagnostic = only(
        validate(sales_eval(eval_config=config(run=run_config(name_template="EVAL_{{ agent }}_{{ ts }}")))),
        "SST-VAL718",
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "run_name 'EVAL_sales_20000101T000000Z' does not include the git SHA"
    assert diagnostic.subject == "eval:sales"


def test_sst_val718_silent() -> None:
    assert "SST-VAL718" not in codes(
        validate(sales_eval(eval_config=config(run=run_config(name_template="EVAL_{{ agent }}_{{ sha7 }}_{{ ts }}"))))
    )
