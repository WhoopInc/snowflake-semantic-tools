"""SST-VAL719: agent_version is LIVE or omitted.

Fires when the agent version is LIVE; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_inputs import (
    codes,
    config,
    only,
    sales_eval,
    validate,
)


def test_sst_val719_fires() -> None:
    diagnostic = only(validate(sales_eval(eval_config=config(agent_version="LIVE"))), "SST-VAL719")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval config for 'sales': agent_version is 'LIVE'"
    assert diagnostic.subject == "eval:sales"


def test_sst_val719_silent() -> None:
    assert "SST-VAL719" not in codes(validate(sales_eval(eval_config=config(agent_version="VERSION$3"))))
