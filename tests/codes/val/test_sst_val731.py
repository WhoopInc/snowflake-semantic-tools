"""SST-VAL731: eval concurrency exceeds the configured ceiling.

Fires when run concurrency exceeds the project ceiling; the nearest legitimate input stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import (
    EvalDefaults,
)
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import validate


def test_sst_val731_fires() -> None:
    diagnostic = only(validate(defaults=EvalDefaults(concurrency=1)), "SST-VAL731")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "eval config for 'sales': concurrency 2 exceeds 1"
    assert diagnostic.subject == "eval:sales"


def test_sst_val731_silent() -> None:
    assert "SST-VAL731" not in codes(validate(defaults=EvalDefaults(concurrency=2)))
