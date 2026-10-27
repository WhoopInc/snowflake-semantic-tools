"""SST-VAL770: a comparison candidate is `auto`.

Fires when an eval's `config.candidate_models` names `auto`; a named model stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_inputs import SETTINGS, sales_eval, validate


def test_sst_val770_fires() -> None:
    resolved = sales_eval(settings={**SETTINGS, "candidate_models": ["AUTO"]})
    diagnostic = only(validate(resolved), "SST-VAL770")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "eval 'sales': config.candidate_models names `auto`, which is no model a comparison can run"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val770_silent() -> None:
    resolved = sales_eval(settings={**SETTINGS, "candidate_models": ["claude-sonnet-4-6"]})
    assert "SST-VAL770" not in codes(validate(resolved))
