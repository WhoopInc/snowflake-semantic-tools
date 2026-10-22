"""SST-VAL771: a comparison candidate is not in `snowflake.orchestration_models`.

Fires when a candidate the allowlist does not list is named; a listed one stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.setting import ROOT_SOURCE
from snowflake_semantic_tools.domain.validate.eval_config import eval_candidate_problems
from tests.helpers.eval_inputs import SETTINGS, sales_eval, settled


def test_sst_val771_fires() -> None:
    resolved = settled(sales_eval(), ROOT_SOURCE, candidate_models=["claude-opus-4-8"])
    [diagnostic] = eval_candidate_problems(resolved, frozenset({"claude-sonnet-4-6"}))
    assert (diagnostic.code, diagnostic.severity) == ("SST-VAL771", Severity.ERROR)
    assert diagnostic.message == (
        "eval 'sales': evals.+candidate_models names 'claude-opus-4-8', "
        "which snowflake.orchestration_models does not list"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val771_silent() -> None:
    listed = sales_eval(settings={**SETTINGS, "candidate_models": ["Claude-Sonnet-4-6"]})
    assert eval_candidate_problems(listed, frozenset({"claude-sonnet-4-6"})) == ()
    # Without an allowlist, any named model is a candidate.
    assert eval_candidate_problems(listed, frozenset()) == ()
