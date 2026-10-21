"""SST-ASK013: an unpublished run refuses a CoWork profile's eval.

A CoWork profile's eval (a Snowflake private preview) is refused before an unpublished run
starts, naming the selector that leaves it out; an agent's eval is not.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.inline import unpublished_refusals
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import only
from tests.helpers.eval_builders import compile_profile_eval, compiled_eval_of
from tests.helpers.eval_inputs import concierge_eval


def test_sst_ask013_fires() -> None:
    evals = tuple(item for item in compile_profile_eval(concierge_eval()).compiled if isinstance(item, CompiledEval))
    diagnostic = only(unpublished_refusals(evals), "SST-ASK013")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval 'concierge' cannot run unpublished: it evaluates a CoWork profile"
    assert diagnostic.suggestion == (
        "run it published with `sst test --suite evals`, or leave it out with `--exclude eval:cowork_profile:concierge`"
    )
    assert diagnostic.subject == "eval:cowork_profile:concierge"


def test_sst_ask013_silent() -> None:
    assert unpublished_refusals((compiled_eval_of(),)) == ()
