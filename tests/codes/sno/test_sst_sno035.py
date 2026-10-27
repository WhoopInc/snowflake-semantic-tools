"""SST-SNO035: a CoWork profile run left questions untraced.

A question of a CoWork profile's eval run (a Snowflake private preview) whose record has no
root span after every read fails routing_accuracy with a warning; a traced decline is quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.routes import ROUTE_READS
from snowflake_semantic_tools.app.evals.run import EvalSuiteResult, RunEvalSuite
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from tests.helpers.clocks import FixedClock
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_builders import (
    PROFILE_RUN,
    EvalSnowflake,
    profile_eval_of,
    profile_results,
    profile_status,
    spans,
)


def _run(*reads: QueryResult) -> EvalSuiteResult:
    compiled = profile_eval_of()
    port = EvalSnowflake([profile_status("COMPLETED"), profile_results(compiled), *reads])
    return RunEvalSuite(port, FixedClock()).run(
        (compiled,), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z")
    )


def test_sst_sno035_fires() -> None:
    result = _run(*(spans(("r1", "DB.AGENTS.SALES")) for _ in range(ROUTE_READS)))
    diagnostic = only(result.diagnostics, "SST-SNO035")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        f"eval 'eval:cowork_profile:concierge': run '{PROFILE_RUN}' traced no route for 1 question(s) "
        f"after {ROUTE_READS} reads of its spans; each fails routing_accuracy"
    )
    assert diagnostic.subject == "eval:cowork_profile:concierge"
    # The attempt is still read: the untraced question fails, it does not read as a decline.
    [_, untraced] = result.evals[0].attempts[0].rows
    assert untraced.routed_subagents is None


def test_sst_sno035_silent() -> None:
    result = _run(spans(("r1", "DB.AGENTS.SALES"), ("r2",)))
    assert "SST-SNO035" not in codes(result.diagnostics)
