"""SST-SNO034: a CoWork profile run's router spans cannot be read.

A CoWork profile's eval run (a Snowflake private preview) whose spans the role cannot read is
not accepted, with a suggestion naming the events table; a readable read is quiet.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.run import EvalSuiteResult, RunEvalSuite
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
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
from tests.helpers.eval_inputs import settled


def _run(answer: QueryResult | Exception) -> EvalSuiteResult:
    compiled = profile_eval_of()
    compiled = replace(compiled, resolved=settled(compiled.resolved, retry=0))
    port = EvalSnowflake([profile_status("COMPLETED"), profile_results(compiled), answer])
    return RunEvalSuite(port, FixedClock()).run(
        (compiled,), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z")
    )


def test_sst_sno034_fires() -> None:
    result = _run(SnowflakePortError("Insufficient privileges"))
    diagnostic = only(result.diagnostics, "SST-SNO034")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        f"eval 'eval:cowork_profile:concierge': the router spans of run '{PROFILE_RUN}' could not be read: "
        "Insufficient privileges"
    )
    assert diagnostic.suggestion == (
        "run the eval with a role that can read SNOWFLAKE.LOCAL.AI_OBSERVABILITY_EVENTS (it may need "
        "READ UNREDACTED AI OBSERVABILITY EVENTS TABLE)"
    )
    assert diagnostic.subject == "eval:cowork_profile:concierge"


def test_sst_sno034_silent() -> None:
    result = _run(spans(("r1", "DB.AGENTS.SALES"), ("r2",)))
    assert "SST-SNO034" not in codes(result.diagnostics)
    assert result.success
