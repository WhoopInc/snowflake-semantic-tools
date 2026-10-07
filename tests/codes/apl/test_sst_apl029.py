"""SST-APL029: an eval attempt did not pass, and a retry completed in its place."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.run import RunEvalSuite
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from tests.helpers.clocks import FixedClock
from tests.helpers.eval_builders import STATUS_COLUMNS, EvalSnowflake, compiled_eval_of, result_rows, status_result

OPTIONS = EvalRunOptions("abcdef0", timestamp="20260928T010203Z")
FIRST_RUN = "EVAL_SALES_AGENT_abcdef0_ci_20260928T010203Z"


def retrying_eval() -> CompiledEval:
    compiled = compiled_eval_of()
    run = compiled.resolved.config.run
    assert run is not None
    config = replace(compiled.resolved.config, run=replace(run, retry=1))
    return replace(compiled, resolved=replace(compiled.resolved, config=config))


def failed(run_name: str) -> QueryResult:
    return QueryResult(STATUS_COLUMNS, ((run_name, "SALES_AGENT", "CORTEX AGENT", "FAILED", ["Invocation failed"]),))


def test_sst_apl029_fires() -> None:
    assert ApplyArtifacts is not None  # apply first: the eval modules import it
    port = EvalSnowflake([failed(FIRST_RUN), status_result("COMPLETED", f"{FIRST_RUN}_R2"), result_rows()])
    result = RunEvalSuite(port, FixedClock()).run((retrying_eval(),), options=OPTIONS)
    [diagnostic] = result.diagnostics
    assert diagnostic.code == "SST-APL029"
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        f"eval 'eval:sales_agent': run '{FIRST_RUN}' ended FAILED and was retried: Invocation failed"
    )
    assert result.success


def test_sst_apl029_silent() -> None:
    # The retry failed as well, so nothing replaced the first attempt: each reports its error.
    port = EvalSnowflake([failed(FIRST_RUN), failed(f"{FIRST_RUN}_R2")])
    result = RunEvalSuite(port, FixedClock()).run((retrying_eval(),), options=OPTIONS)
    assert [item.code for item in result.diagnostics] == ["SST-APL023", "SST-APL023"]
    assert not result.success
