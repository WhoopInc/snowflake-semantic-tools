"""SST-VAL730: partial completion treated as a pass.

A config that accepts PARTIALLY_COMPLETED gets an error, not a pass, when a run ends that way;
without that acceptance the same status is only the SST-APL024 warning.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.run import EvalRunOptions, RunEvalSuite
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.clocks import FixedClock
from tests.helpers.eval_builders import EvalSnowflake, compiled_eval_of, status_result
from tests.helpers.eval_inputs import codes, only


def accepting(*statuses: str) -> CompiledEval:
    compiled = compiled_eval_of()
    run = compiled.resolved.config.run
    assert run is not None
    config = replace(compiled.resolved.config, run=replace(run, accept_statuses=statuses))
    return replace(compiled, resolved=replace(compiled.resolved, config=config))


def test_sst_val730_fires() -> None:
    port = EvalSnowflake([status_result("PARTIALLY_COMPLETED")])
    compiled = accepting("COMPLETED", "PARTIALLY_COMPLETED")
    result = RunEvalSuite(port, FixedClock()).run(
        (compiled,), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z")
    )
    diagnostic = only(result.diagnostics, "SST-VAL730")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval run for 'sales_agent': status 'PARTIALLY_COMPLETED' is not a pass"
    assert diagnostic.subject == "eval:sales_agent"
    assert not result.evals[0].accepted


def test_sst_val730_silent() -> None:
    port = EvalSnowflake([status_result("PARTIALLY_COMPLETED")])
    result = RunEvalSuite(port, FixedClock()).run(
        (accepting("COMPLETED"),), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z")
    )
    assert "SST-VAL730" not in codes(result.diagnostics)
    assert codes(result.diagnostics) == ["SST-APL024"]
