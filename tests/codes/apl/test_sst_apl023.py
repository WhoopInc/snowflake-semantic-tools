"""SST-APL023: Snowflake refused to start an eval run."""

from __future__ import annotations

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.evals.run import EvalRunOptions, RunEvalSuite
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, ExecutionError
from tests.helpers.clocks import FixedClock
from tests.helpers.eval_builders import EvalSnowflake, compiled_eval_of, result_rows, status_result

OPTIONS = EvalRunOptions("abcdef0", timestamp="20260928T010203Z")


def test_sst_apl023_fires() -> None:
    assert ApplyArtifacts is not None  # apply first: the eval modules import it
    port = EvalSnowflake([])
    port.execute_results.append(ExecResult(False, error=ExecutionError("Insufficient privileges to operate on task")))
    result = RunEvalSuite(port, FixedClock()).run((compiled_eval_of(),), options=OPTIONS)
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-APL023"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval 'eval:sales_agent': Insufficient privileges to operate on task"
    assert port.scripts[0][0] == "IN DB.S"


def test_sst_apl023_silent() -> None:
    port = EvalSnowflake([status_result("COMPLETED"), result_rows()])
    result = RunEvalSuite(port, FixedClock()).run((compiled_eval_of(),), options=OPTIONS)
    assert "SST-APL023" not in [item.code for item in result.diagnostics]
