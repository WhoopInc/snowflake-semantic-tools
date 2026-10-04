"""SST-APL024: an eval run ended partially completed."""

from __future__ import annotations

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.evals.run import EvalRunOptions, RunEvalSuite
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.clocks import FixedClock
from tests.helpers.eval_builders import EvalSnowflake, compiled_eval_of, result_rows, status_result

OPTIONS = EvalRunOptions("abcdef0", timestamp="20260928T010203Z")


def test_sst_apl024_fires() -> None:
    assert ApplyArtifacts is not None  # apply first: the eval modules import it
    port = EvalSnowflake([status_result("PARTIALLY_COMPLETED"), result_rows()])
    result = RunEvalSuite(port, FixedClock()).run((compiled_eval_of(),), options=OPTIONS)
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-APL024"]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "eval 'eval:sales_agent': status 'PARTIALLY_COMPLETED'"


def test_sst_apl024_silent() -> None:
    port = EvalSnowflake([status_result("COMPLETED"), result_rows()])
    result = RunEvalSuite(port, FixedClock()).run((compiled_eval_of(),), options=OPTIONS)
    assert "SST-APL024" not in [item.code for item in result.diagnostics]
