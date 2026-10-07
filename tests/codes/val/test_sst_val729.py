"""SST-VAL729: config stage file format is wrong.

An eval run refuses to start when the config stage declares a format EXECUTE_AI_EVALUATION
cannot parse; the format evals read lets it run.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.options import EvalRunOptions
from snowflake_semantic_tools.app.evals.run import RunEvalSuite
from snowflake_semantic_tools.app.lifecycle.evals import EVAL_STAGE_FILE_FORMAT
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.clocks import FixedClock
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_builders import EvalSnowflake, compiled_eval_of, result_rows, status_result

STAGE = "DB.S.EVAL_CONFIGS"


def test_sst_val729_fires() -> None:
    port = EvalSnowflake([])
    port.stage_formats[STAGE] = "TYPE='JSON'"
    result = RunEvalSuite(port, FixedClock()).run(
        (compiled_eval_of(),), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z")
    )
    diagnostic = only(result.diagnostics, "SST-VAL729")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval config for 'sales_agent': stage file format is TYPE='JSON'"
    assert diagnostic.subject == "eval:sales_agent"
    assert port.scripts == []


def test_sst_val729_silent() -> None:
    port = EvalSnowflake([status_result("COMPLETED"), result_rows()])
    port.stage_formats[STAGE] = EVAL_STAGE_FILE_FORMAT
    result = RunEvalSuite(port, FixedClock()).run(
        (compiled_eval_of(),), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z")
    )
    assert "SST-VAL729" not in codes(result.diagnostics)
    assert result.success
