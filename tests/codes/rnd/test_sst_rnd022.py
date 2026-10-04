"""SST-RND022: an eval case expects a tool input that is SQL text, matched as text."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import EvalGroundTruth, EvalInvocation
from snowflake_semantic_tools.domain.render.eval import eval_render_checks
from tests.helpers.eval_builders import ORIGIN
from tests.helpers.rnd_codes import with_truth


def test_sst_rnd022_fires() -> None:
    invocation = EvalInvocation(ORIGIN, tool_name="sales", tool_input="SELECT SUM(amount) FROM orders")
    [diagnostic] = eval_render_checks(with_truth(EvalGroundTruth(ORIGIN, (invocation,), "Answer")))
    assert (diagnostic.code, diagnostic.severity) == ("SST-RND022", Severity.WARNING)
    assert diagnostic.message == "eval 'sales_agent': row 0 uses SQL_MATCH"


def test_sst_rnd022_silent() -> None:
    invocation = EvalInvocation(ORIGIN, tool_name="sales", tool_input="total order amount for last month")
    assert eval_render_checks(with_truth(EvalGroundTruth(ORIGIN, (invocation,), "Answer"))) == ()
