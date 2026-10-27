"""SST-RND020: an eval case names a ground-truth expectation the renderer cannot express."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import EvalGroundTruth
from snowflake_semantic_tools.domain.render.eval import eval_render_checks
from tests.helpers.eval_builders import ORIGIN, compile_eval
from tests.helpers.rnd_codes import with_truth


def test_sst_rnd020_fires() -> None:
    resolved = with_truth(EvalGroundTruth(ORIGIN, (), "Answer", extra={"ground_truth_sql": "SELECT 1"}))
    [diagnostic] = eval_render_checks(resolved)
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-RND020",
        Severity.ERROR,
        "eval:sales_agent",
    )
    assert diagnostic.message == "eval 'sales_agent': expectation kind 'ground_truth_sql' is unknown"
    result = compile_eval(resolved)
    assert result.compiled == () and [item.code for item in result.diagnostics] == ["SST-RND020"]


def test_sst_rnd020_silent() -> None:
    # A key outside the ground_truth_ vocabulary is row metadata, passed through as written.
    resolved = with_truth(EvalGroundTruth(ORIGIN, (), "Answer", extra={"topic": "sales"}))
    assert eval_render_checks(resolved) == ()
    assert len(compile_eval(resolved).compiled) == 1
