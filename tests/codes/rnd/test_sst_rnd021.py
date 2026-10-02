"""SST-RND021: an eval's dataset has no questions."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import EvalGroundTruth, EvalQuestion
from snowflake_semantic_tools.domain.render.eval import eval_render_checks
from tests.helpers.eval_builders import ORIGIN, resolved_eval


def with_truth(truth: EvalGroundTruth):  # type: ignore[no-untyped-def]
    value = resolved_eval()
    return replace(value, dataset=replace(value.dataset, questions=(EvalQuestion(ORIGIN, "Question", truth),)))


def test_sst_rnd021_fires() -> None:
    value = resolved_eval()
    [diagnostic] = eval_render_checks(replace(value, dataset=replace(value.dataset, questions=())))
    assert (diagnostic.code, diagnostic.severity) == ("SST-RND021", Severity.WARNING)
    assert diagnostic.message == "eval 'sales_agent' renders with no cases"


def test_sst_rnd021_silent() -> None:
    assert eval_render_checks(resolved_eval()) == ()
