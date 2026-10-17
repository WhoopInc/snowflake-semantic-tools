"""SST-ASK004: an unpublished eval run cannot judge a metric, so leaves it unscored.

Fires for a custom metric whose prompt uses a placeholder only a published run's trace fills;
the same metric with a prompt a reply fills stays quiet.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.evals.inline import RunInlineEvals
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import ResolvedEval
from tests.helpers.ask_builders import inline_setup
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_builders import resolved_eval
from tests.helpers.snowflake_fake import FakeSnowflake


def _with_prompt(prompt: str) -> ResolvedEval:
    base = resolved_eval()
    return replace(base, custom_metrics=(replace(base.custom_metrics[0], prompt=prompt),))


def test_sst_ask004_fires() -> None:
    setup = inline_setup(_with_prompt("Score {{ llm_model }} from 0 to 5."))
    diagnostic = only(RunInlineEvals(FakeSnowflake()).run((setup,)).diagnostics, "SST-ASK004")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "eval 'sales_agent': metric 'grounding' is not scored without publishing: "
        "its prompt uses llm_model, which only a published run's trace fills"
    )
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_ask004_silent() -> None:
    setup = inline_setup(_with_prompt("Score {{ output }} from 0 to 5."))
    assert "SST-ASK004" not in codes(RunInlineEvals(FakeSnowflake()).run((setup,)).diagnostics)
