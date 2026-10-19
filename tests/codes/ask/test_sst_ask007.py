"""SST-ASK007: an exact metric of an unpublished eval run scored a question outside its threshold.

Fires for an agent that calls a tool where the question expects none; one that calls none
stays quiet.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.evals.inline import InlineEvalSetup, RunInlineEvals
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.eval import EvalSystemMetric, ThresholdRange
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply, ToolUse
from tests.helpers.ask_builders import inline_setup
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_builders import resolved_eval
from tests.helpers.snowflake_fake import FakeSnowflake


def _setup() -> InlineEvalSetup:
    base = resolved_eval()
    metric = EvalSystemMetric(
        Origin("config.yml", 1, 1), "tool_selection_accuracy", "v3", gate=True, threshold=ThresholdRange(0.8)
    )
    return inline_setup(replace(base, config=replace(base.config, system_metrics=(metric,)), custom_metrics=()))


def test_sst_ask007_fires() -> None:
    port = FakeSnowflake()
    # The question's ground truth expects no tool call.
    port.agent_replies = {"Question": AgentReply("x", (ToolUse("SALES", "cortex_analyst_text_to_sql"),))}
    diagnostic = only(RunInlineEvals(port).run((_setup(),)).diagnostics, "SST-ASK007")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "eval 'sales_agent': tool_selection_accuracy needs at least 0.8, and 1 of 1 question(s) fell short: "
        "question 1 scored 0.00 (expected: none; called: SALES)"
    )
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_ask007_silent() -> None:
    assert "SST-ASK007" not in codes(RunInlineEvals(FakeSnowflake()).run((_setup(),)).diagnostics)
