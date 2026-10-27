"""SST-ASK006: a judge in an unpublished eval run gave no score for a question.

Fires for a judge that answers a score off the metric's scale; one on the scale stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.inline import RunInlineEvals
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.ask_builders import inline_setup
from tests.helpers.diagnostic_filters import codes
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_ask006_fires() -> None:
    port = FakeSnowflake()
    port.judge_answer = {"score": 7, "reason": "off the scale"}
    # Off both the custom metric's 0 to 5 and the 0 to 1 SST's own correctness judge scores on.
    diagnostic, _ = [
        item for item in RunInlineEvals(port).run((inline_setup(),)).diagnostics if item.code == "SST-ASK006"
    ]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "eval 'sales_agent': the judge of 'answer_correctness' gave no score for question 1: "
        "it answered no number from 0 to 1"
    )
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_ask006_silent() -> None:
    port = FakeSnowflake()
    port.judge_answer = {"score": 1, "reason": "on the scale"}
    assert "SST-ASK006" not in codes(RunInlineEvals(port).run((inline_setup(),)).diagnostics)
