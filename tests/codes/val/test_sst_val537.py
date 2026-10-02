"""SST-VAL537: a relative-dated sample question is also an eval question."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, compile_agents, cross, evaluation, found


def _sampled(question: str) -> list[Diagnostic]:
    model = agent("sales_agent", sample_questions=(question,))
    return found(cross(compile_agents(model), evals=(evaluation(model, question),)), "SST-VAL537")


def test_sst_val537_fires() -> None:
    [diagnostic] = _sampled("How many orders last month?")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "agent 'sales_agent': sample question 'How many orders last month?' is relative-dated and matches an eval row"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val537_silent() -> None:
    assert _sampled("How many orders in March 2026?") == []
