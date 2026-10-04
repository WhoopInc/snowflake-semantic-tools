"""SST-VAL544: an auto-orchestrated agent has a blocking eval."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, compile_agents, cross, evaluation
from tests.helpers.diagnostic_filters import coded


def _tiered(tier: str) -> list[Diagnostic]:
    model = agent("sales_agent", orchestration_model="auto")
    result = compile_agents(model, orchestration_model="auto")
    return coded(cross(result, evals=(evaluation(model, "How many orders?", tier=tier),)), "SST-VAL544")


def test_sst_val544_fires() -> None:
    [diagnostic] = _tiered("blocking")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': models.orchestration is auto and a blocking eval is configured"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val544_silent() -> None:
    assert _tiered("report") == []
