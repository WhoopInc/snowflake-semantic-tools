"""SST-VAL547: the agent sets a token budget it does not document as orchestration-only."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, compile_agents, found


def test_sst_val547_fires() -> None:
    [diagnostic] = found(compile_agents(agent("sales_agent", budget_tokens=16000)).diagnostics, "SST-VAL547")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'sales_agent': budget.tokens covers orchestration only"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val547_silent() -> None:
    documented = agent("sales_agent", budget_tokens=16000, budget_tokens_documented=True)
    # A project-wide default the agent inherits is not reported once per agent.
    inherited = agent("other_agent")
    assert found(compile_agents(documented, inherited, budget_tokens=16000).diagnostics, "SST-VAL547") == []
