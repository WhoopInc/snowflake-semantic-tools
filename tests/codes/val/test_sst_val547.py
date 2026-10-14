"""SST-VAL547: the agent sets a token budget it does not document as orchestration-only."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.setting import Setting
from tests.helpers.agent_builders import agent, compile_agents
from tests.helpers.diagnostic_filters import coded


def _budget(source: str) -> MappingProxyType[str, Setting]:
    return MappingProxyType({"budget_tokens": Setting("budget_tokens", 16000, source)})


def test_sst_val547_fires() -> None:
    own = agent("sales_agent", budget_tokens=16000, settings=_budget("config"))
    [diagnostic] = coded(compile_agents(own).diagnostics, "SST-VAL547")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'sales_agent': config.budget_tokens covers orchestration only"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val547_silent() -> None:
    documented = agent("sales_agent", budget_tokens=16000, budget_tokens_documented=True, settings=_budget("config"))
    # A project-wide default the agent inherits is not reported once per agent.
    inherited = agent("other_agent", budget_tokens=16000, settings=_budget("route:finance"))
    assert coded(compile_agents(documented, inherited).diagnostics, "SST-VAL547") == []
