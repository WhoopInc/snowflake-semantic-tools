"""SST-VAL508: an agent's tag names no tag object."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, compile_agents, found, observe
from tests.helpers.app_ports import InMemorySnowflake


def _tagged(existing: set[str]) -> list[Diagnostic]:
    port = InMemorySnowflake()
    port.existing = existing
    model = agent("sales_agent", tags=(("COST_CENTER", "analytics"),))
    return found(observe(port, compile_agents(model)), "SST-VAL508")


def test_sst_val508_fires() -> None:
    [diagnostic] = _tagged(set())
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': tag 'COST_CENTER' does not resolve to a tag object"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val508_silent() -> None:
    assert _tagged({"DB.S.COST_CENTER"}) == []
