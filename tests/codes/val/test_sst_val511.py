"""SST-VAL511: the rendered spec is over the 100,000-byte limit."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, compile_agents
from tests.helpers.diagnostic_filters import coded


def test_sst_val511_fires() -> None:
    model = agent("sales_agent", orchestration_instructions="x" * 100_000)
    [diagnostic] = coded(compile_agents(model).diagnostics, "SST-VAL511")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.startswith("agent 'sales_agent': rendered spec is 100")
    assert diagnostic.message.endswith(" bytes, over the 100,000 limit")
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val511_silent() -> None:
    model = agent("sales_agent", orchestration_instructions="x" * 90_000)
    assert coded(compile_agents(model).diagnostics, "SST-VAL511") == []
