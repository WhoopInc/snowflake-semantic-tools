"""SST-VAL512: the rendered spec is over 80% of the size limit."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, compile_agents
from tests.helpers.diagnostic_filters import coded


def test_sst_val512_fires() -> None:
    model = agent("sales_agent", orchestration_instructions="x" * 85_000)
    [diagnostic] = coded(compile_agents(model).diagnostics, "SST-VAL512")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message.endswith(" bytes, over 80% of the limit")
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val512_silent() -> None:
    model = agent("sales_agent", orchestration_instructions="x" * 70_000)
    assert coded(compile_agents(model).diagnostics, "SST-VAL512") == []
