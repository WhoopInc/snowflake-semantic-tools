"""SST-VAL543: the orchestration model is outside the allowlist."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, compile_agents, found


def test_sst_val543_fires() -> None:
    [diagnostic] = found(compile_agents(agent("sales_agent", orchestration_model="gpt-x")).diagnostics, "SST-VAL543")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': models.orchestration 'gpt-x' is not in the allowlist"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val543_silent() -> None:
    assert found(compile_agents(agent("sales_agent", orchestration_model="claude")).diagnostics, "SST-VAL543") == []
