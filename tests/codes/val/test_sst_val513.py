"""SST-VAL513: a resolved tool name is not 1-64 characters."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, builtin_tool, compile_agents, found


def test_sst_val513_fires() -> None:
    model = agent("sales_agent", builtin_tool(name="c" * 65))
    [diagnostic] = found(compile_agents(model).diagnostics, "SST-VAL513")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"agent 'sales_agent': resolved tool name '{'c' * 65}' is 65 chars"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val513_silent() -> None:
    assert found(compile_agents(agent("sales_agent", builtin_tool(name="c" * 64))).diagnostics, "SST-VAL513") == []
