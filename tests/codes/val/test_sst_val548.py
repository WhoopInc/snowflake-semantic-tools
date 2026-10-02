"""SST-VAL548: a profile color is in no known form."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.agent import AgentProfile
from tests.helpers.agent_builders import agent, compile_agents, found


def test_sst_val548_fires() -> None:
    model = agent("sales_agent", profile=AgentProfile("Sales", "Icon", "#ff0000"))
    [diagnostic] = found(compile_agents(model).diagnostics, "SST-VAL548")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "agent 'sales_agent': color '#ff0000' is neither a plain colour name nor a var(--token)"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val548_silent() -> None:
    model = agent("sales_agent", profile=AgentProfile("Sales", "Icon", "var(--chartDim_3-x11sbcwy)"))
    assert found(compile_agents(model, avatar_allowlist=frozenset(("Icon",))).diagnostics, "SST-VAL548") == []
