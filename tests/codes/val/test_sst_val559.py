"""SST-VAL559: an agent tool has type `mcp`, which every run of the agent refuses.

Fires for a `type: mcp` tool, which is not rendered; an MCP server attached through
`spec.passthrough.mcp_servers` stays quiet.
"""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, analyst_tool, builtin_tool, compile_agents, compiled_agents
from tests.helpers.diagnostic_filters import coded

_SERVER = {"mcp_servers": [{"server_spec": {"name": "PARTNER.DELIVERY.DELIVERY_MCP"}}]}


def test_sst_val559_fires() -> None:
    mcp = builtin_tool("mcp", name="delivery_mcp", description="Delivery partner MCP server.")
    result = compile_agents(agent("sales_agent", analyst_tool(), mcp))
    [diagnostic] = coded(result.diagnostics, "SST-VAL559")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "agent 'sales_agent': tool 'delivery_mcp' has type mcp, which every run of the agent refuses"
    )
    assert diagnostic.suggestion == "remove the tool and attach the server under spec.passthrough.mcp_servers"
    assert diagnostic.subject == "agent:sales_agent"
    assert compiled_agents(result) == ()


def test_sst_val559_silent() -> None:
    model = agent("sales_agent", analyst_tool(), passthrough=MappingProxyType(_SERVER))
    result = compile_agents(model, allow_unknown_keys=True)
    assert coded(result.diagnostics, "SST-VAL559") == []
    [compiled] = compiled_agents(result)
    assert '"mcp_servers"' in compiled.payload and '"type": "mcp"' not in compiled.payload
