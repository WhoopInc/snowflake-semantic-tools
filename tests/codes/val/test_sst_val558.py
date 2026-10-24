"""SST-VAL558: an agent's profile renders over Snowflake's 1,000-byte limit.

Fires when the profile's display name makes its JSON 1,001 bytes or more; a profile of exactly
1,000 bytes stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.agent import AgentProfile
from tests.helpers.agent_builders import agent, analyst_tool, compile_agents
from tests.helpers.diagnostic_filters import coded

# `{"display_name":"<name>"}` is 19 bytes around the name.
_WRAPPER = len('{"display_name":""}')


def test_sst_val558_fires() -> None:
    model = agent("sales_agent", analyst_tool(), profile=AgentProfile("D" * (1001 - _WRAPPER)))
    result = compile_agents(model)
    [diagnostic] = coded(result.diagnostics, "SST-VAL558")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "agent 'sales_agent': agent profile renders 1001 bytes of JSON, over Snowflake's 1,000-byte limit"
    )
    assert diagnostic.subject == "agent:sales_agent"
    # The publish would stop part way, so the agent does not compile.
    assert result.compiled == ()


def test_sst_val558_silent() -> None:
    model = agent("sales_agent", analyst_tool(), profile=AgentProfile("D" * (1000 - _WRAPPER)))
    assert coded(compile_agents(model).diagnostics, "SST-VAL558") == []
