"""SST-VAL616: the session lacks USAGE on an object a tool calls."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow
from tests.helpers.agent_builders import agent, catalog, compile_agents, generic_tool, observe, procedure_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.snowflake_fake import FakeSnowflake


def _granted(*grants: GrantRow) -> list[Diagnostic]:
    port = FakeSnowflake()
    port.existing = {"DB.DEV.LOOKUP"}
    port.grants["DB.DEV.LOOKUP"] = grants
    result = compile_agents(agent("sales_agent", generic_tool()), tools=catalog(procedure_member()))
    return coded(observe(port, result), "SST-VAL616")


def test_sst_val616_fires() -> None:
    [diagnostic] = _granted(GrantRow("USAGE", "ROLE", "ANALYST"))
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "tool member 'lookup': TEST_ROLE lacks USAGE on PROCEDURE DB.DEV.LOOKUP"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val616_silent() -> None:
    assert _granted(GrantRow("USAGE", "ROLE", "TEST_ROLE")) == []
