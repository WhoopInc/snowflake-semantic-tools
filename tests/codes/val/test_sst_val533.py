"""SST-VAL533: a generic tool's resource key is not confirmed by a live spec."""

from __future__ import annotations

import json

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, found, generic_tool, observe, procedure_member
from tests.helpers.snowflake_fake import FakeSnowflake


def _described(resources: dict[str, object]) -> list[Diagnostic]:
    port = FakeSnowflake()
    port.existing = {"DB.DEV.LOOKUP"}
    port.show_rows["AGENT DB.S.SALES_AGENT"] = {"owner": "TEST_ROLE"}
    port.descriptions["AGENT DB.S.SALES_AGENT"] = {"agent_spec": json.dumps({"tool_resources": {"lookup": resources}})}
    result = compile_agents(agent("sales_agent", generic_tool()), tools=catalog(procedure_member()))
    return found(observe(port, result), "SST-VAL533")


def test_sst_val533_fires() -> None:
    [diagnostic] = _described({"procedure": "DB.DEV.LOOKUP"})
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "agent 'sales_agent': resource key 'identifier' for a generic tool is unverified"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val533_silent() -> None:
    assert _described({"identifier": "DB.DEV.LOOKUP"}) == []
