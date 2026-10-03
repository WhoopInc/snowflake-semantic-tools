"""SST-VAL507: a shared secure agent is authored non-secure."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow
from tests.helpers.agent_builders import agent, compile_agents, found, observe
from tests.helpers.snowflake_fake import FakeSnowflake


def _granted_to(kind: str) -> list[Diagnostic]:
    port = FakeSnowflake()
    port.show_rows["AGENT DB.S.SALES_AGENT"] = {"owner": "TEST_ROLE", "is_secure": "true"}
    port.grants["DB.S.SALES_AGENT"] = (GrantRow("USAGE", kind, "PARTNER"),)
    return found(observe(port, compile_agents(agent("sales_agent"))), "SST-VAL507")


def test_sst_val507_fires() -> None:
    [diagnostic] = _granted_to("SHARE")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent' is granted to PARTNER; it cannot become non-secure"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val507_silent() -> None:
    assert _granted_to("ROLE") == []
