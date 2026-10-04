"""SST-VAL506: a secure agent's live object is owned by a role other than the session's."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, compile_agents, observe
from tests.helpers.diagnostic_filters import coded
from tests.helpers.snowflake_fake import FakeSnowflake


def _owned_by(owner: str) -> list[Diagnostic]:
    port = FakeSnowflake()
    port.show_rows["AGENT DB.S.SALES_AGENT"] = {"owner": owner}
    return coded(observe(port, compile_agents(agent("sales_agent", secure=True))), "SST-VAL506")


def test_sst_val506_fires() -> None:
    [diagnostic] = _owned_by("AGENT_ADMIN")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'sales_agent' is secure; the round-trip check needs the owner role"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val506_silent() -> None:
    assert _owned_by("TEST_ROLE") == []
