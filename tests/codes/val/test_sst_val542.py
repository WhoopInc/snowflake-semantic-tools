"""SST-VAL542: the session or a consuming role lacks READ on a pinned extension."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow
from tests.helpers.agent_builders import agent, compile_agents, observe
from tests.helpers.diagnostic_filters import coded
from tests.helpers.snowflake_fake import FakeSnowflake


def _read_by(*grants: GrantRow) -> list[Diagnostic]:
    port = FakeSnowflake()
    port.grants["DB.EXT.VENDOR_PACK"] = grants
    skill = AgentSkill("vendor", "CORTEX_EXTENSION", "vendor_pack", "V2", ref="extension")
    return coded(observe(port, compile_agents(agent("sales_agent", skills=(skill,)))), "SST-VAL542")


def test_sst_val542_fires() -> None:
    [diagnostic] = _read_by(GrantRow("READ", "ROLE", "ANALYST"))
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent 'sales_agent': TEST_ROLE lacks READ on extension 'DB.EXT.VENDOR_PACK'"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val542_silent() -> None:
    assert _read_by(GrantRow("READ", "ROLE", "TEST_ROLE")) == []
