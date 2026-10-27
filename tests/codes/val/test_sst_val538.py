"""SST-VAL538: a consumed extension pins no immutable version."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from tests.helpers.agent_builders import agent, compile_agents
from tests.helpers.diagnostic_filters import coded


def _pinned(version: str) -> list[Diagnostic]:
    skill = AgentSkill("vendor", "CORTEX_EXTENSION", "vendor_pack", version, ref="extension")
    return coded(compile_agents(agent("sales_agent", skills=(skill,))).diagnostics, "SST-VAL538")


def test_sst_val538_fires() -> None:
    [diagnostic] = _pinned("LIVE")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': skill source 'vendor' does not pin an immutable version"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val538_silent() -> None:
    assert _pinned("V2") == []
