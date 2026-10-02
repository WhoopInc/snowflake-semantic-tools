"""SST-VAL539: a skill source is a STAGE path into a mutable bundle."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.agent import AgentSkill
from tests.helpers.agent_builders import agent, compile_agents, found


def _sourced(source_type: str) -> list[Diagnostic]:
    skill = AgentSkill("vendor", source_type, "vendor_pack", "V2", ref="extension")
    return found(compile_agents(agent("sales_agent", skills=(skill,))).diagnostics, "SST-VAL539")


def test_sst_val539_fires() -> None:
    [diagnostic] = _sourced("STAGE")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': skill source 'vendor' is a STAGE path into a mutable bundle"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val539_silent() -> None:
    assert _sourced("CORTEX_EXTENSION") == []
