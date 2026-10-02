"""SST-PRS025: an agent's alias is one Snowflake reserves."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.validate.agent import agent_rules


def _codes(alias: str) -> list[str]:
    return [
        item.code
        for item in agent_rules(AgentModel("sales", Origin("agent.yml"), ("agent.yml",), alias=alias), frozenset(), ())
    ]


def test_sst_prs025_fires() -> None:
    model = AgentModel("sales", Origin("agent.yml"), ("agent.yml",), alias="LIVE")
    [diagnostic] = [item for item in agent_rules(model, frozenset(), ()) if item.code == "SST-PRS025"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "sales: alias 'LIVE' is reserved"
    assert diagnostic.subject == "agent:sales"


def test_sst_prs025_silent() -> None:
    assert "SST-PRS025" not in _codes("promoted")
