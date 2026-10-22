"""SST-VAL561: an agent's or CoWork profile's alias starts with the prefix eval-model versions reserve."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.validate.agent import agent_rules, published_agent_rules


def _model(alias: str) -> AgentModel:
    return AgentModel("sales", Origin("agent.yml"), ("agent.yml",), alias=alias)


def test_sst_val561_fires() -> None:
    [diagnostic] = [item for item in agent_rules(_model("Eval_Prod"), frozenset(), ()) if item.code == "SST-VAL561"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "sales: alias 'Eval_Prod' starts with EVAL_, which SST reserves for eval-model versions"
    )
    assert diagnostic.subject == "agent:sales"
    # A CoWork profile's alias is checked by the rules every AGENT object shares.
    assert [item.code for item in published_agent_rules(_model("EVAL_X"), frozenset())].count("SST-VAL561") == 1


def test_sst_val561_silent() -> None:
    assert "SST-VAL561" not in [item.code for item in agent_rules(_model("evaluated"), frozenset(), ())]
