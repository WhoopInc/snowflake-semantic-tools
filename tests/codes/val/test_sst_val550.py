"""SST-VAL550: a deprecated agent still has an alias."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import agent, compile_agents
from tests.helpers.diagnostic_filters import coded


def test_sst_val550_fires() -> None:
    [diagnostic] = coded(
        compile_agents(agent("sales_agent", deprecated=True, alias="current")).diagnostics, "SST-VAL550"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "agent 'sales_agent': is deprecated and alias 'current' still points at a version of it"
    )
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val550_silent() -> None:
    assert coded(compile_agents(agent("sales_agent", deprecated=True)).diagnostics, "SST-VAL550") == []
