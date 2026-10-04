"""SST-VAL509: a live agent SST published unchanged no longer matches its rendered spec."""

from __future__ import annotations

import json

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.render.agent import render_agent_spec
from tests.helpers.agent_builders import agent, analyst_tool, compile_agents, compiled_agents, observe
from tests.helpers.diagnostic_filters import coded
from tests.helpers.snowflake_fake import FakeSnowflake


def _live(edit: dict[str, object]) -> list[Diagnostic]:
    result = compile_agents(agent("sales_agent", analyst_tool()))
    [compiled] = compiled_agents(result)
    spec = {**render_agent_spec(compiled.resolved.model, compiled.resolved.tools), **edit}
    port = FakeSnowflake()
    port.show_rows["AGENT DB.S.SALES_AGENT"] = {"owner": "TEST_ROLE"}
    port.descriptions["AGENT DB.S.SALES_AGENT"] = {"agent_spec": json.dumps(spec)}
    port.markers["DB.S.SALES_AGENT"] = OwnershipMarker("a" * 64, compiled.definition_fingerprint)
    return coded(observe(port, result), "SST-VAL509")


def test_sst_val509_fires() -> None:
    [diagnostic] = _live({"models": {"orchestration": "auto"}})
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': re-render differs from the live spec at models.orchestration"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val509_silent() -> None:
    assert _live({}) == []
