"""SST-VAL509: a live agent SST published unchanged no longer matches its rendered spec."""

from __future__ import annotations

import json

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from snowflake_semantic_tools.domain.render.agent import render_agent_spec
from tests.helpers.agent_builders import agent, analyst_tool, compile_agents, compiled_agents, observe
from tests.helpers.diagnostic_filters import coded
from tests.helpers.snowflake_fake import FakeSnowflake


def _live(edit: dict[str, object], newest_comment: str | None = None) -> list[Diagnostic]:
    result = compile_agents(agent("sales_agent", analyst_tool()))
    [compiled] = compiled_agents(result)
    spec = {**render_agent_spec(compiled.resolved.model, compiled.resolved.tools), **edit}
    port = FakeSnowflake()
    port.show_rows["AGENT DB.S.SALES_AGENT"] = {"owner": "TEST_ROLE"}
    port.descriptions["AGENT DB.S.SALES_AGENT"] = {"agent_spec": json.dumps(spec)}
    port.markers["DB.S.SALES_AGENT"] = OwnershipMarker("a" * 64, compiled.definition_fingerprint)
    if newest_comment is not None:
        port.agent_version_rows["DB.S.SALES_AGENT"] = (AgentVersion("VERSION$1", None, comment=newest_comment),)
    return coded(observe(port, result), "SST-VAL509")


def test_sst_val509_fires() -> None:
    [diagnostic] = _live({"models": {"orchestration": "auto"}})
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': re-render differs from the live spec at models.orchestration"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val509_silent() -> None:
    assert _live({}) == []
    # The marker is written before the specification, so a write that stopped before its
    # version was committed leaves the planned marker over the previous spec: the newest
    # version's comment fingerprints that spec, and no round-trip difference is reported.
    assert _live({"models": {"orchestration": "auto"}}, f"git:abc sst-spec:{'e' * 64}") == []
    # A version whose comment names no fingerprint, as before SST wrote one, is still compared.
    assert len(_live({"models": {"orchestration": "auto"}}, "git:abc")) == 1
