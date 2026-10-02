"""SST-PRS033: a generic tool's `input_schema.required` names a property it does not declare."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.compile.agents.resolve_tools import resolve_tool
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool
from tests.helpers.seam_projects import agent_context, procedure_catalog

AGENT = AgentModel("sales", Origin("agent.yml"), ("agent.yml",))


def _found(required: str) -> list[Diagnostic]:
    schema = {"type": "object", "properties": {"count": {"type": "integer"}}, "required": [required]}
    tool = AgentTool(
        "generic",
        Origin("agent.yml"),
        name="procedure",
        description="Runs the procedure.",
        backing=("platform", "procedure"),
        input_schema=MappingProxyType(schema),
    )
    _, diagnostics = resolve_tool(AGENT, tool, agent_context(procedure_catalog()))
    return [item for item in diagnostics if item.code == "SST-PRS033"]


def test_sst_prs033_fires() -> None:
    [diagnostic] = _found("missing")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.endswith("input_schema.required names 'missing', absent from properties")
    assert diagnostic.subject == "agent:sales"


def test_sst_prs033_silent() -> None:
    assert _found("count") == []
