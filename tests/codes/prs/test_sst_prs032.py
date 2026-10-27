"""SST-PRS032: a generic tool's input_schema property has a type a tool argument cannot carry."""

from __future__ import annotations

from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import agent, catalog, compile_agents, generic_tool, procedure_member
from tests.helpers.diagnostic_filters import coded


def _found(kind: str) -> list[Diagnostic]:
    schema = {"type": "object", "properties": {"payload": {"type": kind}}, "required": ["payload"]}
    tool = generic_tool(input_schema=MappingProxyType(schema))
    return coded(
        compile_agents(agent("sales_agent", tool), tools=catalog(procedure_member())).diagnostics, "SST-PRS032"
    )


@pytest.mark.parametrize("kind", ["object", "array"])
def test_sst_prs032_fires(kind: str) -> None:
    [diagnostic] = _found(kind)
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"sales_agent: input_schema property 'payload' has type '{kind}'"
    assert diagnostic.subject == "agent:sales_agent"


@pytest.mark.parametrize("kind", ["string", "number", "integer", "boolean"])
def test_sst_prs032_silent(kind: str) -> None:
    assert _found(kind) == []
