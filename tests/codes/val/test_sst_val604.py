"""SST-VAL604: a defined member has nothing to create it from."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_builders import dbt_catalog, found, group, procedure_member


def _checked(*groups: ToolGroup) -> list[Diagnostic]:
    return list(validate_tool_catalog(ToolCatalog(groups, "dev", frozenset(("dev", "prod"))), dbt_catalog()))


def test_sst_val604_fires() -> None:
    member = procedure_member(reference=False, body_file=None, body=None)
    [diagnostic] = found(_checked(group("platform", member)), "SST-VAL604")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'lookup' is under define: and declares neither on: nor body_file:"
    assert diagnostic.subject == "tool:lookup"


def test_sst_val604_silent() -> None:
    assert found(_checked(group("platform", procedure_member(reference=False))), "SST-VAL604") == []
