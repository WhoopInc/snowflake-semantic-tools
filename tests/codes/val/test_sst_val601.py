"""SST-VAL601: one group declares a member name twice."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_builders import dbt_catalog, found, group, procedure_member


def _checked(*groups: ToolGroup) -> list[Diagnostic]:
    return list(validate_tool_catalog(ToolCatalog(groups, "dev", frozenset(("dev", "prod"))), dbt_catalog()))


def test_sst_val601_fires() -> None:
    [diagnostic] = found(
        _checked(group("platform", procedure_member("lookup"), procedure_member("Lookup"))), "SST-VAL601"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool group 'platform': member 'lookup' is declared twice"


def test_sst_val601_silent() -> None:
    assert found(_checked(group("platform", procedure_member("lookup"), procedure_member("tier"))), "SST-VAL601") == []
