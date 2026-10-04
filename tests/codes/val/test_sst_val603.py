"""SST-VAL603: a member declares a type SST does not know."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_builders import dbt_catalog, group, procedure_member
from tests.helpers.diagnostic_filters import coded


def _checked(*groups: ToolGroup) -> list[Diagnostic]:
    return list(validate_tool_catalog(ToolCatalog(groups, "dev", frozenset(("dev", "prod"))), dbt_catalog()))


def test_sst_val603_fires() -> None:
    [diagnostic] = coded(_checked(group("platform", replace(procedure_member(), type="lambda"))), "SST-VAL603")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'lookup': type 'lambda' is not a known tool type"
    assert diagnostic.subject == "tool:lookup"


def test_sst_val603_silent() -> None:
    assert coded(_checked(group("platform", procedure_member())), "SST-VAL603") == []
