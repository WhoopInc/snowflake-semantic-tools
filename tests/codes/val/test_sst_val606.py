"""SST-VAL606: an immutable group declares defined members."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_builders import dbt_catalog, group, procedure_member, search_member
from tests.helpers.diagnostic_filters import coded


def _checked(*groups: ToolGroup) -> list[Diagnostic]:
    return list(validate_tool_catalog(ToolCatalog(groups, "dev", frozenset(("dev", "prod"))), dbt_catalog()))


def test_sst_val606_fires() -> None:
    [diagnostic] = coded(_checked(group("vendor", search_member(), immutable=True)), "SST-VAL606")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool group 'vendor' is immutable: true and declares define:"
    assert diagnostic.subject == "tool_group:vendor"


def test_sst_val606_silent() -> None:
    assert coded(_checked(group("vendor", procedure_member(), immutable=True)), "SST-VAL606") == []
