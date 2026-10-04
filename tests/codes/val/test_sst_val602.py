"""SST-VAL602: one member name is declared in two groups."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_builders import dbt_catalog, group, procedure_member
from tests.helpers.diagnostic_filters import coded


def _checked(*groups: ToolGroup) -> list[Diagnostic]:
    return list(validate_tool_catalog(ToolCatalog(groups, "dev", frozenset(("dev", "prod"))), dbt_catalog()))


def test_sst_val602_fires() -> None:
    found_602 = coded(
        _checked(group("platform", procedure_member()), group("partner", procedure_member())), "SST-VAL602"
    )
    [diagnostic] = found_602
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "tool member 'lookup' is declared in platform and partner"


def test_sst_val602_silent() -> None:
    groups = (group("platform", procedure_member()), group("partner", procedure_member("settle")))
    assert coded(_checked(*groups), "SST-VAL602") == []
