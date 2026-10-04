"""SST-VAL608: a defined search service's on: names no dbt model."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_builders import dbt_catalog, group, search_member
from tests.helpers.diagnostic_filters import coded


def _checked(*groups: ToolGroup) -> list[Diagnostic]:
    return list(validate_tool_catalog(ToolCatalog(groups, "dev", frozenset(("dev", "prod"))), dbt_catalog()))


def test_sst_val608_fires() -> None:
    [diagnostic] = coded(_checked(group("platform", search_member(on_model="product_notes"))), "SST-VAL608")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'docs_search': on: 'product_notes' is not a model in the dbt manifest"
    assert diagnostic.subject == "tool:docs_search"


def test_sst_val608_silent() -> None:
    assert coded(_checked(group("platform", search_member())), "SST-VAL608") == []
