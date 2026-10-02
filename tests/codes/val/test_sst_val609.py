"""SST-VAL609: a search column is not on the service's model."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_builders import dbt_catalog, found, group, search_member


def _checked(*groups: ToolGroup) -> list[Diagnostic]:
    return list(validate_tool_catalog(ToolCatalog(groups, "dev", frozenset(("dev", "prod"))), dbt_catalog()))


def test_sst_val609_fires() -> None:
    [diagnostic] = found(_checked(group("platform", search_member(attribute_columns=("REGION",)))), "SST-VAL609")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'docs_search': 'REGION' is not on 'product_docs'"
    assert diagnostic.subject == "tool:docs_search"


def test_sst_val609_silent() -> None:
    assert found(_checked(group("platform", search_member(attribute_columns=("DOC_ID",)))), "SST-VAL609") == []
