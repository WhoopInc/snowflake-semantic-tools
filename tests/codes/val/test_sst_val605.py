"""SST-VAL605: a referenced member declares how to create itself."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_builders import dbt_catalog, group, procedure_member
from tests.helpers.diagnostic_filters import coded


def _checked(*groups: ToolGroup) -> list[Diagnostic]:
    return list(validate_tool_catalog(ToolCatalog(groups, "dev", frozenset(("dev", "prod"))), dbt_catalog()))


def test_sst_val605_fires() -> None:
    member = procedure_member(creation_keys=("body_file",))
    [diagnostic] = coded(_checked(group("platform", member)), "SST-VAL605")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tool member 'lookup' is under reference: and declares 'body_file'"
    assert diagnostic.subject == "tool:lookup"


def test_sst_val605_silent() -> None:
    assert coded(_checked(group("platform", procedure_member())), "SST-VAL605") == []
