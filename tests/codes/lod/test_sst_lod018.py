"""SST-LOD018: a document names a sidecar file that does not exist."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.diagnostic_filters import coded

ORIGIN = Origin("tools/tools.yml", 3, 5)


def _findings(body_file: str, *, present: bool) -> list[Diagnostic]:
    member = ToolMember(
        "group",
        "lookup",
        "procedure",
        ToolOwnership.DEFINE,
        ORIGIN,
        "tools/tools.yml",
        body_file=body_file,
        body="SELECT 1" if present else None,
    )
    catalog = ToolCatalog(
        (ToolGroup("group", ORIGIN, "tools/tools.yml", members=(member,)),), "dev", frozenset(("dev",))
    )
    return coded(validate_tool_catalog(catalog, DbtCatalog("v12", None, None, ())), "SST-LOD018")


def test_sst_lod018_fires() -> None:
    [diagnostic] = _findings("sql/lookup.sql", present=False)
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tools/tools.yml: could not read sql/lookup.sql"
    assert diagnostic.subject == "tool:lookup"


def test_sst_lod018_silent() -> None:
    assert _findings("sql/lookup.sql", present=True) == []
