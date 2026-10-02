"""SST-PRS032: a defined tool's signature declares a parameter of type OBJECT."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolMember, ToolOwnership, ToolParameter
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog


def _found(kind: str) -> list[Diagnostic]:
    member = ToolMember(
        "group",
        "lookup",
        "procedure",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        body_file="lookup.sql",
        body="RETURN 1",
        signature=(ToolParameter("payload", kind, True),),
        relations=MappingProxyType({}),
    )
    catalog = ToolCatalog(
        (ToolGroup("group", Origin("tools.yml"), "tools.yml", members=(member,)),), "dev", frozenset(("dev",))
    )
    return [
        item for item in validate_tool_catalog(catalog, DbtCatalog("v12", None, None, ())) if item.code == "SST-PRS032"
    ]


def test_sst_prs032_fires() -> None:
    [diagnostic] = _found("object")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.endswith("input_schema property 'payload' has type 'object'")


def test_sst_prs032_silent() -> None:
    assert _found("VARCHAR") == []
