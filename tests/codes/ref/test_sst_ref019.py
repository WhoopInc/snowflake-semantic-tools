"""SST-REF019: a referenced tool member's relation is not a three-part name."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog

ORIGIN = Origin("tools/platform.yml")


def _validated(relations: dict[str, str], code: str, *, targets: tuple[str, ...] = ("dev", "prod")) -> list[Diagnostic]:
    """The diagnostics of `code` for a referenced procedure `platform.lookup` with these relations."""
    member = ToolMember(
        "platform",
        "lookup",
        "procedure",
        ToolOwnership.REFERENCE,
        ORIGIN,
        "tools/platform.yml",
        relations=MappingProxyType(relations),
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", ORIGIN, "tools/platform.yml", members=(member,)),), "dev", frozenset(targets)
    )
    return [item for item in validate_tool_catalog(catalog, DbtCatalog("v12", None, None, ())) if item.code == code]


def test_sst_ref019_fires() -> None:
    [diagnostic] = _validated({"dev": "DEV.LOOKUP", "prod": "DB.PROD.LOOKUP"}, "SST-REF019")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "'DEV.LOOKUP' is not a three-part fully-qualified name"
    assert diagnostic.subject == "tool:lookup"


def test_sst_ref019_silent() -> None:
    assert _validated({"dev": "DB.DEV.LOOKUP", "prod": "DB.PROD.LOOKUP"}, "SST-REF019") == []
