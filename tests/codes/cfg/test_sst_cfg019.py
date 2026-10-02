"""SST-CFG019: one tool group declares a member under both `define:` and `reference:`."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog

DBT = DbtCatalog(schema_version="", dbt_version=None, project_name=None, models=())


def _catalog(*ownerships: ToolOwnership) -> ToolCatalog:
    members = tuple(
        ToolMember(
            "platform",
            f"Docs{index}" if len(set(ownerships)) == 1 else "docs",
            "stage",
            ownership,
            Origin("tools/p.yml", index + 1),
            "tools/p.yml",
        )
        for index, ownership in enumerate(ownerships)
    )
    return ToolCatalog(
        (ToolGroup("platform", Origin("tools/p.yml", 1), "tools/p.yml", members=members),), "dev", frozenset(("dev",))
    )


def test_sst_cfg019_fires() -> None:
    found = list(validate_tool_catalog(_catalog(ToolOwnership.DEFINE, ToolOwnership.REFERENCE), DBT))
    [diagnostic] = [item for item in found if item.code == "SST-CFG019"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "'docs' appears under both define: and reference:"
    assert diagnostic.subject == "tool_group:platform"
    assert "SST-VAL601" not in [item.code for item in found]


def test_sst_cfg019_silent() -> None:
    found = [item.code for item in validate_tool_catalog(_catalog(ToolOwnership.DEFINE, ToolOwnership.DEFINE), DBT)]
    assert "SST-CFG019" not in found
