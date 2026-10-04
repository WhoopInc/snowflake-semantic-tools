"""SST-VAL853: MCP config is not valid.

An `mcp.json` that is not the `mcpServers` shape is refused; the documented shape loads.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.profiles import load_profile_catalog
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.profile import ProfileCatalog
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.file_trees import write_tree


def load(root: Path, files: Mapping[str, str | bytes]) -> ProfileCatalog:
    write_tree(root, files)
    return load_profile_catalog(
        root, profiles_dir="profiles", hooks_dir="hooks", mcp_servers_dir="mcp-servers", commands_dir="commands"
    )


def test_sst_val853_fires(tmp_path: Path) -> None:
    catalog = load(tmp_path, {"mcp-servers/dbt/mcp.json": '{"servers": {}}'})
    diagnostic = only(catalog.diagnostics, "SST-VAL853")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "MCP config 'dbt': must hold exactly one mcpServers object"
    assert diagnostic.subject == "mcp:dbt"


def test_sst_val853_silent(tmp_path: Path) -> None:
    catalog = load(tmp_path, {"mcp-servers/dbt/mcp.json": '{"mcpServers": {"dbt": {"command": "dbt-mcp"}}}'})
    assert "SST-VAL853" not in codes(catalog.diagnostics)
