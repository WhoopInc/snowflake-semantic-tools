"""SST-VAL859: command file is invalid.

A command whose frontmatter never closes is refused; frontmatter is optional, so a plain body loads.
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


def test_sst_val859_fires(tmp_path: Path) -> None:
    catalog = load(tmp_path, {"commands/sql/check.md": "---\ndescription: x\n"})
    diagnostic = only(catalog.diagnostics, "SST-VAL859")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "command 'sql/check': frontmatter opens with --- but never closes"
    assert diagnostic.subject == "command:sql/check"


def test_sst_val859_silent(tmp_path: Path) -> None:
    catalog = load(tmp_path, {"commands/sql/check.md": "Check it.\n"})
    assert "SST-VAL859" not in codes(catalog.diagnostics)
