"""SST-LOD018: a document names a sidecar file that is there and cannot be read."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.tools import load_tool_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from tests.helpers.diagnostic_filters import coded

TOOL = "tool:\n  name: lookup\n  type: procedure\n  body: \"{{ file('sql/lookup.sql') }}\"\n"


def _findings(tmp_path: Path, *, readable: bool) -> list[Diagnostic]:
    body = tmp_path / "tools" / "sql" / "lookup.sql"
    body.parent.mkdir(parents=True)
    body.write_text("SELECT 1", encoding="utf-8")
    (tmp_path / "tools" / "tools.yml").write_text(TOOL, encoding="utf-8")
    body.chmod(0o644 if readable else 0)
    try:
        catalog = load_tool_catalog(
            tmp_path, DbtCatalog("v12", None, None, ()), target_name="dev", declared_targets=frozenset(("dev",))
        )
    finally:
        body.chmod(0o644)
    return coded(catalog.diagnostics, "SST-LOD018")


def test_sst_lod018_fires(tmp_path: Path) -> None:
    [diagnostic] = _findings(tmp_path, readable=False)
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "tools/tools.yml: could not read sql/lookup.sql"
    assert diagnostic.subject == "tool:lookup"


def test_sst_lod018_silent(tmp_path: Path) -> None:
    assert _findings(tmp_path, readable=True) == []
