"""SST-PRS005: a name SST would render as an identifier is not one.

The reference project's relationship is renamed with a space, which no unquoted identifier may
hold; the loader reports it and leaves the relationship out rather than render it.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import project_copy
from tests.helpers.projects import load_project

MANIFEST = Path(__file__).resolve().parents[2] / "fixtures" / "reference_project_manifest.json"
RELATIONSHIPS = "semantic_models/relationships/relationships.yml"


def test_sst_prs005_fires(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    path = project / RELATIONSHIPS
    path.write_text(path.read_text().replace("name: orders_to_customers", "name: orders to customers", 1))
    [diagnostic] = [
        item for item in load_project(project, manifest_path=MANIFEST).diagnostics if item.code == "SST-PRS005"
    ]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "relationship:orders to customers: 'orders to customers' is not a valid identifier"
    assert diagnostic.subject == "relationship:orders to customers"


def test_sst_prs005_silent(tmp_path: Path) -> None:
    diagnostics = load_project(project_copy(tmp_path), manifest_path=MANIFEST).diagnostics
    assert "SST-PRS005" not in [item.code for item in diagnostics]
