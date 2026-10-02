"""SST-DBT016: one model feeds more than one built artifact, so its blast radius is wide."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found

OTHER = (
    "semantic_views:\n  - name: other\n    description: Another view.\n    tables:\n      - \"{{ ref('products') }}\"\n"
)


def test_sst_dbt016_fires(tmp_path: Path) -> None:
    project = SmallProject(tmp_path, files={"semantic_models/semantic_views/other.yml": OTHER}).load()
    [diagnostic] = found(project, "SST-DBT016")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "model 'products' feeds 2 artifacts"
    assert diagnostic.subject == "dbt_model:products"


def test_sst_dbt016_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path).load(), "SST-DBT016") == []
