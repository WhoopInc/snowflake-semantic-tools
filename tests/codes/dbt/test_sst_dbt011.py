"""SST-DBT011: a view names a `source()` that no dbt source declares."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, manifest

VIEW = (
    "semantic_views:\n  - name: catalog\n"
    "    description: Use this view for questions about the product catalog.\n    tables:\n"
    "      - \"{{ ref('products') }}\"\n      - \"{{ source('raw', 'orders') }}\"\n"
)
SOURCE = {"source_name": "raw", "name": "orders", "relation_name": "db.raw.orders"}


def test_sst_dbt011_fires(tmp_path: Path) -> None:
    project = SmallProject(tmp_path, files={"semantic_models/semantic_views/views.yml": VIEW}).load()
    [diagnostic] = found(project, "SST-DBT011")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "source 'raw.orders' is not declared in dbt"
    assert diagnostic.subject == "semantic_view:catalog"
    assert project.views == ()


def test_sst_dbt011_silent(tmp_path: Path) -> None:
    document = manifest(sources={"source.fixture.raw.orders": SOURCE})
    project = SmallProject(tmp_path, files={"semantic_models/semantic_views/views.yml": VIEW}, document=document).load()
    assert found(project, "SST-DBT011") == []
    assert [(table.logical_name, table.fqn) for table in project.views[0].tables] == [
        ("PRODUCTS", "DB.SCH.PRODUCTS"),
        ("ORDERS", "DB.RAW.ORDERS"),
    ]
