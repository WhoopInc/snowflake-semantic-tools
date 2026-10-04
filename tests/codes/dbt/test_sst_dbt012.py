"""SST-DBT012: one `source.table` pair is declared more than once across the dbt project."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, manifest

SOURCE = {"source_name": "raw", "name": "orders", "relation_name": "db.raw.orders"}


def test_sst_dbt012_fires(tmp_path: Path) -> None:
    sources = {"source.a.raw.orders": SOURCE, "source.b.raw.orders": dict(SOURCE, relation_name="db.raw2.orders")}
    [diagnostic] = coded(SmallProject(tmp_path, document=manifest(sources=sources)).load().diagnostics, "SST-DBT012")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "source 'raw.orders' is declared more than once"
    assert diagnostic.subject is None


def test_sst_dbt012_silent(tmp_path: Path) -> None:
    sources = {"source.a.raw.orders": SOURCE, "source.a.raw.items": dict(SOURCE, name="items")}
    assert coded(SmallProject(tmp_path, document=manifest(sources=sources)).load().diagnostics, "SST-DBT012") == []
