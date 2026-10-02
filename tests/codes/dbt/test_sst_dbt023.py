"""SST-DBT023: a model a view consumes has no dbt test attached."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, manifest


def test_sst_dbt023_fires(tmp_path: Path) -> None:
    [diagnostic] = found(SmallProject(tmp_path, document=manifest(tested=False)).load(), "SST-DBT023")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "model 'products' has no tests"
    assert diagnostic.subject == "dbt_model:products"


def test_sst_dbt023_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path).load(), "SST-DBT023") == []
