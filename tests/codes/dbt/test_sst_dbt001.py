"""SST-DBT001: the dbt manifest holds no model at all."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import SmallProject, found, manifest


def test_sst_dbt001_fires(tmp_path: Path) -> None:
    [diagnostic] = found(SmallProject(tmp_path, document=manifest({})).load(), "SST-DBT001")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "no dbt models available"
    assert diagnostic.subject is None


def test_sst_dbt001_silent(tmp_path: Path) -> None:
    assert found(SmallProject(tmp_path).load(), "SST-DBT001") == []
