"""SST-DBT005: a manifest given with `--manifest` is older than a model file it describes."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, manifest, model_node

SQL = "select 1 as products_id\n"


def _project(tmp_path: Path, on_disk: str) -> SmallProject:
    node = model_node("products", original_file_path="models/products.sql", package_name="fixture")
    node["checksum"] = {"name": "sha256", "checksum": sha256(SQL.encode()).hexdigest()}
    return SmallProject(
        tmp_path, files={"models/products.sql": on_disk}, document=manifest({"model.fixture.products": node})
    )


def test_sst_dbt005_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(_project(tmp_path, "select 2 as products_id\n").load().diagnostics, "SST-DBT005")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "manifest is older than models/products.sql"
    assert diagnostic.subject == "dbt_model:products"
    assert diagnostic.origin == Origin("models/products.sql")


def test_sst_dbt005_silent(tmp_path: Path) -> None:
    assert coded(_project(tmp_path, SQL).load().diagnostics, "SST-DBT005") == []
