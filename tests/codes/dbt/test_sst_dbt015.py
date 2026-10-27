"""SST-DBT015: a consumed model has no checksum, so change detection cannot see it change."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, manifest, model_node


def test_sst_dbt015_fires(tmp_path: Path) -> None:
    document = manifest({"model.fixture.products": model_node("products", complete=False)})
    [diagnostic] = coded(SmallProject(tmp_path, document=document).load().diagnostics, "SST-DBT015")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "model 'products' has no checksum; change detection is unusable"
    assert diagnostic.subject == "dbt_model:products"


def test_sst_dbt015_silent(tmp_path: Path) -> None:
    # A model no view consumes is not checked.
    nodes = {
        "model.fixture.products": model_node("products"),
        "model.fixture.spare": model_node("spare", complete=False),
    }
    assert coded(SmallProject(tmp_path, document=manifest(nodes)).load().diagnostics, "SST-DBT015") == []
