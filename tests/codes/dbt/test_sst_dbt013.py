"""SST-DBT013: a manifest model node has no name; it is skipped."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.dbt.manifest import catalog_from_document
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import manifest, model_node


def test_sst_dbt013_fires() -> None:
    nodes = {"model.fixture.products": model_node("products"), "model.fixture.nameless": model_node("")}
    catalog = catalog_from_document(manifest(nodes, tested=False))
    [diagnostic] = catalog.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT013", Severity.ERROR)
    assert diagnostic.message == "dbt node model.fixture.nameless has no name and was skipped"
    assert [model.name for model in catalog.models] == ["products"]


def test_sst_dbt013_silent() -> None:
    assert catalog_from_document(manifest()).diagnostics == ()
