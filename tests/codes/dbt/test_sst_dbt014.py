"""SST-DBT014: a model has a relation but an empty `database` or `schema`, so cannot be located."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.dbt.manifest import catalog_from_document
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import manifest, model_node


def test_sst_dbt014_fires() -> None:
    catalog = catalog_from_document(
        manifest({"model.fixture.products": model_node("products", database="", schema="SCH")})
    )
    [diagnostic] = catalog.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT014", Severity.ERROR)
    assert diagnostic.message == "model 'products' has empty database"
    assert diagnostic.subject == "dbt_model:products"
    assert catalog.models == ()


def test_sst_dbt014_silent() -> None:
    node = model_node("products", database="DB", schema="SCH")
    assert catalog_from_document(manifest({"model.fixture.products": node})).diagnostics == ()
