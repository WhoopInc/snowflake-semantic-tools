"""SST-DBT031: `sst enrich` selects, by name, a model with no relation to read columns from."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.dbt.manifest import catalog_from_document
from snowflake_semantic_tools.app.enrich import EnrichRequest, select_models
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import manifest, model_node

CATALOG = catalog_from_document(
    manifest(
        {"model.fixture.products": model_node("products"), "model.fixture.staged": model_node("staged", relation="")}
    )
)


def test_sst_dbt031_fires() -> None:
    _, [diagnostic] = select_models(CATALOG, EnrichRequest(selected=("staged",)))
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT031", Severity.WARNING)
    assert diagnostic.message == "model 'staged' has no relation, so sst enrich has no columns to read"
    assert diagnostic.subject == "dbt_model:staged"


def test_sst_dbt031_silent() -> None:
    # Not selected by name: an ephemeral model is simply not among the models enrich reads.
    assert select_models(CATALOG, EnrichRequest())[1] == []
