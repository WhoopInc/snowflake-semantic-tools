"""SST-DBT002: `sst enrich` selects, by exact name, a model the manifest does not have."""

from __future__ import annotations

from snowflake_semantic_tools.app.enrich import EnrichRequest, select_models
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel

CATALOG = DbtCatalog("v12", None, "fixture", (DbtModel("model.fixture.orders", "orders", "DB.SCH.ORDERS", (), (), ()),))


def test_sst_dbt002_fires() -> None:
    models, [diagnostic] = select_models(CATALOG, EnrichRequest(selected=("ordrs",)))
    assert models == ()
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT002", Severity.ERROR)
    assert diagnostic.message == "model 'ordrs' is not in the dbt manifest"
    assert diagnostic.subject == "dbt_model:ordrs"


def test_sst_dbt002_silent() -> None:
    # A glob that matches nothing selects nothing, without naming a model that is absent.
    assert select_models(CATALOG, EnrichRequest(selected=("orders",)))[1] == []
    assert select_models(CATALOG, EnrichRequest(selected=("ord_*",)))[1] == []
