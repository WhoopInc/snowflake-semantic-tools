"""SST-SNO030: during enrich, a model's relation does not exist or the role cannot see it."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.enrich_ports import ScriptedEnrich
from tests.helpers.sno_codes import ORDERS_COLUMNS, enrich_orders, enrich_types


def test_sst_sno030_fires() -> None:
    [diagnostic] = enrich_orders(ScriptedEnrich(columns={})).run(enrich_types()).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO030", Severity.ERROR)
    assert diagnostic.message == "model 'orders': DB.SCH.ORDERS does not exist, or the role cannot see it"
    assert diagnostic.subject == "dbt_model:orders"


def test_sst_sno030_silent() -> None:
    port = ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS})
    assert enrich_orders(port).run(enrich_types()).diagnostics == ()
