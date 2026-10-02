"""SST-SNO031: an enrich step, such as reading a model's columns, failed for a model."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.enrich_ports import ScriptedEnrich
from tests.helpers.sno_codes import ORDERS_COLUMNS, enrich_orders, enrich_types


def test_sst_sno031_fires() -> None:
    port = ScriptedEnrich(failures={"DB.SCH.ORDERS": SnowflakePortError("Object does not exist\nmore detail")})
    [diagnostic] = enrich_orders(port).run(enrich_types()).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO031", Severity.ERROR)
    assert diagnostic.message == "model 'orders': reading columns failed: Object does not exist"
    assert diagnostic.subject == "dbt_model:orders"


def test_sst_sno031_silent() -> None:
    port = ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS})
    assert [item.code for item in enrich_orders(port).run(enrich_types()).diagnostics] == []
