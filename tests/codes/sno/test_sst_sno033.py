"""SST-SNO033: the synonym model answers, but SHOW CORTEX BASE MODELS lists it as a risk."""

from __future__ import annotations

from snowflake_semantic_tools.app.enrich import EnrichRequest
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.enrich import Component, resolve_options
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import CortexBaseModel
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.enrich_ports import ScriptedEnrich
from tests.helpers.sno_codes import ORDERS_COLUMNS, enrich_orders


def _table_synonyms() -> EnrichRequest:
    return EnrichRequest(options=resolve_options(frozenset({Component.TABLE_SYNONYMS}), frozenset()))


def _warned(*listed: CortexBaseModel) -> list[str]:
    port = ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS}, listed=listed)
    return [
        item.message for item in enrich_orders(port).run(_table_synonyms()).diagnostics if item.code == "SST-SNO033"
    ]


def test_sst_sno033_fires() -> None:
    port = ScriptedEnrich(
        columns={"DB.SCH.ORDERS": ORDERS_COLUMNS},
        listed=(CortexBaseModel("claude-sonnet-4-5", "LEGACY", "2026-09-01", "2027-01-15"),),
    )
    report = enrich_orders(port).run(_table_synonyms())
    [diagnostic] = [item for item in report.diagnostics if item.code == "SST-SNO033"]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "synonym model 'claude-sonnet-4-5' is legacy; end-of-life: 2027-01-15"
    # A warning: the run goes on.
    assert not report.stopped and port.calls


def test_sst_sno033_names_each_risk() -> None:
    assert _warned() == ["synonym model 'claude-sonnet-4-5' is not in SHOW CORTEX BASE MODELS"]
    assert _warned(CortexBaseModel("claude-sonnet-4-5", "PUPR")) == [
        "synonym model 'claude-sonnet-4-5' is in public preview"
    ]
    assert _warned(CortexBaseModel("CLAUDE-SONNET-4-5", "INTERNAL")) == [
        "synonym model 'claude-sonnet-4-5' is marked INTERNAL by SHOW CORTEX BASE MODELS"
    ]


def test_sst_sno033_silent() -> None:
    assert _warned(CortexBaseModel("claude-sonnet-4-5", "GA")) == []


def test_sst_sno033_silent_when_show_cannot_be_read() -> None:
    port = ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS}, show_failure=SnowflakePortError("denied"))
    report = enrich_orders(port).run(_table_synonyms())
    assert [item.code for item in report.diagnostics if item.code == "SST-SNO033"] == []
