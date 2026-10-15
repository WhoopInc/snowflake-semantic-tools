"""SST-SNO032: the synonym model refuses enrich's one-token test completion, so the run stops."""

from __future__ import annotations

from snowflake_semantic_tools.app.enrich import EnrichRequest
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.enrich import Component, resolve_options
from tests.helpers.enrich_ports import ScriptedEnrich
from tests.helpers.sno_codes import ORDERS_COLUMNS, enrich_orders, enrich_types

REFUSED = "The model mistral-large2 has been in legacy state, please use other models."


def _synonyms() -> EnrichRequest:
    return EnrichRequest(options=resolve_options(frozenset({Component.COLUMN_SYNONYMS}), frozenset()))


def test_sst_sno032_fires() -> None:
    port = ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS}, refused_model=REFUSED)
    report = enrich_orders(port).run(_synonyms())
    [diagnostic] = report.diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO032", Severity.ERROR)
    assert diagnostic.message == f"synonym model 'claude-sonnet-4-5' refused a test completion: {REFUSED}"
    assert diagnostic.subject == "config:enrichment.synonym_model"
    # The run stops before any read, and writes nothing.
    assert report.stopped and report.writable == () and port.calls == []


def test_sst_sno032_silent_when_the_run_asks_cortex_nothing() -> None:
    port = ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS}, refused_model=REFUSED)
    assert enrich_orders(port).run(enrich_types()).diagnostics == ()
    assert port.probed == []


def test_sst_sno032_silent() -> None:
    port = ScriptedEnrich(columns={"DB.SCH.ORDERS": ORDERS_COLUMNS})
    report = enrich_orders(port).run(_synonyms())
    assert [item.code for item in report.diagnostics if item.code == "SST-SNO032"] == []
    assert not report.stopped and port.calls
    assert port.probed == ["claude-sonnet-4-5"]
