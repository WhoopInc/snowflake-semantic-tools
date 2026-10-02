"""SST-VAL618: a search service's source relation has change tracking off."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.agent_builders import compile_tools, found, observe, search_member
from tests.helpers.app_ports import InMemorySnowflake


def _tracked(state: str) -> list[Diagnostic]:
    port = InMemorySnowflake()
    port.show_rows["TABLE DB.MARTS.PRODUCT_DOCS"] = {"change_tracking": state}
    return found(observe(port, compile_tools((search_member(),))), "SST-VAL618")


def test_sst_val618_fires() -> None:
    [diagnostic] = _tracked("OFF")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "DB.MARTS.PRODUCT_DOCS has change_tracking = OFF; service 'docs_search' cannot refresh incrementally and "
        "is serving stale data"
    )
    assert diagnostic.subject == "tool:docs_search"


def test_sst_val618_silent() -> None:
    assert _tracked("ON") == []
