"""SST-VAL619: a search service being replaced holds explicit grants a replay must restore."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow
from tests.helpers.agent_builders import compile_tools, found, observe, search_member
from tests.helpers.app_ports import InMemorySnowflake


def _granted(*grants: GrantRow) -> list[Diagnostic]:
    port = InMemorySnowflake()
    port.descriptions["CORTEX SEARCH SERVICE DB.S.DOCS_SEARCH"] = {"name": "DOCS_SEARCH"}
    port.grants["DB.S.DOCS_SEARCH"] = grants
    return found(observe(port, compile_tools((search_member(),))), "SST-VAL619")


def test_sst_val619_fires() -> None:
    [diagnostic] = _granted(GrantRow("OWNERSHIP", "ROLE", "DEPLOYER"), GrantRow("USAGE", "ROLE", "ANALYST"))
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "tool member 'docs_search': 1 explicit grant(s) will be captured and replayed -- they do not exist "
        "between commit and replay"
    )
    assert diagnostic.subject == "tool:docs_search"


def test_sst_val619_silent() -> None:
    # OWNERSHIP is re-established on the new object, so it is never replayed.
    assert _granted(GrantRow("OWNERSHIP", "ROLE", "DEPLOYER")) == []
