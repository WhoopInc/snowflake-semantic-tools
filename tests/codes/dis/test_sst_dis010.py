"""SST-DIS010: a `--select` selector matched no artifact."""

from __future__ import annotations

from snowflake_semantic_tools.cli.wiring.selectors import selector_report
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.compiled_things import Thing

COMPILED = (Thing("orders"), Thing("lookup", "tool"))


def test_sst_dis010_fires() -> None:
    [diagnostic] = selector_report(("orders", "ordrs"), (), COMPILED)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS010", Severity.WARNING)
    assert diagnostic.message == "--select ordrs matched no artifact"


def test_sst_dis010_silent() -> None:
    assert selector_report(("orders", "type:tool"), (), COMPILED) == ()
