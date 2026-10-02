"""SST-DIS201: `--exclude` left artifacts out of the run."""

from __future__ import annotations

from snowflake_semantic_tools.cli.wiring.selectors import selector_report
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.compiled_things import Thing

COMPILED = (Thing("orders"), Thing("lookup", "tool"))


def test_sst_dis201_fires() -> None:
    [diagnostic] = selector_report((), ("type:tool",), COMPILED)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS201", Severity.INFO)
    assert diagnostic.message == "1 files excluded by --exclude"


def test_sst_dis201_silent() -> None:
    assert selector_report((), ("type:agent",), COMPILED) == ()
