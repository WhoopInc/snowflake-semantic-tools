"""SST-DIS201: `--exclude` left artifacts out of the run."""

from __future__ import annotations

from snowflake_semantic_tools.cli.wiring.compile import compiled_universe, selector_report
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.compiled_things import Thing

COMPILED = compiled_universe((Thing("orders"), Thing("lookup", "tool")))


def test_sst_dis201_fires() -> None:
    [diagnostic] = selector_report((), ("type:tool",), COMPILED)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DIS201", Severity.INFO)
    assert diagnostic.message == "1 files excluded by --exclude"


def test_sst_dis201_silent() -> None:
    # An exclusion that matched nothing left nothing out; SST-DIS011 says so instead.
    assert [item.code for item in selector_report((), ("type:agent",), COMPILED)] == ["SST-DIS011"]
