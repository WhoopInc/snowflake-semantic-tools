"""SST-PLN900: a computed order places a change before one it depends on."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.plan.order import check_order
from tests.helpers.artifact_builders import change, rendered


def test_sst_pln900_fires() -> None:
    dependency = change(rendered("BASE"))
    dependent = change(rendered("TOP", depends_on=(dependency.key,)))
    diagnostic = check_order((dependent, dependency))
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN900", Severity.ERROR)
    assert diagnostic.message == "change order violates 'semantic_view:top' before its dependency 'semantic_view:base'"


def test_sst_pln900_silent() -> None:
    dependency = change(rendered("BASE"))
    dependent = change(rendered("TOP", depends_on=(dependency.key,)))
    assert check_order((dependency, dependent)) is None
