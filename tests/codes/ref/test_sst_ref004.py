"""SST-REF004: a template call names a function the dialect does not have."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.resolve_builders import resolved


def test_sst_ref004_fires() -> None:
    value, (diagnostic,) = resolved("{{ refs('orders') }}", METRIC_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF004", Severity.ERROR)
    assert diagnostic.message == "'refs' is not a template function"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref004_silent() -> None:
    value, diagnostics = resolved("{{ ref('orders') }}", METRIC_EXPR)
    assert diagnostics == ()
    assert not value.poisoned
