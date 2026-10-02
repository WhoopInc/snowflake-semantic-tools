"""SST-REF006: a `metric()` call names no declared metric."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.resolve_builders import resolved


def test_sst_ref006_fires() -> None:
    value, (diagnostic,) = resolved("{{ metric('missing') }}", METRIC_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF006", Severity.ERROR)
    assert diagnostic.message == "{ metric('missing') } does not resolve"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref006_silent() -> None:
    value, diagnostics = resolved("{{ metric('order_count') }}", METRIC_EXPR, metric_names=frozenset(("order_count",)))
    assert diagnostics == ()
    assert not value.poisoned
