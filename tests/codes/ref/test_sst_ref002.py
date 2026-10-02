"""SST-REF002: a two-argument `ref()` names a column its model does not have."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.resolve_builders import resolved


def test_sst_ref002_fires() -> None:
    value, (diagnostic,) = resolved("{{ ref('orders', 'nope') }}", METRIC_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF002", Severity.ERROR)
    assert diagnostic.message == "{ ref('orders','nope') }: 'nope' is not a column on orders"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref002_silent() -> None:
    value, diagnostics = resolved("{{ ref('orders', 'id') }}", METRIC_EXPR)
    assert diagnostics == ()
    assert not value.poisoned
