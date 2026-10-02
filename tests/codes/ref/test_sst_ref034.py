"""SST-REF034: the legacy `table()` global is refused, located at the call."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.resolve_builders import resolved


def test_sst_ref034_fires() -> None:
    value, (diagnostic,) = resolved("{{ table('orders') }}", METRIC_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF034", Severity.ERROR)
    assert (
        diagnostic.message
        == "semantic_models/metrics/metrics.yml:1:1: '{ table('orders') }' is not a reference in 1.0; "
        "use '{ ref('orders') }'"
    )
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref034_silent() -> None:
    value, diagnostics = resolved("{{ ref('orders') }}", METRIC_EXPR)
    assert diagnostics == ()
    assert not value.poisoned
