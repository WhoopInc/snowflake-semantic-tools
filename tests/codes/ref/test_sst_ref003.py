"""SST-REF003: a template call's arguments do not parse, such as an unquoted model name."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.resolve_builders import resolved


def test_sst_ref003_fires() -> None:
    value, (diagnostic,) = resolved("{{ ref(orders) }}", METRIC_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF003", Severity.ERROR)
    assert (
        diagnostic.message
        == "semantic_models/metrics/metrics.yml:1:8: malformed template expression '{{ ref(orders) }}'"
    )
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref003_silent() -> None:
    value, diagnostics = resolved("{{ ref('orders') }}", METRIC_EXPR)
    assert diagnostics == ()
    assert not value.poisoned
