"""SST-REF041: a call to a function the field does not accept, such as `metric()` in a filter."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import FILTER_EXPR
from tests.helpers.resolve_builders import resolved


def test_sst_ref041_fires() -> None:
    value, (diagnostic,) = resolved("{{ metric('m') }}", FILTER_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF041", Severity.ERROR)
    assert diagnostic.message == "metric:m: metric() is not allowed in metric.expression"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref041_silent() -> None:
    value, diagnostics = resolved("{{ ref('orders', 'id') }} > 0", FILTER_EXPR)
    assert diagnostics == ()
    assert not value.poisoned
