"""SST-REF015: a template call has an argument count its function does not take."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.resolve_builders import resolved


def test_sst_ref015_fires() -> None:
    value, (diagnostic,) = resolved("{{ ref() }}", METRIC_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF015", Severity.ERROR)
    assert diagnostic.message == "{ ref() } takes 1 or 2 arguments, found 0"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref015_silent() -> None:
    value, diagnostics = resolved("{{ ref('orders') }}", METRIC_EXPR)
    assert diagnostics == ()
    assert not value.poisoned
