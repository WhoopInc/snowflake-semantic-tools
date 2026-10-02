"""SST-REF001: a one-argument `ref()` names a model the dbt manifest does not list."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.resolve_builders import resolved


def test_sst_ref001_fires() -> None:
    value, (diagnostic,) = resolved("{{ ref('missing') }}", METRIC_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF001", Severity.ERROR)
    assert diagnostic.message == "{ ref('missing') } is not a model in the dbt manifest"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref001_silent() -> None:
    value, diagnostics = resolved("{{ ref('orders') }}", METRIC_EXPR)
    assert diagnostics == ()
    assert not value.poisoned
