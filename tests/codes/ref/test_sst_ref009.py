"""SST-REF009: a call resolves, and to an empty string."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.resolve_builders import resolved


def test_sst_ref009_fires() -> None:
    value, (diagnostic,) = resolved("{{ var('blank') }}", METRIC_EXPR, variables={"blank": ""})
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF009", Severity.ERROR)
    assert diagnostic.message == "{ var('blank') } resolved to an empty string"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref009_silent() -> None:
    value, diagnostics = resolved("{{ var('blank') }}", METRIC_EXPR, variables={"blank": "x"})
    assert diagnostics == ()
    assert not value.poisoned
