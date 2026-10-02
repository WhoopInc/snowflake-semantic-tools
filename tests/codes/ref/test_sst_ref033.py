"""SST-REF033: a `{{ ... }}` span is not a `fn(args)` call at all."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.resolve_builders import resolved


def test_sst_ref033_fires() -> None:
    value, (diagnostic,) = resolved("{{ sha_version }}", METRIC_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF033", Severity.ERROR)
    assert (
        diagnostic.message == "semantic_models/metrics/metrics.yml:1:16: '{{ sha_version }}' is not a valid reference: "
        "expected '(' after sha_version"
    )
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref033_silent() -> None:
    value, diagnostics = resolved("{{ var('sha_version') }}", METRIC_EXPR, variables={"sha_version": "abc1234"})
    assert diagnostics == ()
    assert not value.poisoned
