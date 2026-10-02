"""SST-CFG029: a `var()` call names no variable declared under `vars:`."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.resolve_builders import resolved


def test_sst_cfg029_fires() -> None:
    value, (diagnostic,) = resolved("{{ var('missing') }}", METRIC_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG029", Severity.ERROR)
    assert diagnostic.message == "{ var('missing') } is not declared in config"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_cfg029_silent() -> None:
    value, diagnostics = resolved("{{ var('state') }}", METRIC_EXPR, variables={"state": "completed"})
    assert diagnostics == ()
    assert not value.poisoned
