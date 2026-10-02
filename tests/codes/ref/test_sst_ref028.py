"""SST-REF028: a `tag()` call names no tag declared under `tags:`."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import TAG_NAME
from tests.helpers.resolve_builders import resolved


def test_sst_ref028_fires() -> None:
    value, (diagnostic,) = resolved("{{ tag('missing') }}", TAG_NAME)
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF028", Severity.ERROR)
    assert diagnostic.message == "{ tag('missing') } does not resolve"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref028_silent() -> None:
    value, diagnostics = resolved("{{ tag('tier') }}", TAG_NAME, tags={"tier": "DB.SCH.TIER"})
    assert diagnostics == ()
    assert not value.poisoned
