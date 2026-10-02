"""SST-REF007: a `custom_instructions()` call names no declared custom instruction."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import CUSTOM_INSTRUCTION_ITEM
from tests.helpers.resolve_builders import resolved


def test_sst_ref007_fires() -> None:
    value, (diagnostic,) = resolved("{{ custom_instructions('missing') }}", CUSTOM_INSTRUCTION_ITEM)
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF007", Severity.ERROR)
    assert diagnostic.message == "{ custom_instructions('missing') } does not resolve"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref007_silent() -> None:
    value, diagnostics = resolved(
        "{{ custom_instructions('rules') }}", CUSTOM_INSTRUCTION_ITEM, instruction_names=frozenset(("rules",))
    )
    assert diagnostics == ()
    assert not value.poisoned
