"""SST-INT902: rendering an input that validated raised."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import compile_each
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity
from tests.helpers.compiled_things import Thing


def render(label: str) -> Thing:
    if label == "bad":
        raise ValueError("no tables")
    return Thing(label)


def test_sst_int902_fires() -> None:
    result = compile_each(("bad",), lambda label: f"semantic_view:{label}", render, DiagnosticBag())
    [diagnostic] = result.diagnostics
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-INT902",
        Severity.ERROR,
        "semantic_view:bad",
    )
    assert diagnostic.message == "domain invariant violated: no tables"


def test_sst_int902_silent() -> None:
    result = compile_each(("good",), lambda label: f"semantic_view:{label}", render, DiagnosticBag())
    assert result.diagnostics == () and len(result.compiled) == 1
