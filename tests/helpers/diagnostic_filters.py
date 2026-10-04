"""Pick diagnostics out of what a run reported: by code, the one of a code, or every code.

Every test that asserts on diagnostics reads them through these, whatever reported them: a
load, a compile, a plan's change set (`changeset.diagnostics`), or an apply run
(`result.diagnostics`).
"""

from __future__ import annotations

from collections.abc import Iterable

from snowflake_semantic_tools.domain.diagnostics import Diagnostic


def coded(diagnostics: Iterable[Diagnostic], code: str) -> list[Diagnostic]:
    """The diagnostics carrying `code`, in report order."""
    return [item for item in diagnostics if item.code == code]


def only(diagnostics: Iterable[Diagnostic], code: str) -> Diagnostic:
    """The one diagnostic carrying `code`, failing the test when there is none or several."""
    found = coded(diagnostics, code)
    assert len(found) == 1, f"expected one {code}, found {[item.message for item in found]}"
    return found[0]


def codes(diagnostics: Iterable[Diagnostic]) -> list[str]:
    """Every diagnostic's code, in report order."""
    return [item.code for item in diagnostics]
