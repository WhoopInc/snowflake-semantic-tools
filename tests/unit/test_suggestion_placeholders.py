"""Every raise site the source names a code at fills the placeholders that code's suggestion names.

A suggestion may name a shared placeholder its message does not, such as `use one of {expected}`.
`D` refuses a context without it (SST-INT901), so a raise site that omits it reports an internal
error instead of the problem. Calls that name their code by a variable, or spread their context
with `**`, are not checkable from the source; their codes' tests exercise them.
"""

from __future__ import annotations

import ast
from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, D
from snowflake_semantic_tools.domain.diagnostics.core import _placeholders
from snowflake_semantic_tools.domain.diagnostics.integrity import PLACEHOLDERS
from tests.helpers.code_guards import PACKAGE


def _suggestion_only() -> dict[str, frozenset[str]]:
    """Return, by code, the shared placeholders its suggestion names and its message does not."""
    found: dict[str, frozenset[str]] = {}
    for code, entry in ERROR_REGISTRY.items():
        extra = (_placeholders(entry.suggestion or "") & PLACEHOLDERS) - _placeholders(entry.template)
        if extra:
            found[code] = frozenset(extra)
    return found


def _calls(root: Path) -> list[tuple[str, str, ast.Call]]:
    """Return each `D("SST-...", ...)` call under `root` naming its code literally, with `path:line`."""
    found: list[tuple[str, str, ast.Call]] = []
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "D"):
                continue
            first = node.args[0] if node.args else None
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                found.append((first.value, f"{path.relative_to(root.parent).as_posix()}:{node.lineno}", node))
    return found


def _lacking(call: ast.Call, needed: frozenset[str]) -> frozenset[str]:
    """Return what of `needed` a call does not pass; nothing when it spreads a mapping."""
    if any(keyword.arg is None for keyword in call.keywords):
        return frozenset()
    return needed - {keyword.arg for keyword in call.keywords if keyword.arg is not None}


def test_each_literal_raise_site_fills_its_suggestion() -> None:
    needed = _suggestion_only()
    calls = [(code, where, call) for code, where, call in _calls(PACKAGE) if code in needed]

    assert calls, "no raise site names a code whose suggestion needs its own placeholder"
    assert [
        (where, code, sorted(_lacking(call, needed[code])))
        for code, where, call in calls
        if _lacking(call, needed[code])
    ] == []


def test_d_refuses_a_context_without_a_suggestion_placeholder() -> None:
    missing = D("SST-PRT102", value="semantic_view:lv_*")
    assert (missing.code, missing.message) == (
        "SST-INT901",
        "SST-PRT102 template needs expected, which was not supplied",
    )
    filled = D("SST-PRT102", value="nope:x", expected="one of type, path")
    assert (filled.code, filled.suggestion) == ("SST-PRT102", "use one of type, path")


def test_a_suggestions_other_braces_are_text_not_placeholders() -> None:
    # `{{ skill('<member>') }}` is an example written as text, which no context fills.
    assert not {"SST-VAL840", "SST-REF033"} & set(_suggestion_only())
