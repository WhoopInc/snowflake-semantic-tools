"""Every `ProjectError` the package builds carries the coded diagnostics that report it.

A refusal with no diagnostic reaches the runner as a bare `error:` line with an empty
`diagnostics` list, so the run names no SST code. The type demands `diagnostics=`; this scan
also refuses an empty literal, which the type allows.
"""

from __future__ import annotations

import ast
from pathlib import Path

from tests.helpers.code_guards import PACKAGE


def _constructions(root: Path) -> list[tuple[str, ast.Call]]:
    """Return each call of `ProjectError` under `root`, with its `path:line`."""
    found: list[tuple[str, ast.Call]] = []
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        relative = path.relative_to(root.parent).as_posix()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call):
                continue
            name = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", None)
            if name == "ProjectError":
                found.append((f"{relative}:{node.lineno}", node))
    return found


def _uncoded(call: ast.Call) -> bool:
    """Report whether a call passes no `diagnostics=`, or passes it an empty tuple or list literal."""
    passed = next((keyword.value for keyword in call.keywords if keyword.arg == "diagnostics"), None)
    if passed is None:
        return not any(keyword.arg is None for keyword in call.keywords)
    return isinstance(passed, (ast.Tuple, ast.List)) and not passed.elts


def test_every_project_error_passes_its_diagnostics() -> None:
    constructions = _constructions(PACKAGE)

    assert len(constructions) > 50
    assert [where for where, call in constructions if _uncoded(call)] == []


def test_the_scan_flags_a_refusal_with_no_code() -> None:
    def call(source: str) -> ast.Call:
        expression = ast.parse(source, mode="eval").body
        assert isinstance(expression, ast.Call)
        return expression

    assert _uncoded(call("ProjectError('no code')"))
    assert _uncoded(call("errors.ProjectError('no code', diagnostics=())"))
    assert not _uncoded(call("ProjectError(diagnostics=(found,))"))
    assert not _uncoded(call("ProjectError(**context)"))
