"""Size and complexity rules for package code, measured with `tests.helpers.code_metrics`.

Each violation is keyed `<rule> <path>[::<qualname>]` and carries the measured value,
so a failure names how far past its budget the code is.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator

from tests.helpers.code_metrics import Definition, complexity, definitions, package_modules

MODULE_LINES = 800
FUNCTION_LINES = 100
FUNCTION_COMPLEXITY = 25
NESTED_FUNCTION_LINES = 25

RULES = {
    "module-lines": f"a module holds at most {MODULE_LINES} lines",
    "function-lines": f"a function holds at most {FUNCTION_LINES} lines",
    "function-complexity": f"a function scores at most {FUNCTION_COMPLEXITY} on the decision count",
    "nested-function-lines": f"a function defined inside another holds at most {NESTED_FUNCTION_LINES} lines",
}


def module_violations(path: str, tree: ast.Module, text: str) -> Iterator[tuple[str, int]]:
    """Violations in one module, as (key, measured value)."""
    lines = len(text.splitlines())
    if lines > MODULE_LINES:
        yield f"module-lines {path}", lines
    for definition in definitions(path, tree):
        if isinstance(definition.node, ast.ClassDef):
            continue
        yield from _function_violations(definition)


def _function_violations(definition: Definition) -> Iterator[tuple[str, int]]:
    assert not isinstance(definition.node, ast.ClassDef)
    if definition.in_function and definition.lines > NESTED_FUNCTION_LINES:
        yield f"nested-function-lines {definition.key}", definition.lines
    if definition.lines > FUNCTION_LINES:
        yield f"function-lines {definition.key}", definition.lines
    score = complexity(definition.node)
    if score > FUNCTION_COMPLEXITY:
        yield f"function-complexity {definition.key}", score


def violations() -> dict[str, int | None]:
    """Every structure violation in the package."""
    found: dict[str, int | None] = {}
    for path, tree, text in package_modules():
        found.update(module_violations(path, tree, text))
    return found
