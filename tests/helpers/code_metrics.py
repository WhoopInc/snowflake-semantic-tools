"""Static code metrics shared by the structure, docstring and import gates, using only `ast`.

`package_modules` and `python_modules` parse the code each gate reads: the package alone, or
the package and the test suite (not its fixtures or goldens, which are data).
`definitions` walks a module and yields every class and function with its dotted
qualified name. `complexity` is a McCabe-style count: 1, plus one per `if`/`elif`,
conditional expression, loop, `except` handler, `match` case and comprehension
clause, plus one per extra operand of `and`/`or`. Nested functions and classes are
measured on their own and do not add to the function that contains them.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE = REPO_ROOT / "snowflake_semantic_tools"
TESTS = REPO_ROOT / "tests"
TEST_DATA = (TESTS / "fixtures", TESTS / "golden")

FunctionNode = ast.FunctionDef | ast.AsyncFunctionDef
DefinitionNode = FunctionNode | ast.ClassDef
_DECISIONS = (ast.If, ast.IfExp, ast.For, ast.AsyncFor, ast.While, ast.ExceptHandler, ast.match_case)


@dataclass(frozen=True)
class Definition:
    """One class or function, located by module path and dotted name."""

    path: str
    qualname: str
    node: DefinitionNode
    in_class: bool
    in_function: bool

    @property
    def key(self) -> str:
        return f"{self.path}::{self.qualname}"

    @property
    def lines(self) -> int:
        return int(self.node.end_lineno or self.node.lineno) - self.node.lineno + 1


def package_modules() -> Iterator[tuple[str, ast.Module, str]]:
    """Every package module as (repository-relative path, parsed tree, source text)."""
    yield from _modules(PACKAGE)


def python_modules() -> Iterator[tuple[str, ast.Module, str]]:
    """Every module in the package and the test suite, the package first, as `package_modules` yields them."""
    yield from _modules(PACKAGE)
    yield from _modules(TESTS)


def _modules(root: Path) -> Iterator[tuple[str, ast.Module, str]]:
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts or any(path.is_relative_to(data) for data in TEST_DATA):
            continue
        text = path.read_text(encoding="utf-8")
        yield path.relative_to(REPO_ROOT).as_posix(), ast.parse(text), text


def definitions(path: str, tree: ast.Module) -> Iterator[Definition]:
    """Every class and function in a module, outermost first."""

    def walk(body: list[ast.stmt], prefix: str, in_class: bool, in_function: bool) -> Iterator[Definition]:
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                qualname = f"{prefix}{node.name}"
                yield Definition(path, qualname, node, in_class, in_function)
                is_function = not isinstance(node, ast.ClassDef)
                yield from walk(node.body, f"{qualname}.", not is_function, in_function or is_function)
            elif isinstance(node, (ast.If, ast.Try, ast.With, ast.For, ast.While)):
                # Definitions guarded by a condition or try block are still members of the enclosing scope.
                nested: list[ast.stmt] = []
                for field in ("body", "orelse", "finalbody"):
                    nested.extend(getattr(node, field, []) or [])
                for handler in getattr(node, "handlers", []) or []:
                    nested.extend(handler.body)
                yield from walk(nested, prefix, in_class, in_function)

    yield from walk(tree.body, "", False, False)


def _own_nodes(function: FunctionNode) -> Iterator[ast.AST]:
    stack: list[ast.AST] = list(function.body)
    while stack:
        node = stack.pop()
        yield node
        for child in ast.iter_child_nodes(node):
            if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                stack.append(child)


def complexity(function: FunctionNode) -> int:
    """The McCabe-style decision count described in the module docstring."""
    score = 1
    for node in _own_nodes(function):
        if isinstance(node, _DECISIONS):
            score += 1
        elif isinstance(node, ast.comprehension):
            score += 1 + len(node.ifs)
        elif isinstance(node, ast.BoolOp):
            score += len(node.values) - 1
    return score
