"""Import rules for every module in the package and the test suite, checked with `ast`.

Each violation is keyed `<rule> <path>:<line>`:

- `relative-import`: an import written relative to the importing module, such as
  `from .dbt import DbtCatalog` or `from .. import main`, anywhere in the module, including
  inside a function or an `if TYPE_CHECKING:` block. Name the module by its full dotted path
  instead (`from snowflake_semantic_tools.domain.model.dbt import DbtCatalog`), so a search for
  a module's name finds every module that imports it.
- `test-support-import`: a test importing a conftest or another test module. Pytest imports
  those under names of its own (`app.conftest`, because `tests/unit` is not a package), so
  importing one by its full name loads a second copy. Code that tests share belongs in
  `tests/helpers/` and is imported as `tests.helpers.<module>`.

The `run_recorded_*` scripts import their siblings by bare name (`from recorded_snowflake
import ...`); that is an absolute import, and it is how a script run as a file finds them.
"""

from __future__ import annotations

import ast
from typing import Iterator

from tests.helpers.code_metrics import python_modules

RULES = {
    "relative-import": "every import names its module by the full dotted path",
    "test-support-import": "tests share code only through tests.helpers",
}


def module_violations(path: str, tree: ast.Module) -> Iterator[str]:
    """Violations in one module, by key; `path` is relative to the repository root."""
    in_tests = path.startswith("tests/")
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level:
            yield f"relative-import {path}:{node.lineno}"
        elif in_tests and any(_is_test_module(name) for name in _imported_modules(node)):
            yield f"test-support-import {path}:{node.lineno}"


def _imported_modules(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if isinstance(node, ast.ImportFrom) and node.module:
        return [node.module]
    return []


def _is_test_module(module: str) -> bool:
    """Whether `module` is under `tests` but outside `tests.helpers`."""
    parts = module.split(".")
    return parts[0] == "tests" and parts[1:2] != ["helpers"]


def violations() -> list[str]:
    """Every import violation in the package and the test suite, sorted."""
    return sorted(key for path, tree, _ in python_modules() for key in module_violations(path, tree))
