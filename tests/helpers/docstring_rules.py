"""Docstring rules for package code; see "Docstrings and comments" in CONTRIBUTING.md.

Rules, keyed `<rule> <path>[::<qualname>]`:

- docstring-module: every module has a docstring.
- docstring-class: every public class has one.
- docstring-function: every public function and method has one -- including `Protocol`
  methods, whose docstring is the contract -- unless it overrides a documented method of
  a base class defined in this package.
- docstring-private: a private function has one when it is 25 lines or longer or scores
  10 or more on the decision count.
- docstring-summary: a docstring opens with one summary line of at most 100 characters
  ending in a period, and a longer docstring leaves the second line blank.
- docstring-codes: every code named in a `Diagnostics:` section is registered.
- docstring-fields: every public dataclass in `domain/model/` and `api/` lists each field it
  declares under `Attributes:`; one line may list several (`a, b: ...`). A field of a base
  class is documented there.
- docstring-authored: each table an `Authored keys:` line names, as
  `authored_schema.<NAME>`, is one `domain.model.authored_schema` exports.

"Public" means no part of the dotted name starts with an underscore and the definition
is not nested in a function. Dunder methods are exempt: document the class instead.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterable, Iterator

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY
from snowflake_semantic_tools.domain.model import authored_schema
from tests.helpers.code_metrics import Definition, complexity, definitions, package_modules

PRIVATE_LINES = 25
PRIVATE_COMPLEXITY = 10
SUMMARY_LENGTH = 100
CODE = re.compile(r"SST-[A-Z]{3}\d{3}\b")
SECTION = re.compile(r"^[A-Z][A-Za-z ]*:$")
# The packages whose dataclasses document every field: the models, and the public API's results.
FIELD_PACKAGES = ("snowflake_semantic_tools/domain/model/", "snowflake_semantic_tools/api/")
_ENTRY = re.compile(r"^([A-Za-z_]\w*(?:\s*,\s*[A-Za-z_]\w*)*)\s*:")
_AUTHORED = re.compile(r"Authored keys:((?:\s*`[^`]+`,?)+)")

RULES = {
    "docstring-module": "every module has a docstring",
    "docstring-class": "every public class has a docstring",
    "docstring-function": "every public function and method has a docstring",
    "docstring-private": f"a private function of {PRIVATE_LINES}+ lines or complexity {PRIVATE_COMPLEXITY}+ has one",
    "docstring-summary": f"a docstring opens with one summary line of at most {SUMMARY_LENGTH} characters and a period",
    "docstring-codes": "a Diagnostics: section names only registered codes",
    "docstring-fields": "a model or API dataclass lists each field it declares under Attributes:",
    "docstring-authored": "an Authored keys: line names only tables authored_schema exports",
}


def documented_codes(docstring: str) -> list[str]:
    """The codes a `Diagnostics:` section lists, in order."""
    codes: list[str] = []
    in_section = False
    for line in docstring.splitlines():
        stripped = line.strip()
        if stripped == "Diagnostics:":
            in_section = True
        elif in_section and SECTION.match(stripped):
            in_section = False
        elif in_section and stripped:
            match = CODE.match(stripped)
            if match:
                codes.append(match.group(0))
    return codes


def documented_attributes(docstring: str) -> set[str]:
    """The names an `Attributes:` section lists, each entry's line read up to its colon."""
    names: set[str] = set()
    indent: int | None = None
    in_section = False
    for line in docstring.splitlines():
        stripped = line.strip()
        if stripped == "Attributes:":
            in_section, indent = True, None
            continue
        if not in_section or not stripped:
            continue
        depth = len(line) - len(line.lstrip())
        if indent is None:
            indent = depth
        if depth < indent or (depth == indent and SECTION.match(stripped)):
            in_section = False
        elif depth == indent and (entry := _ENTRY.match(stripped)):
            names.update(part.strip() for part in entry.group(1).split(","))
    return names


def cited_tables(docstring: str) -> list[str]:
    """The tables `Authored keys:` lines name, each as written: `authored_schema.AGENT`."""
    return [name for match in _AUTHORED.finditer(docstring) for name in re.findall(r"`([^`]+)`", match.group(1))]


def _known_table(name: str) -> bool:
    prefix, _, attribute = name.partition(".")
    return prefix == "authored_schema" and "." not in attribute and hasattr(authored_schema, attribute)


def _is_dataclass(node: ast.ClassDef) -> bool:
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if (isinstance(target, ast.Name) and target.id == "dataclass") or (
            isinstance(target, ast.Attribute) and target.attr == "dataclass"
        ):
            return True
    return False


def declared_fields(node: ast.ClassDef) -> list[str]:
    """The fields a dataclass declares itself, in order; a `ClassVar` is no field."""
    return [
        item.target.id
        for item in node.body
        if isinstance(item, ast.AnnAssign)
        and isinstance(item.target, ast.Name)
        and "ClassVar" not in ast.unparse(item.annotation)
    ]


def summary_problem(docstring: str) -> str | None:
    """Why a docstring's opening does not follow the summary-line rule, or None."""
    lines = docstring.splitlines()
    first = lines[0].strip() if lines else ""
    if not first:
        return "empty summary line"
    if len(first) > SUMMARY_LENGTH:
        return f"summary line is {len(first)} characters"
    if not first.endswith((".", "?", "!")):
        return "summary line does not end with a period"
    if len(lines) > 1 and lines[1].strip():
        return "second line is not blank"
    return None


def _is_public(definition: Definition) -> bool:
    return not definition.in_function and not any(part.startswith("_") for part in definition.qualname.split("."))


def _is_dunder(name: str) -> bool:
    return name.startswith("__") and name.endswith("__")


def _decorator_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    names: set[str] = set()
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(target, ast.Name):
            names.add(target.id)
        elif isinstance(target, ast.Attribute):
            names.add(target.attr)
    return names


def _base_names(node: ast.ClassDef) -> list[str]:
    names: list[str] = []
    for base in node.bases:
        target = base.value if isinstance(base, ast.Subscript) else base
        if isinstance(target, ast.Name):
            names.append(target.id)
        elif isinstance(target, ast.Attribute):
            names.append(target.attr)
    return names


class ClassIndex:
    """Package classes by simple name, to find documented base-class methods."""

    def __init__(self, classes: Iterable[ast.ClassDef]) -> None:
        self._by_name: dict[str, list[ast.ClassDef]] = {}
        for node in classes:
            self._by_name.setdefault(node.name, []).append(node)

    def inherits_documented(self, owner: ast.ClassDef, method: str) -> bool:
        """Whether a package base class of `owner` documents `method`."""
        seen: set[int] = set()
        pending = [base for name in _base_names(owner) for base in self._by_name.get(name, [])]
        while pending:
            base = pending.pop()
            if id(base) in seen:
                continue
            seen.add(id(base))
            for item in base.body:
                if (
                    isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and item.name == method
                    and ast.get_docstring(item)
                ):
                    return True
            pending.extend(parent for name in _base_names(base) for parent in self._by_name.get(name, []))
        return False


def module_violations(
    path: str, tree: ast.Module, index: ClassIndex, registered: Iterable[str] = ERROR_REGISTRY
) -> Iterator[str]:
    """Docstring violations in one module."""
    known = set(registered)
    module_doc = ast.get_docstring(tree)
    if not module_doc:
        yield f"docstring-module {path}"
    elif summary_problem(module_doc):
        yield f"docstring-summary {path}"
    owners: dict[str, ast.ClassDef] = {}
    for definition in definitions(path, tree):
        node = definition.node
        docstring = ast.get_docstring(node)
        if isinstance(node, ast.ClassDef):
            owners[definition.qualname] = node
            if docstring is None and _is_public(definition):
                yield f"docstring-class {definition.key}"
            if path.startswith(FIELD_PACKAGES) and _is_public(definition) and _is_dataclass(node):
                listed = documented_attributes(docstring or "")
                if any(name not in listed for name in declared_fields(node)):
                    yield f"docstring-fields {definition.key}"
        elif docstring is None:
            yield from _missing_function_doc(definition, node, owners, index)
        if docstring is not None:
            if summary_problem(docstring):
                yield f"docstring-summary {definition.key}"
            if any(code not in known for code in documented_codes(docstring)):
                yield f"docstring-codes {definition.key}"
            if not all(_known_table(name) for name in cited_tables(docstring)):
                yield f"docstring-authored {definition.key}"


def _missing_function_doc(
    definition: Definition,
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    owners: dict[str, ast.ClassDef],
    index: ClassIndex,
) -> Iterator[str]:
    if _is_dunder(node.name) or {"overload", "setter", "deleter"} & _decorator_names(node):
        return
    owner = owners.get(definition.qualname.rpartition(".")[0]) if definition.in_class else None
    if owner is not None and index.inherits_documented(owner, node.name):
        return
    if _is_public(definition):
        yield f"docstring-function {definition.key}"
    elif definition.lines >= PRIVATE_LINES or complexity(node) >= PRIVATE_COMPLEXITY:
        yield f"docstring-private {definition.key}"


def violations() -> dict[str, int | None]:
    """Every docstring violation in the package."""
    modules = list(package_modules())
    index = ClassIndex(node for _, tree, _ in modules for node in ast.walk(tree) if isinstance(node, ast.ClassDef))
    found: dict[str, int | None] = {}
    for path, tree, _ in modules:
        found.update((key, None) for key in module_violations(path, tree, index))
    return found
