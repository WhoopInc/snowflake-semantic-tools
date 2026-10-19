"""Dependencies: pyproject.toml, poetry.lock, DEPENDENCIES.md and the package's imports agree.

WHY THIS FILE EXISTS. The lock cannot always be regenerated where SST is developed, so it is
sometimes edited by hand, and DEPENDENCIES.md is prose nothing else reads. Each drifted once
unnoticed: Textual went undocumented, the dbt range kept an old floor, linkify-it-py stayed in the
dev group though Textual needs it at runtime, and certifi and cryptography were imported without
being declared, present only because the connector happens to require them. Each check below has
a companion test that proves it fails on exactly that kind of drift.
"""

from __future__ import annotations

import ast
import copy
import re
import sys
import tomllib
from functools import cache
from importlib.metadata import distribution, packages_distributions
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE = REPO_ROOT / "snowflake_semantic_tools"
SECTIONS = {"runtime": "Runtime Dependencies", "optional": "Optional Dependencies", "dev": "Development Dependencies"}
_NAME = re.compile(r"^[A-Za-z0-9._-]+")


def _norm(name: str) -> str:
    """A distribution name as PEP 503 normalises it, so `PyYAML` and `pyyaml` are one package."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _toml(name: str) -> dict[str, Any]:
    return tomllib.loads((REPO_ROOT / name).read_text(encoding="utf-8"))


def declared(poetry: dict[str, Any]) -> dict[str, dict[str, str]]:
    """Each section's dependencies from pyproject's `[tool.poetry]`, by name, with the declared range."""
    main = {name: spec for name, spec in poetry["dependencies"].items() if name != "python"}

    def version(spec: str | dict[str, Any]) -> str:
        return spec if isinstance(spec, str) else str(spec["version"])

    def optional(spec: str | dict[str, Any]) -> bool:
        return isinstance(spec, dict) and bool(spec.get("optional"))

    return {
        "runtime": {_norm(name): version(spec) for name, spec in main.items() if not optional(spec)},
        "optional": {_norm(name): version(spec) for name, spec in main.items() if optional(spec)},
        "dev": {_norm(name): version(spec) for name, spec in poetry["group"]["dev"]["dependencies"].items()},
    }


def documented(text: str) -> dict[str, dict[str, str]]:
    """Each section's table in DEPENDENCIES.md: the package cell's first word to the version cell."""
    tables: dict[str, dict[str, str]] = {}
    for key, heading in SECTIONS.items():
        assert f"\n## {heading}\n" in text, heading
        body = text.split(f"\n## {heading}\n", 1)[1].split("\n## ", 1)[0]
        rows = [line.split("|")[1:-1] for line in body.splitlines() if line.startswith("|")][2:]
        tables[key] = {_norm(cells[0].split()[0]): cells[1].strip() for cells in rows}
    return tables


def third_party_imports(package: Path) -> dict[str, set[str]]:
    """Every module the package imports from outside itself and the standard library, with its files."""
    found: dict[str, set[str]] = {}
    for path in sorted(package.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules = [node.module]
            else:
                continue
            for module in modules:
                top = module.partition(".")[0]
                if top not in sys.stdlib_module_names and top != package.name:
                    found.setdefault(module, set()).add(path.relative_to(package.parent).as_posix())
    return found


@cache
def _files(dist: str) -> frozenset[str]:
    return frozenset(str(file) for file in distribution(dist).files or ())


def owners(module: str) -> set[str]:
    """The installed distributions that ship `module`: within a namespace, only the one that holds it."""
    candidates = packages_distributions().get(module.partition(".")[0], [])
    parts = module.split(".")
    for depth in range(len(parts), 0, -1):
        stem = "/".join(parts[:depth])
        holding = {_norm(d) for d in candidates if {f"{stem}/__init__.py", f"{stem}.py"} & _files(d)}
        if holding:
            return holding
    return {_norm(d) for d in candidates}


def undeclared(imports: dict[str, set[str]], runtime: set[str]) -> dict[str, set[str]]:
    """The imported modules no declared runtime dependency ships, with the distributions that do."""
    return {module: owned for module in imports if not (owned := owners(module)) & runtime}


def group_mismatches(poetry: dict[str, Any], lock: dict[str, Any]) -> dict[str, str]:
    """Locked packages whose `main` group membership disagrees with the graph from pyproject's deps."""
    packages = {_norm(package["name"]): package for package in lock["package"]}
    stack: list[tuple[str, frozenset[str]]] = [
        (_norm(name), frozenset(spec.get("extras", ())) if isinstance(spec, dict) else frozenset())
        for name, spec in poetry["dependencies"].items()
        if name != "python"
    ]
    walked: dict[str, set[str]] = {}
    while stack:
        name, extras = stack.pop()
        if name in walked and extras <= walked[name]:
            continue
        walked.setdefault(name, set()).update(extras)
        package = packages[name]
        wanted = {
            _norm(match.group())
            for extra in extras
            for requirement in package.get("extras", {}).get(extra, ())
            if (match := _NAME.match(requirement))
        }
        for dependency, specs in package.get("dependencies", {}).items():
            for spec in specs if isinstance(specs, list) else [specs]:
                if isinstance(spec, dict) and spec.get("optional") and _norm(dependency) not in wanted:
                    continue
                sub = frozenset(spec.get("extras", ())) if isinstance(spec, dict) else frozenset()
                stack.append((_norm(dependency), sub))
    mismatches: dict[str, str] = {}
    for name, package in packages.items():
        in_main = "main" in package.get("groups", ())
        if name in walked and not in_main:
            mismatches[name] = "a runtime dependency locked outside the main group"
        elif in_main and name not in walked:
            mismatches[name] = "locked in the main group but nothing at runtime requires it"
    return mismatches


def test_the_license_tables_list_exactly_what_pyproject_declares() -> None:
    poetry = _toml("pyproject.toml")["tool"]["poetry"]
    assert documented((REPO_ROOT / "DEPENDENCIES.md").read_text(encoding="utf-8")) == declared(poetry)


def test_the_table_check_catches_a_missing_row_and_a_stale_range() -> None:
    text = (REPO_ROOT / "DEPENDENCIES.md").read_text(encoding="utf-8")
    poetry = _toml("pyproject.toml")["tool"]["poetry"]
    row = next(line for line in text.splitlines() if line.startswith("| textual |"))
    assert documented(text.replace(row + "\n", "")) != declared(poetry)
    assert documented(text.replace(">=1.10,<2.0", ">=1.8,<2.0")) != declared(poetry)


def test_every_third_party_module_the_package_imports_is_a_declared_runtime_dependency() -> None:
    runtime = set(declared(_toml("pyproject.toml")["tool"]["poetry"])["runtime"])
    imports = third_party_imports(PACKAGE)
    assert imports, "the scan found the package's imports"
    assert undeclared(imports, runtime) == {}


def test_the_import_check_catches_a_dependency_present_only_transitively() -> None:
    runtime = set(declared(_toml("pyproject.toml")["tool"]["poetry"])["runtime"])
    imports = third_party_imports(PACKAGE)
    assert undeclared(imports, runtime - {"certifi"}) == {"certifi": {"certifi"}}
    # A namespace is narrowed to the distribution that holds the module, not whichever shares it.
    assert owners("snowflake.connector") == {"snowflake-connector-python"}


def test_the_lock_main_group_is_exactly_what_the_runtime_graph_reaches() -> None:
    assert group_mismatches(_toml("pyproject.toml")["tool"]["poetry"], _toml("poetry.lock")) == {}


def test_the_lock_group_check_catches_a_runtime_package_left_in_dev_and_a_stale_main() -> None:
    poetry = _toml("pyproject.toml")["tool"]["poetry"]
    lock = _toml("poetry.lock")
    edited = copy.deepcopy(lock)
    for package in edited["package"]:
        if package["name"] == "linkify-it-py":
            package["groups"] = ["dev"]
        if package["name"] == "pytest":
            package["groups"] = ["main", "dev"]
    assert group_mismatches(poetry, edited) == {
        "linkify-it-py": "a runtime dependency locked outside the main group",
        "pytest": "locked in the main group but nothing at runtime requires it",
    }
