"""Dependencies: pyproject.toml, poetry.lock, DEPENDENCIES.md and the package's imports agree.

WHY THIS FILE EXISTS. The lock cannot always be regenerated where SST is developed, so it is
sometimes edited by hand, and DEPENDENCIES.md is prose nothing else reads. Each drifted once
unnoticed: Textual went undocumented, the dbt range kept an old floor, linkify-it-py stayed in the
dev group though Textual needs it at runtime, and certifi and cryptography were imported without
being declared, present only because the connector happens to require them. Each check below has
a companion test that proves it fails on exactly that kind of drift.

The license checks hold SST to Apache-2.0 distribution: every package a plain install reaches,
transitively and on every platform, is listed with its license, and that license is permissive
(certifi's MPL-2.0 is the one recorded exception). A re-lock that brings in a new package fails
until its license is recorded, and one whose metadata names a copyleft or proprietary license
fails outright.
"""

from __future__ import annotations

import ast
import copy
import re
import sys
import tomllib
from functools import cache
from importlib.metadata import PackageNotFoundError, distribution, packages_distributions
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE = REPO_ROOT / "snowflake_semantic_tools"
SECTIONS = {"runtime": "Runtime Dependencies", "optional": "Optional Dependencies", "dev": "Development Dependencies"}
TRANSITIVE = "Transitive Runtime Dependencies"
_NAME = re.compile(r"^[A-Za-z0-9._-]+")

# The licenses a runtime dependency may carry, as SPDX identifiers.
PERMISSIVE = frozenset(
    {"MIT", "MIT-0", "BSD-2-Clause", "BSD-3-Clause", "Apache-2.0", "PSF-2.0", "ISC", "0BSD", "Unlicense"}
)
# A weak copyleft accepted for one package each, used unmodified and never bundled.
EXCEPTIONS = {"certifi": frozenset({"MPL-2.0"})}
# Words in a distribution's license metadata that rule it out of an Apache-2.0 distribution.
_FORBIDDEN = re.compile(
    r"\b(A?GPL|LGPL|GNU|SSPL|BUSL|Business Source|Elastic|Commons Clause|Proprietary|Commercial|"
    r"Product Licensing Agreement)\b",
    re.IGNORECASE,
)


def _norm(name: str) -> str:
    """A distribution name as PEP 503 normalizes it, so `PyYAML` and `pyyaml` are one package."""
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


def _walk(poetry: dict[str, Any], lock: dict[str, Any], *, optional: bool) -> set[str]:
    """Every locked package pyproject's deps reach, on every platform; the optional ones only if asked."""
    packages = {_norm(package["name"]): package for package in lock["package"]}
    stack: list[tuple[str, frozenset[str]]] = [
        (_norm(name), frozenset(spec.get("extras", ())) if isinstance(spec, dict) else frozenset())
        for name, spec in poetry["dependencies"].items()
        if name != "python" and (optional or not (isinstance(spec, dict) and spec.get("optional")))
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
    return set(walked)


def group_mismatches(poetry: dict[str, Any], lock: dict[str, Any]) -> dict[str, str]:
    """Locked packages whose `main` group membership disagrees with the graph from pyproject's deps."""
    packages = {_norm(package["name"]): package for package in lock["package"]}
    walked = _walk(poetry, lock, optional=True)
    mismatches: dict[str, str] = {}
    for name, package in packages.items():
        in_main = "main" in package.get("groups", ())
        if name in walked and not in_main:
            mismatches[name] = "a runtime dependency locked outside the main group"
        elif in_main and name not in walked:
            mismatches[name] = "locked in the main group but nothing at runtime requires it"
    return mismatches


def spdx(cell: str) -> frozenset[str]:
    """The SPDX identifiers a DEPENDENCIES.md license cell names; `Apache 2.0` reads as `Apache-2.0`."""
    text = re.sub(r"\bApache 2\.0\b", "Apache-2.0", cell)
    return frozenset(part.strip() for part in re.split(r"\s+(?:or|OR|and|AND)\s+", text) if part.strip())


def license_table(text: str, heading: str, column: int) -> dict[str, str]:
    """One DEPENDENCIES.md table: the package cell's first word to the cell at `column`."""
    assert f"\n## {heading}\n" in text, heading
    body = text.split(f"\n## {heading}\n", 1)[1].split("\n## ", 1)[0]
    rows = [line.split("|")[1:-1] for line in body.splitlines() if line.startswith("|")][2:]
    return {_norm(cells[0].split()[0]): cells[column].strip() for cells in rows}


def runtime_licenses(text: str) -> dict[str, str]:
    """The license DEPENDENCIES.md records for every runtime package, direct and transitive."""
    return {**license_table(text, SECTIONS["runtime"], 2), **license_table(text, TRANSITIVE, 2)}


def unrecorded(poetry: dict[str, Any], lock: dict[str, Any], text: str) -> dict[str, set[str]]:
    """The transitive runtime packages the lock reaches and DEPENDENCIES.md does not list, and back."""
    direct = set(declared(poetry)["runtime"])
    reached = _walk(poetry, lock, optional=False) - direct
    listed = set(license_table(text, TRANSITIVE, 2))
    found = {"missing": reached - listed, "stale": listed - reached}
    return {kind: names for kind, names in found.items() if names}


def disallowed(licenses: dict[str, str]) -> dict[str, str]:
    """The recorded licenses that an Apache-2.0 distribution cannot carry, by package."""
    return {
        name: cell
        for name, cell in licenses.items()
        if not spdx(cell) or not spdx(cell) <= PERMISSIVE | EXCEPTIONS.get(name, frozenset())
    }


def metadata_text(name: str) -> str | None:
    """What an installed distribution says about its license; None when it is not installed here."""
    try:
        meta = distribution(name).metadata
    except PackageNotFoundError:
        return None
    classifiers = [c for c in meta.get_all("Classifier") or () if c.startswith("License ::")]
    licenses = [*(meta.get_all("License-Expression") or ()), *(meta.get_all("License") or ()), *classifiers]
    return "\n".join(licenses).strip()


def forbidden_metadata(names: set[str], read: Any = metadata_text) -> dict[str, str]:
    """The installed packages whose own license metadata names a copyleft or proprietary license."""
    found: dict[str, str] = {}
    for name in sorted(names):
        text = read(name)
        if text is None:
            continue
        match = _FORBIDDEN.search(text)
        if match and name not in EXCEPTIONS:
            found[name] = match.group()
    return found


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


def test_every_transitive_runtime_package_has_a_recorded_license() -> None:
    text = (REPO_ROOT / "DEPENDENCIES.md").read_text(encoding="utf-8")
    assert unrecorded(_toml("pyproject.toml")["tool"]["poetry"], _toml("poetry.lock"), text) == {}


def test_the_transitive_check_catches_a_new_package_and_a_stale_row() -> None:
    poetry = _toml("pyproject.toml")["tool"]["poetry"]
    text = (REPO_ROOT / "DEPENDENCIES.md").read_text(encoding="utf-8")
    row = next(line for line in text.splitlines() if line.startswith("| rich |"))
    assert unrecorded(poetry, _toml("poetry.lock"), text.replace(row + "\n", "")) == {"missing": {"rich"}}
    assert unrecorded(poetry, _toml("poetry.lock"), text.replace(row, row + "\n| leftpad | 1.0 | MIT |")) == {
        "stale": {"leftpad"}
    }


def test_every_runtime_license_is_permissive_or_a_recorded_exception() -> None:
    licenses = runtime_licenses((REPO_ROOT / "DEPENDENCIES.md").read_text(encoding="utf-8"))
    assert licenses, "the tables were read"
    assert disallowed(licenses) == {}


def test_the_license_check_catches_copyleft_and_an_exception_on_the_wrong_package() -> None:
    assert disallowed({"rich": "GPL-3.0-only", "pyyaml": "MIT"}) == {"rich": "GPL-3.0-only"}
    assert disallowed({"rich": "MPL-2.0", "certifi": "MPL-2.0"}) == {"rich": "MPL-2.0"}
    assert disallowed({"textual": "MIT or LGPL-2.1"}) == {"textual": "MIT or LGPL-2.1"}
    assert disallowed({"dbt": ""}) == {"dbt": ""}


def test_no_installed_runtime_package_declares_a_copyleft_or_proprietary_license() -> None:
    poetry = _toml("pyproject.toml")["tool"]["poetry"]
    assert forbidden_metadata(_walk(poetry, _toml("poetry.lock"), optional=False)) == {}


def test_the_metadata_check_catches_gpl_and_proprietary_dbt() -> None:
    fake = {
        "rich": "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "dbt": "dbt Product Licensing Agreement",
        "pyyaml": "MIT",
        "certifi": "MPL-2.0",
    }
    assert forbidden_metadata(set(fake) | {"absent"}, fake.get) == {"rich": "GNU", "dbt": "Product Licensing Agreement"}


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
