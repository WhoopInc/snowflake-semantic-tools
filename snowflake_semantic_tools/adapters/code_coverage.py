"""What the coverage reference is generated from: where each code is raised, and its tests.

`raise_sites` reads the package for every string constant that is exactly a code; the error
registry's own modules are left out, since registering a code is not raising it. `code_tests`
reads the test tree for each code's conventional test file, `tests/codes/<area>/test_<code>.py`,
counting it only when it defines both the `fires` and the `silent` test. `prechecks` reads the
catalog projection the code tests check the registry against. Files a sync tool left behind as
conflict copies, whose names hold a space, are never read.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

_CODE = re.compile(r"SST-[A-Z]{3}\d{3}\Z")


def raise_sites(package_dir: Path, *, exclude: Path) -> dict[str, tuple[str, ...]]:
    """Return, by code, the dotted modules under `package_dir` that name it, outside `exclude`.

    A module that cannot be read or parsed is skipped.
    """
    found: dict[str, list[str]] = {}
    for path in sorted(package_dir.rglob("*.py")):
        if path.is_relative_to(exclude) or " " in path.name or "__pycache__" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        module = path.relative_to(package_dir.parent).with_suffix("").as_posix().replace("/", ".")
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and _CODE.match(node.value):
                sites = found.setdefault(node.value, [])
                if module not in sites:
                    sites.append(module)
    return {code: tuple(sites) for code, sites in found.items()}


def code_tests(codes_dir: Path, codes: tuple[str, ...]) -> dict[str, str]:
    """Return, by code, its test file's path relative to `codes_dir`'s grandparent, when complete.

    A code is listed only when `codes_dir/<area>/test_<code>.py` defines both
    `test_<code>_fires` and `test_<code>_silent`; a file that cannot be parsed counts as absent.
    """
    found: dict[str, str] = {}
    for code in codes:
        stem = "test_" + code.casefold().replace("-", "_")
        path = codes_dir / code[4:7].casefold() / f"{stem}.py"
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        names = {node.name for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
        if {f"{stem}_fires", f"{stem}_silent"} <= names:
            found[code] = path.relative_to(codes_dir.parent.parent).as_posix()
    return found


def prechecks(catalog: Path) -> dict[str, str]:
    """Return, by code, where the catalog says its condition can first be detected; empty without one.

    `local` needs no connection, `observe` needs a read of Snowflake, and `runtime` is seen only
    while a statement runs. A file that is absent or not a JSON list of rows reads as empty.
    """
    try:
        rows = json.loads(catalog.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(rows, list):
        return {}
    return {
        str(row["code"]): str(row.get("precheck") or "")
        for row in rows
        if isinstance(row, dict) and isinstance(row.get("code"), str)
    }
