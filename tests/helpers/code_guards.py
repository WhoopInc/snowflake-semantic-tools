"""The diagnostic-code guards' shared parts: the allowlists and the source scans.

`tests/codes/` checks that every code in the engine's `ERROR_REGISTRY` is emitted somewhere and
tested both ways. Each guard tolerates today's gaps only through an allowlist in
`tests/codes/allowlist/`, and every allowlist is shrink-only: `ratchet` fails on a gap that is not
listed, on a listed reason that no longer describes the gap, and on a listed code that has no gap
left, so an entry is removed the moment its code conforms.
"""

from __future__ import annotations

import ast
import json
import re
from collections.abc import Iterable, Mapping
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE = REPO_ROOT / "snowflake_semantic_tools"
TESTS = REPO_ROOT / "tests"
CODES_DIR = TESTS / "codes"
ALLOWLIST_DIR = CODES_DIR / "allowlist"
# The registry declares every code; a reference there proves nothing about emission.
REGISTRY_SPECS = PACKAGE / "domain" / "diagnostics" / "specs"

CODE = re.compile(r"^SST-[A-Z]{3}\d{3}$")
# `test_sst_cfg010_fires` or `test_sst_cfg010_silent`, optionally followed by `_<what it shows>`.
CODE_TEST = re.compile(r"^test_sst_([a-z]{3})(\d{3})_(fires|silent)(?:_\w+)?$")


def load_allowlist(name: str) -> dict[str, str]:
    """Read `allowlist/<name>.json`, a mapping of code to the reason it is tolerated."""
    value: dict[str, str] = json.loads((ALLOWLIST_DIR / f"{name}.json").read_text(encoding="utf-8"))
    return value


def ratchet(found: Mapping[str, str], allowed: Mapping[str, str], name: str) -> list[str]:
    """Compare the gaps a guard found with its allowlist; return one line per disagreement.

    Args:
        found: Each code the guard flags, with the reason it is flagged.
        allowed: The allowlist, read with `load_allowlist(name)`.

    Returns:
        Empty when the two agree exactly. Otherwise a line for each unlisted gap (fix it: the
        allowlist only shrinks), each listed code whose reason changed, and each listed code that
        now conforms (remove it).
    """
    where = f"tests/codes/allowlist/{name}.json"
    problems = [
        f"{code}: {reason} -- fix it; {where} only shrinks"
        for code, reason in sorted(found.items())
        if code not in allowed
    ]
    problems += [
        f"{code}: {where} lists {allowed[code]!r} but it is now {reason!r} -- update the entry"
        for code, reason in sorted(found.items())
        if code in allowed and allowed[code] != reason
    ]
    problems += [f"{code}: remove it from {where}, it now conforms" for code in sorted(set(allowed) - set(found))]
    return problems


def unemitted(registry: Iterable[str], references: Mapping[str, list[str]]) -> dict[str, str]:
    """Return each registered code that no package module outside the registry names."""
    return dict.fromkeys(sorted(set(registry) - set(references)), "registered, never referenced outside the registry")


def untested(registry: Iterable[str], tests: Mapping[str, set[str]]) -> dict[str, str]:
    """Return each registered code that lacks a `fires` test, a `silent` test, or both."""
    gaps: dict[str, str] = {}
    for code in sorted(registry):
        stem = "test_" + code.casefold().replace("-", "_")
        missing = [f"{stem}_{kind}" for kind in ("fires", "silent") if kind not in tests.get(code, set())]
        if missing:
            gaps[code] = "no " + " or ".join(missing)
    return gaps


def code_references(root: Path = PACKAGE, *, exclude: Path = REGISTRY_SPECS) -> dict[str, list[str]]:
    """Return every string constant under `root` that is exactly a code, with where each appears.

    A reference is any constant, so a code passed through a variable, a table, or a conditional
    counts at the literal that names it; a code mentioned inside longer text (a docstring) does not.

    Returns:
        By code, its `path:line` references in file order, paths relative to `root`'s parent.
    """
    references: dict[str, list[str]] = {}
    for path in sorted(root.rglob("*.py")):
        if path.is_relative_to(exclude) or "__pycache__" in path.parts:
            continue
        relative = path.relative_to(root.parent).as_posix()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and CODE.match(node.value):
                references.setdefault(node.value, []).append(f"{relative}:{node.lineno}")
    return references


def code_tests(root: Path = TESTS) -> dict[str, set[str]]:
    """Return, by code, which of `fires` and `silent` some test function under `root` is named for."""
    found: dict[str, set[str]] = {}
    for path in sorted(root.rglob("test_*.py")):
        if any(part in {"fixtures", "golden", "__pycache__"} for part in path.relative_to(root).parts):
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and (match := CODE_TEST.match(node.name)):
                area, number, kind = match.groups()
                found.setdefault(f"SST-{area.upper()}{number}", set()).add(kind)
    return found
