"""The diagnostic-code guards' shared parts: the 1.0 catalog, the allowlists, and the source scans.

`tests/codes/` compares the engine's `ERROR_REGISTRY` with `tests/codes/catalog.json`, checks that
every registered code is emitted somewhere and tested both ways. Each guard tolerates today's gaps
only through an allowlist in `tests/codes/allowlist/`, and every allowlist is shrink-only: `ratchet`
fails on a gap that is not listed, on a listed reason that no longer describes the gap, and on a
listed code that has no gap left, so an entry is removed the moment its code conforms.

Run as a script, this module regenerates `tests/codes/catalog.json` from the catalog's extracted
declaration rows; see `tests/codes/README.md`.
"""

from __future__ import annotations

import ast
import json
import re
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import ErrorSpec

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE = REPO_ROOT / "snowflake_semantic_tools"
TESTS = REPO_ROOT / "tests"
CODES_DIR = TESTS / "codes"
CATALOG_PATH = CODES_DIR / "catalog.json"
ALLOWLIST_DIR = CODES_DIR / "allowlist"
# The registry declares every code; a reference there proves nothing about emission.
REGISTRY_SPECS = PACKAGE / "domain" / "diagnostics" / "specs"

CODE = re.compile(r"^SST-[A-Z]{3}\d{3}$")
# `test_sst_cfg010_fires` or `test_sst_cfg010_silent`, optionally followed by `_<what it shows>`.
CODE_TEST = re.compile(r"^test_sst_([a-z]{3})(\d{3})_(fires|silent)(?:_\w+)?$")
# The catalog columns committed here. The others carry design rationale that cites documents and
# decision identifiers which do not ship with this repository.
CATALOG_FIELDS = ("code", "area", "number", "severity", "non_demotable", "title", "message", "suggestion", "precheck")
# A planning identifier, as tests/unit/test_public_docs.py defines it; regeneration refuses one.
PLANNING_ID = re.compile(r"(?<![\w$-])[A-Z]\d{3}(?![\w-])")
# What the catalog's Suggestion column carries besides the suggestion itself, removed in order:
# a parenthesised planning identifier, a bold aside and all after it, a pointer into the design
# documents, and a placeholder backticked so the catalog's markdown keeps its angle brackets.
_SUGGESTION_ASIDES = (
    (re.compile(r"\s*\(`?[A-Z]\d{3}`?\)"), ""),
    (re.compile(r"\s*\*\*.*$", re.DOTALL), ""),
    (re.compile(r"\s+--\s+see\s+specs/.*$", re.DOTALL), ""),
    (re.compile(r"(?<![\w/-])specs/(\w+)/"), r"the \1 directory"),
    (re.compile(r"`(<[^<>`]+>)`"), r"\1"),
)


def public_suggestion(text: object) -> str | None:
    """Return a catalog Suggestion cell as the engine registers it; None for `--`, which offers none.

    The cell's rationale -- planning identifiers, bold asides, design-document pointers -- is
    removed mechanically, by `_SUGGESTION_ASIDES`, so the suggestion ships without it.
    """
    if not isinstance(text, str) or text.strip() in ("", "--"):
        return None
    for pattern, replacement in _SUGGESTION_ASIDES:
        text = pattern.sub(replacement, text)
    return text.strip()


@dataclass(frozen=True, slots=True)
class CatalogRow:
    """One code as the 1.0 catalog declares it.

    Attributes:
        severity: ERROR, WARNING, or INFO for a live code; RETIRED for a number that is burned.
        message: The message template, in `str.format` syntax; "--" for a retired code.
        suggestion: The Suggestion column as `public_suggestion` reads it; None when it offers none.
        precheck: Where the condition is first observable -- local, observe, or runtime -- as
            extracted; a malformed catalog row can leave something else here.
    """

    code: str
    area: str
    number: int
    severity: str
    non_demotable: bool
    title: str
    message: str
    suggestion: str | None
    precheck: str

    @property
    def live(self) -> bool:
        """Report whether the code is in use, rather than retired."""
        return self.severity != "RETIRED"


def load_catalog(path: Path = CATALOG_PATH) -> dict[str, CatalogRow]:
    """Read the committed catalog, keyed by code."""
    rows = json.loads(path.read_text(encoding="utf-8"))
    return {row["code"]: CatalogRow(**row) for row in rows}


def project_catalog(rows: Iterable[Mapping[str, object]]) -> list[dict[str, object]]:
    """Return the committed projection of extracted catalog rows: `CATALOG_FIELDS`, sorted by code.

    The suggestion is projected through `public_suggestion`.

    Raises:
        ValueError: a code repeats, or a kept field carries a planning identifier.
    """
    projected = sorted(
        (
            {field: public_suggestion(row[field]) if field == "suggestion" else row[field] for field in CATALOG_FIELDS}
            for row in rows
        ),
        key=lambda row: str(row["code"]),
    )
    codes = [row["code"] for row in projected]
    if len(codes) != len(set(codes)):
        raise ValueError("the extracted catalog repeats a code")
    leaks = [
        f"{row['code']}.{key}" for row in projected for key, value in row.items() if PLANNING_ID.search(str(value))
    ]
    if leaks:
        raise ValueError(f"kept catalog fields carry a planning identifier: {leaks}")
    return projected


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


def catalog_divergence(catalog: Mapping[str, CatalogRow], registry: Mapping[str, ErrorSpec]) -> dict[str, str]:
    """Return each code on which the catalog and the registry disagree, with every disagreement.

    A live catalog code must be registered with the catalog's severity, non-demotable flag,
    title, message template, and suggestion; a retired one must not be registered; and a
    registered code must be in the catalog at all.
    """
    divergence: dict[str, str] = {}
    for code in sorted(set(catalog) | set(registry)):
        row, spec = catalog.get(code), registry.get(code)
        if row is None:
            divergence[code] = "registered, not in the catalog"
        elif spec is None:
            if row.live:
                divergence[code] = "declared by the catalog, not registered"
        elif not row.live:
            divergence[code] = "retired by the catalog, still registered"
        else:
            reasons = []
            if row.severity != spec.severity.name:
                reasons.append(f"severity: catalog {row.severity}, engine {spec.severity.name}")
            if row.non_demotable == spec.demotable:
                reasons.append(f"non-demotable: catalog {row.non_demotable}, engine {not spec.demotable}")
            if row.message != spec.template:
                reasons.append(f"template: catalog {row.message!r}, engine {spec.template!r}")
            if row.title != spec.title:
                reasons.append(f"title: catalog {row.title!r}, engine {spec.title!r}")
            if row.suggestion != spec.suggestion:
                reasons.append(f"suggestion: catalog {row.suggestion!r}, engine {spec.suggestion!r}")
            if reasons:
                divergence[code] = "; ".join(reasons)
    return divergence


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


def main(argv: list[str]) -> int:
    """Rewrite `tests/codes/catalog.json` from the extracted catalog rows in the JSON file `argv[1]`."""
    if len(argv) != 2:
        print("usage: python -m tests.helpers.code_guards <extracted-catalog.json>", file=sys.stderr)
        return 2
    rows = json.loads(Path(argv[1]).read_text(encoding="utf-8"))
    CATALOG_PATH.write_text(json.dumps(project_catalog(rows), indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
