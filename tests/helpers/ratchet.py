"""Ratchet allowlists: a known violation may shrink or disappear, never grow or reappear.

Each allowlist under tests/unit/ratchets/ lists the violations the package carries today,
one `<rule> <location> [<measured value>]` per line. A gate fails on a violation that is
not listed, on a listed value that got worse, and on a listed entry that no longer
occurs -- so fixing one forces its line out, and the list only ever shrinks.

Regenerate after moving code (keys carry module paths), and check the diff shrinks:

    python -m tests.helpers.ratchet
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable

from tests.helpers.code_metrics import REPO_ROOT

RATCHETS = REPO_ROOT / "tests" / "unit" / "ratchets"
HEADER = "# Known violations; the list may only shrink. Regenerate: python -m tests.helpers.ratchet\n"
REGENERATE = "python -m tests.helpers.ratchet"


def read(name: str) -> dict[str, int | None]:
    """The allowlist `name`, as location key -> measured value (None where nothing is measured)."""
    path = RATCHETS / f"{name}.txt"
    if not path.exists():
        return {}
    entries: dict[str, int | None] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        rule, location, *value = line.split(" ")
        entries[f"{rule} {location}"] = int(value[0]) if value else None
    return entries


def write(name: str, violations: dict[str, int | None]) -> None:
    """Replace the allowlist `name` with the current violations, sorted."""
    RATCHETS.mkdir(parents=True, exist_ok=True)
    lines = [key if value is None else f"{key} {value}" for key, value in sorted(violations.items())]
    (RATCHETS / f"{name}.txt").write_text(HEADER + "".join(f"{line}\n" for line in lines), encoding="utf-8")


def problems(name: str, current: dict[str, int | None]) -> list[str]:
    """Every difference between the current violations and the allowlist that fails the gate."""
    allowed = read(name)
    found: list[str] = []
    for key, value in sorted(current.items()):
        if key not in allowed:
            found.append(f"new violation: {key}" + (f" ({value})" if value is not None else ""))
        elif value is not None and (limit := allowed[key]) is not None and value > limit:
            found.append(f"worse than allowed: {key} is {value}, allowlist says {limit}")
    for key in sorted(set(allowed) - set(current)):
        found.append(f"fixed or moved, remove from tests/unit/ratchets/{name}.txt: {key}")
    return found


def main() -> int:
    from tests.helpers import docstring_rules, structure_rules

    sources: dict[str, Callable[[], dict[str, int | None]]] = {
        "structure": structure_rules.violations,
        "docstrings": docstring_rules.violations,
    }
    for name, compute in sources.items():
        before = len(read(name))
        current = compute()
        write(name, current)
        print(f"{name}: {before} -> {len(current)} known violations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
