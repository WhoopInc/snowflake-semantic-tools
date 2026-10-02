"""Guard: the engine's registry declares every code the 1.0 catalog declares, the way it declares it.

`tests/codes/catalog.json` is the source of truth for what a code means: its severity, whether a
setting may demote it, and its message template. Each disagreement the registry has with it today
is listed in `allowlist/catalog_divergence.json`, and that list only shrinks.
"""

from __future__ import annotations

import json

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY
from tests.helpers.code_guards import (
    CATALOG_FIELDS,
    CATALOG_PATH,
    PLANNING_ID,
    catalog_divergence,
    load_allowlist,
    load_catalog,
    ratchet,
)


def test_the_catalog_is_the_sorted_projection_of_its_declaration_rows() -> None:
    rows = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    assert [tuple(row) for row in rows] == [CATALOG_FIELDS] * len(rows)
    codes = [row["code"] for row in rows]
    assert codes == sorted(set(codes)), "catalog.json is not sorted by code, or repeats one"
    leaks = [row["code"] for row in rows if PLANNING_ID.search(json.dumps(row))]
    assert not leaks, f"catalog rows carry a planning identifier: {leaks}"


def test_the_registry_matches_the_catalog_except_where_allowlisted() -> None:
    found = catalog_divergence(load_catalog(), ERROR_REGISTRY)
    problems = ratchet(found, load_allowlist("catalog_divergence"), "catalog_divergence")
    assert not problems, "\n".join(problems)
