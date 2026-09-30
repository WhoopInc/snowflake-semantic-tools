"""Golden test: the renderer's bytes against DDL verified against Snowflake.

One pytest run fails when the renderer and the golden disagree. It is a byte
comparison, deliberately: the whole reason `domain/` may not read a clock or the
environment is so that this comparison is stable, and a looser assertion would
spend that property for nothing.

THE COMPARISON CONTRACT. A golden is a leading block of `--` provenance comments,
then a blank line, then DDL to end of file. The golden proper is everything from the
first line that is neither blank nor a comment. `tests/golden/README.md` records
this, and also records that these files are NOT what `GET_DDL` returns.
"""

from __future__ import annotations

import dataclasses
import difflib
from pathlib import Path

import pytest
from _pytest.outcomes import Failed

from tests.helpers.projects import load_views
from snowflake_semantic_tools.app.compile import CompiledView
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView
from snowflake_semantic_tools.domain.render.semantic_view import render

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "reference_project"
GOLDEN_DIR = REPO_ROOT / "tests" / "golden" / "expected" / "ddl"


def golden_ddl(name: str) -> str:
    """Read a golden and strip its provenance header."""
    lines = (GOLDEN_DIR / f"{name}.sql").read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        if line.strip() and not line.lstrip().startswith("--"):
            return "\n".join(lines[index:]).rstrip("\n")
    raise AssertionError(f"golden {name}.sql contains no DDL, only comments")


@pytest.fixture(scope="module")
def views() -> dict[str, SemanticView]:
    manifest = REPO_ROOT / "tests" / "fixtures" / "reference_project_manifest.json"
    loaded = load_views(FIXTURE, manifest_path=manifest)
    return {view.fqn.rsplit(".", 1)[-1]: view for view in loaded}


def test_fixture_and_goldens_are_present() -> None:
    """Guard the paths above, so a bad path cannot be mistaken for a passing suite."""
    assert (FIXTURE / "sst_config.yml").is_file(), f"fixture missing at {FIXTURE}"
    assert sorted(p.name for p in GOLDEN_DIR.glob("*.sql")) == [
        "jaffle_menu.sql",
        "jaffle_minimal.sql",
        "jaffle_sales.sql",
    ]


def test_all_fixture_views_load(views: dict[str, SemanticView]) -> None:
    assert sorted(views) == ["JAFFLE_MENU", "JAFFLE_MINIMAL", "JAFFLE_SALES"]


def test_target_is_resolved_from_the_profile(views: dict[str, SemanticView]) -> None:
    """`{{ target.database }}` is replaced before the model is built, not in domain."""
    assert views["JAFFLE_MINIMAL"].fqn == "SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL"
    assert views["JAFFLE_MINIMAL"].tables[0].fqn == "SST_REF_DEV.JAFFLE.PRODUCTS"


def test_folder_route_folds_over_the_base_semantic_view_target(views: dict[str, SemanticView]) -> None:
    assert views["JAFFLE_MENU"].fqn == "SST_REF_DEV.CORE.JAFFLE_MENU"


def assert_matches_golden(view: SemanticView, name: str) -> None:
    rendered = render(view)
    expected = golden_ddl(name)
    if rendered != expected:
        diff = "\n".join(
            difflib.unified_diff(
                expected.splitlines(),
                rendered.splitlines(),
                fromfile=f"golden/{name}.sql",
                tofile="rendered",
                lineterm="",
            )
        )
        pytest.fail(f"rendered DDL does not match the golden:\n{diff}")


def test_jaffle_minimal_matches_its_golden(views: dict[str, SemanticView]) -> None:
    """Rung 1: one table, required keys only. Proves the whole pipeline end to end."""
    assert_matches_golden(views["JAFFLE_MINIMAL"], "jaffle_minimal")


def test_jaffle_sales_matches_its_golden(views: dict[str, SemanticView]) -> None:
    """Rung 2: relationships, variables, filters, metrics, instructions and governance."""
    assert_matches_golden(views["JAFFLE_SALES"], "jaffle_sales")


def test_jaffle_menu_matches_its_golden(views: dict[str, SemanticView]) -> None:
    """Rung 3: aliasing, temporal joins, semi-additive metrics and SQL sidecars."""
    assert_matches_golden(views["JAFFLE_MENU"], "jaffle_menu")


def test_rendering_is_deterministic(views: dict[str, SemanticView]) -> None:
    """Same model, same bytes. The property that makes golden testing possible.

    This is not redundant with the golden comparison: that one proves the bytes are
    RIGHT, this proves they are STABLE. A renderer that reached for a clock or a set
    iteration order could pass the first and fail this.
    """
    view = views["JAFFLE_MINIMAL"]
    assert render(view) == render(view)
    manifest = REPO_ROOT / "tests" / "fixtures" / "reference_project_manifest.json"
    reloaded = {
        v.fqn.rsplit(".", 1)[-1]: v for v in load_views(FIXTURE, manifest_path=manifest)
    }
    assert render(reloaded["JAFFLE_MINIMAL"]) == render(view)


def test_golden_comparison_detects_a_changed_model(views: dict[str, SemanticView]) -> None:
    """Prove the comparison BITES, rather than trusting that it does.

    A golden test that has never rejected anything is indistinguishable from one
    that cannot. The ring contracts get the same treatment in
    `test_ring_boundaries.py`, and for the same reason: two vacuous passes were
    found while building that file.
    """
    drifted = dataclasses.replace(views["JAFFLE_MINIMAL"], comment="something else entirely")
    with pytest.raises(Failed, match="does not match the golden"):
        assert_matches_golden(drifted, "jaffle_minimal")


def test_golden_has_no_ungrammatical_clause_heads() -> None:
    """The clause heads no grammar has, asserted against the goldens rather than argued.

    `FILTERS (`, `VERIFIED QUERIES (` and `CUSTOM INSTRUCTIONS (` appear in no
    grammar. The goldens fold filters into DIMENSIONS as LABELS = (FILTER) and use
    the AI_* forms, so if one of these ever shows up in a golden, the golden is
    wrong and this test is the place that says so.
    """
    for path in sorted(GOLDEN_DIR.glob("*.sql")):
        ddl = golden_ddl(path.stem)
        for head in ("FILTERS (", "VERIFIED QUERIES (", "CUSTOM INSTRUCTIONS ("):
            assert head not in ddl, f"{path.name} contains ungrammatical clause head {head!r}"


def test_every_golden_fingerprint_is_its_compiled_canonical_ddl(views: dict[str, SemanticView]) -> None:
    for name in ("jaffle_menu", "jaffle_minimal", "jaffle_sales"):
        view = views[name.upper()]
        compiled = CompiledView(view, render(view))
        expected = golden_ddl(name)
        expected_compiled = CompiledView(view, expected)
        assert compiled.byte_length == expected_compiled.byte_length
        assert compiled.fingerprint == expected_compiled.fingerprint
