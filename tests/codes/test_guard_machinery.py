"""The guards' own machinery bites: the ratchet, the source scans, and the catalog projection.

A guard that has never failed is indistinguishable from one that cannot, so each part is shown
failing on the shape it exists for, on synthetic input.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from snowflake_semantic_tools.domain.diagnostics.core import spec
from tests.helpers.code_guards import (
    CatalogRow,
    catalog_divergence,
    code_references,
    code_tests,
    project_catalog,
    public_suggestion,
    ratchet,
    untested,
)


def test_the_ratchet_rejects_new_gaps_changed_reasons_and_stale_entries() -> None:
    assert ratchet({"SST-AAA001": "gap"}, {"SST-AAA001": "gap"}, "x") == []
    assert ratchet({"SST-AAA001": "gap"}, {}, "x") == [
        "SST-AAA001: gap -- fix it; tests/codes/allowlist/x.json only shrinks"
    ]
    assert ratchet({"SST-AAA001": "wider"}, {"SST-AAA001": "gap"}, "x") == [
        "SST-AAA001: tests/codes/allowlist/x.json lists 'gap' but it is now 'wider' -- update the entry"
    ]
    assert ratchet({}, {"SST-AAA001": "gap"}, "x") == [
        "SST-AAA001: remove it from tests/codes/allowlist/x.json, it now conforms"
    ]


def test_divergence_reports_each_way_a_code_can_disagree() -> None:
    def row(
        code: str,
        severity: str = "ERROR",
        *,
        non_demotable: bool = False,
        message: str = "m",
        title: str = "t",
        suggestion: str | None = None,
    ) -> CatalogRow:
        return CatalogRow(code, code[4:7], int(code[7:]), severity, non_demotable, title, message, suggestion, "local")

    catalog = {
        "SST-AAA001": row("SST-AAA001"),
        "SST-AAA002": row("SST-AAA002"),
        "SST-AAA003": row("SST-AAA003", "RETIRED", message="--"),
        "SST-AAA004": row("SST-AAA004", "RETIRED", message="--"),
        "SST-AAA005": row("SST-AAA005", "WARNING", non_demotable=True, message="other"),
        "SST-AAA007": row("SST-AAA007", title="other", suggestion="fix it"),
    }
    registry = {
        "SST-AAA001": spec("SST-AAA001", Severity.ERROR, "t", "m", None),
        "SST-AAA004": spec("SST-AAA004", Severity.ERROR, "t", "m", None),
        "SST-AAA005": spec("SST-AAA005", Severity.ERROR, "t", "m", None),
        "SST-AAA006": spec("SST-AAA006", Severity.ERROR, "t", "m", None),
        "SST-AAA007": spec("SST-AAA007", Severity.ERROR, "t", "m", "repair it"),
    }
    assert catalog_divergence(catalog, registry) == {
        "SST-AAA002": "declared by the catalog, not registered",
        "SST-AAA004": "retired by the catalog, still registered",
        "SST-AAA005": "severity: catalog WARNING, engine ERROR; non-demotable: catalog True, engine False; "
        "template: catalog 'other', engine 'm'",
        "SST-AAA006": "registered, not in the catalog",
        "SST-AAA007": "title: catalog 'other', engine 't'; suggestion: catalog 'fix it', engine 'repair it'",
    }


def test_the_reference_scan_counts_exact_constants_and_skips_prose_and_the_registry(tmp_path: Path) -> None:
    (tmp_path / "specs").mkdir()
    (tmp_path / "specs" / "codes.py").write_text('spec("SST-AAA009")\n', encoding="utf-8")
    (tmp_path / "emit.py").write_text(
        '"""Reports SST-AAA002."""\n'
        'D("SST-AAA001")\n'
        'code = "SST-AAA003" if x else "SST-AAA004"\n'
        'note = "see SST-AAA005"\n',
        encoding="utf-8",
    )
    found = code_references(tmp_path, exclude=tmp_path / "specs")
    assert sorted(found) == ["SST-AAA001", "SST-AAA003", "SST-AAA004"]


def test_the_test_scan_reads_fires_and_silent_names_only(tmp_path: Path) -> None:
    (tmp_path / "test_m.py").write_text(
        "def test_sst_aaa001_fires(): ...\n"
        "def test_sst_aaa001_silent_on_a_valid_key(): ...\n"
        "def test_sst_aaa002_fires(): ...\n"
        "def test_sst_aaa003_firesx(): ...\n"
        "def helper_sst_aaa004_silent(): ...\n",
        encoding="utf-8",
    )
    found = code_tests(tmp_path)
    assert found == {"SST-AAA001": {"fires", "silent"}, "SST-AAA002": {"fires"}}
    assert untested(("SST-AAA001", "SST-AAA002", "SST-AAA003"), found) == {
        "SST-AAA002": "no test_sst_aaa002_silent",
        "SST-AAA003": "no test_sst_aaa003_fires or test_sst_aaa003_silent",
    }


def test_the_catalog_projection_drops_rationale_and_refuses_planning_identifiers() -> None:
    extracted = {
        "code": "SST-AAA001",
        "area": "AAA",
        "number": 1,
        "severity": "ERROR",
        "non_demotable": False,
        "title": "t",
        "message": "m",
        "suggestion": "s",
        "trigger": "t",
        "precheck": "local",
        "origin": "o",
        "line": 1,
    }
    assert project_catalog([extracted]) == [
        {
            key: extracted[key]
            for key in (
                "code",
                "area",
                "number",
                "severity",
                "non_demotable",
                "title",
                "message",
                "suggestion",
                "precheck",
            )
        }
    ]
    with pytest.raises(ValueError, match="planning identifier"):
        project_catalog([{**extracted, "title": "settled by D248"}])
    with pytest.raises(ValueError, match="repeats"):
        project_catalog([extracted, extracted])


@pytest.mark.parametrize(
    ("cell", "suggestion"),
    [
        ("--", None),
        ("", None),
        (
            "break the dependency -- the ORDER is not authorable (`D999`)",
            "break the dependency -- the ORDER is not authorable",
        ),
        ("rename it. **This used to read otherwise, which `D998` made wrong**: so it goes", "rename it."),
        ("use `{{ fn('arg') }}` -- see specs/README section 4", "use `{{ fn('arg') }}`"),
        ("declare the member in specs/tools/", "declare the member in the tools directory"),
        ("pass `<database>`.`<schema>`", "pass <database>.<schema>"),
        ("use `--manifest`", "use `--manifest`"),
    ],
)
def test_a_suggestion_ships_without_the_catalogs_rationale(cell: str, suggestion: str | None) -> None:
    assert public_suggestion(cell) == suggestion


def test_the_live_registry_is_what_the_guards_read() -> None:
    assert "SST-CFG003" in ERROR_REGISTRY and "SST-CFG003" in code_references()
