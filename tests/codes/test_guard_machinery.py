"""The guards' own machinery bites: the ratchet and the source scans.

A guard that has never failed is indistinguishable from one that cannot, so each part is shown
failing on the shape it exists for, on synthetic input.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY
from tests.helpers.code_guards import code_references, code_tests, ratchet, untested


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


def test_the_live_registry_is_what_the_guards_read() -> None:
    assert "SST-CFG003" in ERROR_REGISTRY and "SST-CFG003" in code_references()
