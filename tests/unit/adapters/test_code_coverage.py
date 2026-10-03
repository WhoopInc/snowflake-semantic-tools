"""Where the coverage reference reads each code's raise sites, test file, and pre-check from."""

from __future__ import annotations

import json
from pathlib import Path

from snowflake_semantic_tools.adapters.code_coverage import code_tests, prechecks, raise_sites


def test_raise_sites_names_each_module_once_and_skips_the_registry_and_conflict_copies(tmp_path: Path) -> None:
    package = tmp_path / "pkg"
    (package / "specs").mkdir(parents=True)
    (package / "a.py").write_text('X = ("SST-CFG001", "SST-CFG001", "SST-V002", "SST-CFG001 text")\n')
    (package / "b.py").write_text('Y = "SST-CFG001"\nZ = "SST-REF002"\n')
    (package / "a 2.py").write_text('X = "SST-PLN001"\n')
    (package / "broken.py").write_text("def (:\n")
    (package / "specs" / "registry.py").write_text('S = "SST-PLN002"\n')
    assert raise_sites(package, exclude=package / "specs") == {
        "SST-CFG001": ("pkg.a", "pkg.b"),
        "SST-REF002": ("pkg.b",),
    }


def test_code_tests_counts_a_file_only_when_it_holds_both_directions(tmp_path: Path) -> None:
    codes = tmp_path / "tests" / "codes"
    (codes / "cfg").mkdir(parents=True)
    (codes / "cfg" / "test_sst_cfg001.py").write_text(
        "def test_sst_cfg001_fires(): ...\ndef test_sst_cfg001_silent(): ...\n"
    )
    (codes / "cfg" / "test_sst_cfg002.py").write_text("def test_sst_cfg002_fires(): ...\n")
    (codes / "cfg" / "test_sst_cfg003.py").write_text("def (:\n")
    found = code_tests(codes, ("SST-CFG001", "SST-CFG002", "SST-CFG003", "SST-CFG004"))
    assert found == {"SST-CFG001": "tests/codes/cfg/test_sst_cfg001.py"}


def test_prechecks_reads_the_catalog_projection_and_tolerates_its_absence(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog.json"
    assert prechecks(catalog) == {}
    catalog.write_text(json.dumps({"rows": []}))
    assert prechecks(catalog) == {}
    catalog.write_text(json.dumps([{"code": "SST-CFG001", "precheck": "local"}, {"code": "SST-CFG002"}, "stray"]))
    assert prechecks(catalog) == {"SST-CFG001": "local", "SST-CFG002": ""}
