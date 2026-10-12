"""SST-LOD011: a multi-line value is a folded scalar, which joins its lines."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.diagnostics.baseline import stable_fingerprint
from tests.helpers.seam_projects import parsed


def test_sst_lod011_fires() -> None:
    [diagnostic] = [item for item in parsed(b"a: >\n  one\n  two\n").diagnostics if item.code == "SST-LOD011"]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "f.yml:1: 'a' uses a folded scalar"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml", 1)


def test_sst_lod011_silent() -> None:
    assert "SST-LOD011" not in [item.code for item in parsed(b"a: |-\n  one\n  two\n").diagnostics]


def test_sst_lod011_keeps_its_baseline_fingerprint_when_the_value_moves_down() -> None:
    [here] = [item for item in parsed(b"a: >\n  one\n").diagnostics if item.code == "SST-LOD011"]
    [moved] = [item for item in parsed(b"# x\n# y\n# z\na: >\n  one\n").diagnostics if item.code == "SST-LOD011"]
    assert moved.message == "f.yml:4: 'a' uses a folded scalar"
    assert stable_fingerprint(moved) == stable_fingerprint(here)


def test_sst_lod011_findings_for_one_key_in_a_file_keep_distinct_fingerprints() -> None:
    text = b"columns:\n  - description: >\n      one\n  - description: >\n      two\n"
    found = [item for item in parsed(text).diagnostics if item.code == "SST-LOD011"]
    assert [item.context["key_path"] for item in found] == ["columns.0.description", "columns.1.description"]
    assert len({stable_fingerprint(item) for item in found}) == 2


def test_sst_lod011_names_a_list_item_by_its_name_so_inserting_one_above_keeps_the_fingerprint() -> None:
    def found(text: bytes) -> list[Diagnostic]:
        return [item for item in parsed(text).diagnostics if item.code == "SST-LOD011"]

    later = b"  - name: amount\n    description: >\n      two\n"
    [before] = found(b"columns:\n" + later)
    [after] = found(b"columns:\n  - name: order_id\n    description: plain\n" + later)
    assert after.context["key_path"] == "columns[amount].description"
    assert stable_fingerprint(after) == stable_fingerprint(before)
    # An item with no string name, or a name a sibling repeats, falls back to its index.
    same = b"columns:\n  - name: a\n    description: >\n      x\n  - name: a\n    description: >\n      y\n"
    unnamed = b"c:\n- name: 1\n  description: >\n    x\n- description: >\n    y\n"
    assert [item.context["key_path"] for item in found(same)] == ["columns.0.description", "columns.1.description"]
    assert [item.context["key_path"] for item in found(unnamed)] == ["c.0.description", "c.1.description"]
