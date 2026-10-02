"""SST-LOD011: a multi-line value is a folded scalar, which joins its lines."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import parsed


def test_sst_lod011_fires() -> None:
    [diagnostic] = [item for item in parsed(b"a: >\n  one\n  two\n").diagnostics if item.code == "SST-LOD011"]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "f.yml:1: 'a' uses a folded scalar"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml", 1)


def test_sst_lod011_silent() -> None:
    assert "SST-LOD011" not in [item.code for item in parsed(b"a: |-\n  one\n  two\n").diagnostics]
