"""SST-LOD202: CRLF line endings are read as LF, leaving the file as it is."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import parsed


def test_sst_lod202_fires() -> None:
    [diagnostic] = [item for item in parsed(b"a: 1\r\nb: 2\r\n").diagnostics if item.code == "SST-LOD202"]
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "f.yml normalised 2 line endings on read"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml")


def test_sst_lod202_silent() -> None:
    assert "SST-LOD202" not in [item.code for item in parsed(b"a: 1\nb: 2\n").diagnostics]
