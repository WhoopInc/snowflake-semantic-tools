"""SST-LOD012: a file carries trailing whitespace or CRLF line endings."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import parsed


def test_sst_lod012_fires() -> None:
    [crlf, trailing] = [item for item in parsed(b"a: 1 \r\nb: 2\r\n").diagnostics if item.code == "SST-LOD012"]
    assert (crlf.severity, trailing.severity) == (Severity.WARNING, Severity.WARNING)
    assert crlf.message == "f.yml: 2 line(s) end in CRLF"
    assert trailing.message == "f.yml: trailing whitespace on 1 line(s), first at line 1"
    assert (crlf.subject, trailing.origin) == (None, Origin("f.yml", 1))


def test_sst_lod012_silent() -> None:
    assert parsed(b"a: 1\nnote: |\n  text\n").diagnostics == ()
