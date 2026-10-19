"""SST-LOD012: a file carries trailing whitespace or CRLF line endings."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import parsed


def test_sst_lod012_fires() -> None:
    [crlf, trailing] = [item for item in parsed(b"a: 1 \r\nb: 2\r\n").diagnostics if item.code == "SST-LOD012"]
    assert (crlf.severity, trailing.severity) == (Severity.INFO, Severity.INFO)
    assert crlf.message == "f.yml: 2 line(s) end in CRLF"
    assert trailing.message == "f.yml: trailing whitespace on 1 line(s), first at line 1"
    assert (crlf.subject, trailing.origin) == (None, Origin("f.yml", 1))


def test_sst_lod012_silent() -> None:
    assert parsed(b"a: 1\nnote: |\n  text\n").diagnostics == ()


def test_trailing_whitespace_never_reaches_a_block_scalar_value() -> None:
    # YAML keeps trailing spaces inside `|`; the loader drops them, so they cannot publish.
    assert parsed(b"note: |-\n  one  \n  two\t\n").tree["note"] == "one\ntwo"
    assert parsed(b"note: |-\r\n  one \r\n  two\r\n").tree["note"] == "one\ntwo"
