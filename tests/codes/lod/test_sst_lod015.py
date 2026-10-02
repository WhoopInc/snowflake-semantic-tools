"""SST-LOD015: a mapping key reads as a number, a boolean, a date or null rather than a string."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import parsed, refused


def test_sst_lod015_fires() -> None:
    [diagnostic] = refused(b"1: one\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD015", Severity.ERROR)
    assert diagnostic.message == "f.yml:1: mapping key 1 is not a string"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml", 1)


def test_sst_lod015_silent() -> None:
    assert parsed(b"'1': one\non: a word-boolean key stays a string\n").tree
