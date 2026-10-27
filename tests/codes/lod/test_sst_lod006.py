"""SST-LOD006: a file's bytes are not UTF-8."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import parsed, refused


def test_sst_lod006_fires() -> None:
    [diagnostic] = refused(b"a: \xff\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD006", Severity.ERROR)
    assert diagnostic.message == "f.yml: invalid UTF-8 at byte 3"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml")


def test_sst_lod006_silent() -> None:
    assert parsed(b"a: caf\xc3\xa9\n").tree
