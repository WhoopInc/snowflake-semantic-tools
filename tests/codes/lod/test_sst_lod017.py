"""SST-LOD017: a file begins with a UTF-8 byte-order mark."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import parsed, refused


def test_sst_lod017_fires() -> None:
    [diagnostic] = refused(b"\xef\xbb\xbfa: 1\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD017", Severity.ERROR)
    assert diagnostic.message == "f.yml begins with a BOM"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml")


def test_sst_lod017_silent() -> None:
    assert parsed(b"a: 1\n").tree
