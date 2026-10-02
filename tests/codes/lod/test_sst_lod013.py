"""SST-LOD013: a document uses an alias, which SST does not expand."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import parsed, refused


def test_sst_lod013_fires() -> None:
    [diagnostic] = refused(b"a: &x 1\nb: 2\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD013", Severity.ERROR)
    assert diagnostic.message == "f.yml:1: YAML anchors are not supported"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml", 1, 4)


def test_sst_lod013_silent() -> None:
    assert parsed(b"a: '&x 1'\nb: '*x'\n").tree
