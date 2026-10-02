"""SST-LOD008: a file holds a stream of several YAML documents."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import parsed, refused


def test_sst_lod008_fires() -> None:
    [diagnostic] = refused(b"a: 1\n---\nb: 2\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD008", Severity.ERROR)
    assert diagnostic.message == "f.yml contains 2 documents"
    assert diagnostic.subject is None


def test_sst_lod008_silent() -> None:
    assert parsed(b"---\na: 1\n").tree
