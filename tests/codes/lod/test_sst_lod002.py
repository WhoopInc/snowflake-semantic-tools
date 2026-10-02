"""SST-LOD002: a document's root is a list or a scalar, not a mapping."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import parsed, refused


def test_sst_lod002_fires() -> None:
    [diagnostic] = refused(b"- a\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD002", Severity.ERROR)
    assert diagnostic.message == "f.yml root is list, expected a mapping"
    assert diagnostic.subject is None


def test_sst_lod002_silent() -> None:
    assert parsed(b"a: [x]\n").tree
