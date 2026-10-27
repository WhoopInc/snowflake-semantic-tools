"""SST-LOD009: a plain value holds an unquoted `: `, which YAML refuses as a nested mapping."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import parsed, refused


def test_sst_lod009_fires() -> None:
    [diagnostic] = refused(b"description: Note: read this\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD009", Severity.ERROR)
    assert diagnostic.message == "f.yml:1: 'description' value contains an unquoted ':'"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml", 1)


def test_sst_lod009_silent() -> None:
    assert parsed(b"description: 'Note: read this'\n").tree
