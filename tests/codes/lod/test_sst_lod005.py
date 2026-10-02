"""SST-LOD005: one mapping writes a key twice; YAML would silently keep the later."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import parsed, refused


def test_sst_lod005_fires() -> None:
    [diagnostic] = refused(b"a: 1\na: 2\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD005", Severity.ERROR)
    assert diagnostic.message == "f.yml:2: duplicate key 'a'"
    assert diagnostic.subject is None


def test_sst_lod005_silent() -> None:
    assert parsed(b"a: 1\nb: 2\n").tree
