"""SST-LOD001: a file is not valid YAML, reported at the position YAML stopped."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import parsed, refused


def test_sst_lod001_fires() -> None:
    [diagnostic] = refused(b"a: [1\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD001", Severity.ERROR)
    assert diagnostic.message == "f.yml:2:1: expected ',' or ']', but got '<stream end>'"
    assert diagnostic.subject is None


def test_sst_lod001_silent() -> None:
    assert parsed(b"a: [1]\n").tree
