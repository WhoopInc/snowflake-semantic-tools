"""SST-LOD014: a mapping uses a merge key (`<<`), which SST does not expand."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import parsed, refused


def test_sst_lod014_fires() -> None:
    [diagnostic] = refused(b"a:\n  <<: {b: 1}\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD014", Severity.ERROR)
    assert diagnostic.message == "f.yml:2: merge keys are not supported"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml", 2)


def test_sst_lod014_silent() -> None:
    assert parsed(b"a:\n  b: 1\n").tree
