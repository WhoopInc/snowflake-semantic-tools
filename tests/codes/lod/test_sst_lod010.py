"""SST-LOD010: a structural line is indented with a tab, which YAML forbids."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import parsed, refused


def test_sst_lod010_fires() -> None:
    [diagnostic] = refused(b"a:\n\tb: 1\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD010", Severity.ERROR)
    assert diagnostic.message == "f.yml:2: tab used for indentation"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml", 2)


def test_sst_lod010_silent() -> None:
    assert parsed(b"a:\n  b: 1\nnote: |\n  \tA tab inside a block scalar is text.\n").tree
