"""SST-LOD004: a `{{ ... }}` template is unterminated or nested."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import parsed, refused


def test_sst_lod004_fires() -> None:
    [diagnostic] = refused(b"a: {{ ref(\n")
    assert (diagnostic.code, diagnostic.severity) == ("SST-LOD004", Severity.ERROR)
    assert diagnostic.message == "f.yml:1:4: malformed template: unterminated template expression"
    assert diagnostic.subject is None


def test_sst_lod004_silent() -> None:
    assert parsed(b"a: {{ ref('m') }}\n").tree
