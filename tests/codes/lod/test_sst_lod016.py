"""SST-LOD016: a plain value YAML 1.1 reads as a boolean or null other than as written."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.seam_projects import parsed


def test_sst_lod016_fires() -> None:
    [diagnostic] = [item for item in parsed(b"enabled: yes\n").diagnostics if item.code == "SST-LOD016"]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "f.yml:1: 'enabled' value 'yes' coerced to true"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml", 1)


def test_sst_lod016_silent() -> None:
    assert "SST-LOD016" not in [
        item.code for item in parsed(b"enabled: true\nlabel: 'yes'\nnothing: null\n").diagnostics
    ]
