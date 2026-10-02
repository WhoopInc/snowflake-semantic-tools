"""SST-INT003: a compiler returned an artifact of a type the stream does not place."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompileArtifacts, CompileResult
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.compiled_things import Thing

POSITIONS = {"semantic_view": 100}


def test_sst_int003_fires() -> None:
    merged = CompileArtifacts.merge((CompileResult((Thing("v"), Thing("d", "dashboard"))),), POSITIONS)
    [diagnostic] = merged.diagnostics
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == ("SST-INT003", Severity.ERROR, "dashboard:d")
    assert diagnostic.message == "compile returned artifact type 'dashboard', expected a registered artifact type"
    assert [item.artifact_key for item in merged.compiled] == ["semantic_view:v"]


def test_sst_int003_silent() -> None:
    merged = CompileArtifacts.merge((CompileResult((Thing("v"),)),), POSITIONS)
    assert merged.diagnostics == () and len(merged.compiled) == 1
