"""SST-MAN006: the compiled manifest was written for another target than the run's."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.manifest import build_manifest, target_mismatch
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.state import Manifest


def compiled_for(target_name: str) -> Manifest:
    return build_manifest(CompileResult(()), target_name=target_name)


def test_sst_man006_fires() -> None:
    assert compiled_for("prod").project["target"] == "prod"
    diagnostic = target_mismatch(compiled_for("prod"), compiled_for("dev"))
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN006", Severity.ERROR)
    assert diagnostic.message == "manifest target 'prod', current target 'dev'"


def test_sst_man006_silent() -> None:
    assert target_mismatch(compiled_for("dev"), compiled_for("dev")) is None
    # A manifest written before targets were recorded agrees with every target.
    assert target_mismatch(compiled_for(""), compiled_for("dev")) is None
