"""SST-REG015: a module outside diagnostics/ compares severities itself.

Raised while a registry is built, never reported for a project.
"""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY
from snowflake_semantic_tools.domain.diagnostics.integrity import severity_comparisons

PACKAGE = Path(__file__).resolve().parents[3] / "snowflake_semantic_tools"


def test_sst_reg015_fires() -> None:
    source = "def blocked(item):\n    return item.severity is Severity.ERROR\n"
    [error] = severity_comparisons("app.example", source)
    assert (error.code, str(error)) == (
        "SST-REG015",
        "SST-REG015: app.example:2 compares severities outside diagnostics/",
    )
    assert ERROR_REGISTRY["SST-REG015"].always_error


def test_sst_reg015_silent() -> None:
    assert severity_comparisons("app.example", "def blocked(item):\n    return item.blocks\n") == ()
    found = [
        str(error)
        for path in sorted(PACKAGE.rglob("*.py"))
        if "diagnostics" not in path.relative_to(PACKAGE).parts
        for error in severity_comparisons(path.relative_to(PACKAGE).as_posix(), path.read_text(encoding="utf-8"))
    ]
    assert found == []
