"""SST-VAL020: a connected rule was skipped because no Snowflake connection was available."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.app_ports import InMemorySnowflake


def test_sst_val020_fires() -> None:
    found = ValidateArtifacts().run(CompileResult(()), strict=False, connected=False).diagnostics
    assert [item.context["rule_id"] for item in found] == ["SST-VAL418", "SST-VAL415", "SST-VAL212", "SST-VAL218"]
    assert found[0].severity is Severity.INFO
    assert found[0].message == "SST-VAL418 skipped: Snowflake syntax checking was disabled"


def test_sst_val020_silent() -> None:
    result = ValidateArtifacts(InMemorySnowflake()).run(CompileResult(()), strict=False, connected=True)
    assert [item.code for item in result.diagnostics] == []
