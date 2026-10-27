"""SST-VAL876: a CoWork profile (a Snowflake private preview) carries a tool it may not."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml.cowork_profiles import load_cowork_profiles
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from tests.helpers.diagnostic_filters import codes, only

FILE = "cowork_profiles/everyone/cowork_profile.yml"
SUBAGENTS = "  subagents:\n    - agent: \"{{ agent('sales') }}\"\n"


def _write(tmp_path: Path, tool_type: str) -> None:
    path = tmp_path / FILE
    path.parent.mkdir(parents=True)
    tools = f"  tools:\n    - type: {tool_type}\n      name: helper\n"
    path.write_text(f"name: everyone\nspec:\n{SUBAGENTS}{tools}", encoding="utf-8")


@pytest.mark.parametrize("tool_type", ["cortex_search", "data_to_chart", "agent_router"])
def test_sst_val876_fires(tmp_path: Path, tool_type: str) -> None:
    _write(tmp_path, tool_type)
    _, diagnostics = load_cowork_profiles(tmp_path)
    diagnostic = only(diagnostics, "SST-VAL876")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        f"cowork profile 'everyone': spec.tools[0] has type '{tool_type}', not web_search or code_execution"
    )
    assert (diagnostic.subject, diagnostic.origin) == ("cowork_profile:everyone", Origin(FILE, 6, 7))


@pytest.mark.parametrize("tool_type", ["web_search", "code_execution"])
def test_sst_val876_silent(tmp_path: Path, tool_type: str) -> None:
    _write(tmp_path, tool_type)
    _, diagnostics = load_cowork_profiles(tmp_path)
    assert codes(diagnostics) == []
