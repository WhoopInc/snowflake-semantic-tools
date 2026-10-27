"""SST-PRS024: an agent's passthrough block carries keys SST renders without checking them."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import agent_spec_findings


def test_sst_prs024_fires(tmp_path: Path) -> None:
    [diagnostic] = agent_spec_findings(
        tmp_path, "SST-PRS024", "  tools:\n    - type: data_to_chart\n      tool_spec_passthrough: {x: 1}\n"
    )
    assert diagnostic.severity is Severity.WARNING
    assert (
        diagnostic.message
        == "agent:sales.tools[0].tool_spec_passthrough: 1 passthrough keys will be rendered unvalidated"
    )
    assert diagnostic.subject == "agent:sales"


def test_sst_prs024_silent(tmp_path: Path) -> None:
    assert agent_spec_findings(tmp_path, "SST-PRS024", "  tools:\n    - type: data_to_chart\n") == []
