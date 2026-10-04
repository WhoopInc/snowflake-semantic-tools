"""SST-PRS018: an element of a collection, here an agent's `spec.tools`, has the wrong shape."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import agent_spec_findings


def test_sst_prs018_fires(tmp_path: Path) -> None:
    [diagnostic] = agent_spec_findings(tmp_path, "SST-PRS018", "  tools: [1]\n")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agents/sales/agent.yml: 'tools'[0] expects mapping, found int"
    assert diagnostic.subject is None


def test_sst_prs018_silent(tmp_path: Path) -> None:
    assert agent_spec_findings(tmp_path, "SST-PRS018", "  tools:\n    - type: data_to_chart\n") == []
