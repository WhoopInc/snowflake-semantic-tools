"""SST-PRS023: an agent's passthrough block would overwrite a key SST renders itself."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import agent_spec_findings


def test_sst_prs023_fires(tmp_path: Path) -> None:
    [diagnostic] = agent_spec_findings(tmp_path, "SST-PRS023", "  passthrough:\n    models: {orchestration: x}\n")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "sales.spec: passthrough key 'models.orchestration' is already rendered by SST"
    assert diagnostic.subject == "agent:sales"


def test_sst_prs023_silent(tmp_path: Path) -> None:
    assert agent_spec_findings(tmp_path, "SST-PRS023", "  passthrough:\n    experimental: {}\n") == []
    # A key added inside a section SST writes merges into it, as an import keeps one.
    merged = agent_spec_findings(tmp_path / "merged", "SST-PRS023", "  passthrough:\n    models: {fallback: other}\n")
    assert merged == []
