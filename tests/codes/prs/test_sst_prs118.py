"""SST-PRS118: an agent's sample question is a bare string rather than a mapping."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import agent_spec_findings


def test_sst_prs118_fires(tmp_path: Path) -> None:
    [diagnostic] = agent_spec_findings(
        tmp_path, "SST-PRS118", "  instructions:\n    sample_questions:\n      - What sold\n"
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent:sales: sample_questions[0] is a string, expected a mapping"
    assert diagnostic.subject == "agent:sales"


def test_sst_prs118_silent(tmp_path: Path) -> None:
    spec = "  instructions:\n    sample_questions:\n      - question: What sold?\n"
    assert agent_spec_findings(tmp_path, "SST-PRS118", spec) == []
