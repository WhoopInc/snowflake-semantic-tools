"""SST-PRS117: an eval dataset row has no question, or nothing to score against."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.evals.dataset import parse_dataset
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.seam_projects import parsed

FILE = "agents/sales/evals/dataset.yml"


def _found(code: str, rows: str) -> list[Diagnostic]:
    diagnostics: list[Diagnostic] = []
    parse_dataset((FILE, parsed(f"agent: sales\nquestions:\n{rows}", FILE)), diagnostics)
    return [item for item in diagnostics if item.code == code]


def test_sst_prs117_fires() -> None:
    [diagnostic] = _found("SST-PRS117", "  - ground_truth:\n      ground_truth_output: Pizza\n")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"{FILE}: row 0 has no question"
    assert diagnostic.subject is None


def test_sst_prs117_silent() -> None:
    assert _found("SST-PRS117", "  - question: What sold?\n    ground_truth:\n      ground_truth_output: Pizza\n") == []
