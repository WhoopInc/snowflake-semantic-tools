"""SST-PRS019: a boolean field, here a ground truth's `immutable`, holds something else."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.evals.dataset import parse_dataset
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.seam_projects import parsed

FILE = "agents/sales/evals/dataset.yml"


def _found(code: str, rows: str) -> list[Diagnostic]:
    diagnostics: list[Diagnostic] = []
    parse_dataset((FILE, parsed(f"agent: sales\nquestions:\n{rows}", FILE)), diagnostics)
    return [item for item in diagnostics if item.code == code]


ROW = "  - question: What sold?\n    ground_truth:\n      ground_truth_output: Pizza\n      immutable: {value}\n"


def test_sst_prs019_fires() -> None:
    [diagnostic] = _found("SST-PRS019", ROW.format(value="'yes'"))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"{FILE}: 'immutable' is 'yes', expected a boolean"


def test_sst_prs019_silent() -> None:
    assert _found("SST-PRS019", ROW.format(value="true")) == []
