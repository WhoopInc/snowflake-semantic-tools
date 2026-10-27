"""SST-PRS019: a boolean field, here a ground truth's `immutable`, holds something else."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import FILE, dataset_findings

ROW = "  - question: What sold?\n    ground_truth:\n      ground_truth_output: Pizza\n      immutable: {value}\n"


def test_sst_prs019_fires() -> None:
    [diagnostic] = dataset_findings("SST-PRS019", ROW.format(value="'yes'"))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"{FILE}: 'immutable' is 'yes', expected a boolean"


def test_sst_prs019_silent() -> None:
    assert dataset_findings("SST-PRS019", ROW.format(value="true")) == []
