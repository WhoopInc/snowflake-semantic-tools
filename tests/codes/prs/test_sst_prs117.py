"""SST-PRS117: an eval dataset row has no question, or nothing to score against."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.prs_codes import FILE, dataset_findings


def test_sst_prs117_fires() -> None:
    [diagnostic] = dataset_findings("SST-PRS117", "  - ground_truth:\n      ground_truth_output: Pizza\n")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"{FILE}: row 0 has no question"
    assert diagnostic.subject is None


def test_sst_prs117_silent() -> None:
    assert (
        dataset_findings(
            "SST-PRS117", "  - question: What sold?\n    ground_truth:\n      ground_truth_output: Pizza\n"
        )
        == []
    )
