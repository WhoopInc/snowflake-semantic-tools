"""SST-PRS002: a semantic member omits a field it requires, here a metric's `expr`."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.reference_project import edited, load, member_file, project_copy, reported
from tests.helpers.seam_projects import COUNTED, SmallProject, metric_file


def test_sst_prs002_fires(tmp_path: Path) -> None:
    [diagnostic] = coded(SmallProject(tmp_path, files=metric_file("")).load().diagnostics, "SST-PRS002")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "metric:total: required field 'expr' is missing"
    assert diagnostic.subject == "metric:total"


def test_sst_prs002_silent(tmp_path: Path) -> None:
    assert coded(SmallProject(tmp_path, files=metric_file(COUNTED)).load().diagnostics, "SST-PRS002") == []


QUESTION = '  question: "How many orders are there in each fulfilment state?"\n'


@pytest.mark.parametrize("question", ["", '  question: "  "\n'], ids=["missing", "blank"])
def test_sst_prs002_fires_for_a_verified_query_without_a_question(tmp_path: Path, question: str) -> None:
    project = edited(tmp_path, member_file("verified_queries", "order_count_by_state"), QUESTION, question)
    [diagnostic] = reported(project, "SST-PRS002")
    assert diagnostic.message == "verified_query:order_count_by_state: required field 'question' is missing"
    assert diagnostic.subject == "verified_query:order_count_by_state"
    assert diagnostic.origin is not None
    assert diagnostic.origin.file == member_file("verified_queries", "order_count_by_state")


def test_sst_prs002_silent_for_a_verified_query_with_a_question(tmp_path: Path) -> None:
    assert reported(project_copy(tmp_path), "SST-PRS002") == []


def test_a_verified_query_without_a_name_is_reported_by_prs107(tmp_path: Path) -> None:
    project = edited(
        tmp_path, member_file("verified_queries", "order_count_by_state"), "  name: order_count_by_state\n", ""
    )
    assert [item.code for item in load(project).diagnostics if item.code in ("SST-PRS002", "SST-PRS107")] == [
        "SST-PRS107"
    ]
