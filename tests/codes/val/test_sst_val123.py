"""SST-VAL123: a view would publish a metric although a derived-metric restriction failed for it."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Severity
from snowflake_semantic_tools.domain.model.semantic_view import Metric, SemanticView, Table
from snowflake_semantic_tools.domain.validate.semantic_view import restriction_diagnostics
from tests.helpers.sql_values import authored

VIEW = SemanticView("DB.S.V", (Table("T", "DB.S.T"),), metrics=(Metric("SUMMED", authored("SUM(T.TOTAL_METRIC)")),))


def test_sst_val123_fires() -> None:
    failed = D("SST-VAL103", metric="summed", other="total", subject="metric:summed")
    [found] = restriction_diagnostics(VIEW, (failed,), artifact="semantic_view:v")
    assert found.severity is Severity.ERROR
    assert found.message == "metric 'summed' would publish without the derived-metric restriction check"
    assert found.subject == "semantic_view:v"


def test_sst_val123_silent() -> None:
    elsewhere = D("SST-VAL103", metric="other", other="total", subject="metric:other")
    assert restriction_diagnostics(VIEW, (elsewhere,), artifact="semantic_view:v") == ()
