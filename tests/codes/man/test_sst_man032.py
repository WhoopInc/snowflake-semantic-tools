"""SST-MAN032: a file given to `--refresh-cost` is not a comparison report SST wrote.

Fires for a document of another schema; a report as SST writes it stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.compare_refresh import refresh_cost
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.evaluate.compare_json import report_json
from tests.helpers.compare_reports import compared_report
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_man032_fires() -> None:
    diagnostic = refresh_cost(FakeSnowflake(), {"schema": "sst.plan/1"}, path="c.json", now="2026-10-22T15:00:00Z")
    assert isinstance(diagnostic, Diagnostic)
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN032", Severity.ERROR)
    assert diagnostic.message == (
        "c.json cannot have its cost refreshed: the report's schema is 'sst.plan/1', not 'sst.compare/1'"
    )


def test_sst_man032_silent() -> None:
    refreshed = refresh_cost(FakeSnowflake(), report_json(compared_report()), path="c.json", now="2026-10-22T15:00:00Z")
    assert not isinstance(refreshed, Diagnostic)
