"""SST-VAL827: a statement reaches the publisher as raw text rather than as built `Sql`.

The statement builder takes only `Sql` parts, so every interpolated identifier was validated
on the way in; the publisher refuses anything else before it runs, and the publish fails
with what it wrote so far.
"""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.app.lifecycle.extensions import _Run
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import OutcomeStatus
from snowflake_semantic_tools.domain.sql import sql
from snowflake_semantic_tools.domain.validate.publication import statement_diagnostic
from tests.helpers.publications import compiled_skill, publish_skill
from tests.helpers.recorded_snowflake import RecordedSnowflake

RAW = "CREATE CORTEX EXTENSION DB.S.X; DROP TABLE T"


def test_sst_val827_fires() -> None:
    diagnostic = statement_diagnostic("skill:month-close", RAW, certification_pending=False)
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-VAL827",
        Severity.ERROR,
        "skill:month-close",
    )
    assert diagnostic.message == f"skill 'month-close': '{RAW}' reaches DDL unvalidated"


def test_sst_val827_refuses_raw_text_before_it_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_Run, "_create_extension", lambda run: run._run_statement(RAW))
    port = RecordedSnowflake(existing=())
    _, result, _ = publish_skill(port, compiled_skill())
    [outcome] = result.outcomes
    assert outcome.status is OutcomeStatus.FAILED
    assert [item.message for item in result.diagnostics if item.code == "SST-VAL827"] == [
        f"skill 'month-close': '{RAW}' reaches DDL unvalidated"
    ]
    assert not any("DROP TABLE" in statement for script in port.scripts for statement in script)


def test_sst_val827_silent() -> None:
    assert statement_diagnostic("skill:month-close", sql("SELECT 1"), certification_pending=False) is None
    _, result, _ = publish_skill(RecordedSnowflake(existing=()), compiled_skill())
    assert result.success, result.outcomes
