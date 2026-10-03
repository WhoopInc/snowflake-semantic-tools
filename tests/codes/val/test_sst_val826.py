"""SST-VAL826: a grant would be issued before certification succeeded, or without a role type.

SST grants nothing: a certified publish tags the version and stops. The publisher refuses a
GRANT while the version's certification is pending, and one that does not say ROLE or
DATABASE ROLE, before it runs.
"""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.app.lifecycle.extensions import _Run
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import OutcomeStatus
from snowflake_semantic_tools.domain.sql import Sql, ident, qname, sql
from snowflake_semantic_tools.domain.validate.publication import statement_diagnostic
from tests.helpers.publications import compiled_skill, publish_skill
from tests.helpers.snowflake_fake import FakeSnowflake

TARGET = QualifiedName.parse("DB.S.MONTH_CLOSE")


def _grant(template: str) -> Sql:
    return sql(template, target=qname(TARGET), role=ident(Identifier.parse("ANALYST")))


TYPED = "GRANT USAGE ON CORTEX EXTENSION {target} TO ROLE {role}"


def test_sst_val826_fires() -> None:
    diagnostic = statement_diagnostic("skill:month-close", _grant(TYPED), certification_pending=True)
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-VAL826",
        Severity.ERROR,
        "skill:month-close",
    )
    assert diagnostic.message == "skill 'month-close': grants would be issued before certification"
    untyped = statement_diagnostic(
        "skill:month-close", _grant("GRANT USAGE ON CORTEX EXTENSION {target} TO {role}"), certification_pending=False
    )
    assert untyped is not None and untyped.code == "SST-VAL826"


def test_sst_val826_refuses_a_grant_before_the_version_is_certified(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_Run, "_create_extension", lambda run: run._run_statement(_grant(TYPED)))
    port = FakeSnowflake(existing=())
    _, result, _ = publish_skill(port, compiled_skill(certified=True))
    [outcome] = result.outcomes
    assert outcome.status is OutcomeStatus.FAILED
    assert [item.message for item in result.diagnostics if item.code == "SST-VAL826"] == [
        "skill 'month-close': grants would be issued before certification"
    ]
    assert not any(statement.lstrip().upper().startswith("GRANT") for script in port.scripts for statement in script)


def test_sst_val826_silent() -> None:
    # A typed grant once certification succeeded passes; a certified publish sends none at all.
    assert statement_diagnostic("skill:month-close", _grant(TYPED), certification_pending=False) is None
    port = FakeSnowflake(existing=())
    _, result, _ = publish_skill(port, compiled_skill(certified=True))
    assert result.success, result.outcomes
    assert "SST-VAL826" not in [item.code for item in result.diagnostics]
