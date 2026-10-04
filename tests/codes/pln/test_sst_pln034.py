"""SST-PLN034: an artifact state records is no longer declared; its prune is reported, never executed."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from tests.helpers.artifact_builders import empty_state
from tests.helpers.lifecycle_codes import month_close, plan_skills, publish_skills
from tests.helpers.snowflake_fake import FakeSnowflake


def _planned(*, declared: bool) -> list[Diagnostic]:
    compiled = month_close()
    port = FakeSnowflake(existing=())
    published = publish_skills(port, compiled, empty_state())
    changeset = plan_skills(port, compiled if declared else {}, published, prune=True)
    if not declared:
        assert [(change.action, change.prune_executable) for change in changeset.changes] == [(Action.PRUNE, False)]
    return [item for item in changeset.diagnostics if item.code == "SST-PLN034"]


def test_sst_pln034_fires() -> None:
    [diagnostic] = _planned(declared=False)
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message.startswith("skill:month-close is no longer declared; SST never removes ")


def test_sst_pln034_silent() -> None:
    assert _planned(declared=True) == []
