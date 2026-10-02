"""SST-PRT005: a relation a planned change needs does not exist, so apply fails the change and runs nothing."""

from __future__ import annotations

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from snowflake_semantic_tools.domain.model.lifecycle import OutcomeStatus
from tests.helpers.app_ports import FixedClock, InMemorySnowflake, InMemoryStateStore
from tests.helpers.artifact_builders import change, changeset, rendered, state


def _apply(port: InMemorySnowflake) -> list[tuple[OutcomeStatus, str | None, str | None]]:
    use_case = ApplyArtifacts(port, InMemoryStateStore(), FixedClock(), state_table=rendered("STATE").target, actor="R")
    result = use_case.run(changeset(change(rendered("ORDERS"))), state())
    return [
        (item.status, item.error.code if item.error else None, item.error.message if item.error else None)
        for item in result.outcomes
    ]


def test_sst_prt005_fires() -> None:
    port = InMemorySnowflake()
    port.existing = {"ELSEWHERE"}
    assert _apply(port) == [(OutcomeStatus.FAILED, "SST-PRT005", "missing required relations: DB.SCHEMA.T")]
    assert ERROR_REGISTRY["SST-PRT005"].severity is Severity.ERROR
    assert port.scripts == []


def test_sst_prt005_silent() -> None:
    assert [code for _, code, _ in _apply(InMemorySnowflake())] == [None]
