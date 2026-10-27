"""SST-PLN044: `sst drop` names an object outside every schema the target publishes to."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.drop import DropObject, DropRequest
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.clocks import FixedClock
from tests.helpers.snowflake_fake import FakeSnowflake

TABLE = QualifiedName.parse("DB.S.SST_STATE")
ELSEWHERE = QualifiedName.parse("OTHER_DB.OTHER.ORDERS")
REQUEST = DropRequest(ELSEWHERE, "semantic_view", "SEMANTIC VIEW", "dev", scope=SchemaScope.parse("DB.S"))


def _port(recorded_in: str) -> FakeSnowflake:
    entry = AppliedEntry("b" * 64, f"{recorded_in}.OTHER_VIEW", "then", "run", APPLIED, "b" * 64, "a" * 64)
    return FakeSnowflake(
        existing=(ELSEWHERE.sql,),
        markers={ELSEWHERE.sql: OwnershipMarker("a" * 64, "b" * 64)},
        state={"semantic_view:other_view": entry},
    )


def _drop(port: FakeSnowflake, request: DropRequest = REQUEST) -> object:
    return DropObject(port, InMemoryStateStore(), FixedClock(), state_table=TABLE).run(request)


def test_sst_pln044_fires() -> None:
    port = _port("DB.S")
    result = _drop(port)
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-PLN044"]  # type: ignore[attr-defined]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "semantic_view:orders: OTHER_DB.OTHER.ORDERS is outside every schema target 'dev' publishes to (DB.S)"
    )
    assert port.scripts == []


def test_sst_pln044_silent() -> None:
    # A schema state records an object in is the target's; --outside-target skips the check.
    for port, request in ((_port("OTHER_DB.OTHER"), REQUEST), (_port("DB.S"), replace(REQUEST, outside_target=True))):
        result = _drop(port, request)
        assert [item.code for item in result.diagnostics] == []  # type: ignore[attr-defined]
        assert port.scripts == [("DROP SEMANTIC VIEW OTHER_DB.OTHER.ORDERS",)]
