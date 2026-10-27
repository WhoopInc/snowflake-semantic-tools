"""SST-APL039: an absent schema the plan creates first could not be created."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import change, changeset, rendered, state
from tests.helpers.clocks import FixedClock
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.snowflake_fake import FakeSnowflake, failed

_SCHEMAS = (SchemaScope.parse("DB.FRESH"), SchemaScope.parse("SST.STATE"))


def _apply(port: FakeSnowflake):  # type: ignore[no-untyped-def]
    store = InMemoryStateStore()
    planned = replace(changeset(change(rendered())), schemas=_SCHEMAS)
    result = ApplyArtifacts(port, store, FixedClock(), state_table=rendered("STATE").target).run(planned, state())
    return result, store


def test_sst_apl039_fires() -> None:
    port = FakeSnowflake()
    port.execute_results = [failed("Insufficient privileges to operate on database 'DB'")]
    result, store = _apply(port)
    diagnostic = only(result.diagnostics, "SST-APL039")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "DB.FRESH: the schema could not be created: Insufficient privileges to operate on database 'DB'"
    )
    # Nothing else ran: no second schema, no lock, no write.
    assert port.scripts == [("CREATE SCHEMA IF NOT EXISTS DB.FRESH",)]
    assert not result.state_written and store.writes == []


def test_sst_apl039_silent() -> None:
    port = FakeSnowflake()
    result, _ = _apply(port)
    assert "SST-APL039" not in codes(result.diagnostics) and result.success
    # Each schema is created first, in plan order, before the lock and the write.
    assert port.scripts[:2] == [("CREATE SCHEMA IF NOT EXISTS DB.FRESH",), ("CREATE SCHEMA IF NOT EXISTS SST.STATE",)]
    assert len(port.scripts) > 2
