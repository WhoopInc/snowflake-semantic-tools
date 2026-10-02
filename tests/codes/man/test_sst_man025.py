"""SST-MAN025: the target's state file was written for another schema, so two writers share one file."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.state import read_state
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import Identifier
from tests.helpers.app_ports import InMemorySnowflake, InMemoryStateStore
from tests.helpers.apply_runs import STATE_TABLE
from tests.helpers.artifact_builders import target
from tests.helpers.stored_documents import cached_state


def test_sst_man025_fires() -> None:
    other = replace(target(), schema=Identifier.parse("other_schema"))
    store = InMemoryStateStore(cached_state(recorded_for=other))
    port = InMemorySnowflake()
    port.remote_state = MappingProxyType({})
    state, diagnostics = read_state(store, port, state_table=STATE_TABLE, target=target())
    [diagnostic] = diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN025", Severity.ERROR)
    assert diagnostic.message == "target/sst/state.verify.json was written by target verify for DB.OTHER_SCHEMA"
    # The other writer's file is neither read nor rewritten.
    assert state.applied == {} and store.writes == []


def test_sst_man025_silent() -> None:
    _, diagnostics = read_state(InMemoryStateStore(cached_state()), None, state_table=STATE_TABLE, target=target())
    assert list(diagnostics) == []
