"""SST-MAN027: the local state cache disagrees with the state table, which wins."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.state import read_state
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.apply_runs import STATE_TABLE
from tests.helpers.artifact_builders import target
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.stored_documents import ENTRY, cached_state


def test_sst_man027_fires() -> None:
    store = InMemoryStateStore(cached_state())
    port = FakeSnowflake()
    port.remote_state = MappingProxyType({})
    state, diagnostics = read_state(store, port, state_table=STATE_TABLE, target=target())
    [diagnostic] = diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN027", Severity.WARNING)
    assert diagnostic.message == f"local state.json for target verify disagrees with {STATE_TABLE.sql}; the table wins"
    assert state.applied == {} and store.writes[-1].applied == {}


def test_sst_man027_silent() -> None:
    port = FakeSnowflake()
    port.remote_state = MappingProxyType({"semantic_view:v": ENTRY})
    _, diagnostics = read_state(InMemoryStateStore(cached_state()), port, state_table=STATE_TABLE, target=target())
    assert list(diagnostics) == []
