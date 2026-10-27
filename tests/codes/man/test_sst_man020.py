"""SST-MAN020: offline, there is no local state cache, so every artifact reads as new."""

from __future__ import annotations

from snowflake_semantic_tools.app.state import read_state
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.apply_runs import STATE_TABLE
from tests.helpers.artifact_builders import target
from tests.helpers.stored_documents import cached_state


def test_sst_man020_fires() -> None:
    state, diagnostics = read_state(InMemoryStateStore(), None, state_table=STATE_TABLE, target=target())
    [diagnostic] = diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN020", Severity.WARNING)
    assert diagnostic.message == "no state file; treating every artifact as new"
    assert state.applied == {}


def test_sst_man020_silent() -> None:
    _, diagnostics = read_state(InMemoryStateStore(cached_state()), None, state_table=STATE_TABLE, target=target())
    assert list(diagnostics) == []
