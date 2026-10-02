"""SST-MAN024: the local state cache was written by a later SST release."""

from __future__ import annotations

from snowflake_semantic_tools.app.state import read_state
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity
from snowflake_semantic_tools.domain.state import SST_VERSION, LastRun, State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.apply_runs import STATE_TABLE
from tests.helpers.artifact_builders import target
from tests.helpers.stored_documents import cached_state


def read_written_by(version: str) -> tuple[State, DiagnosticBag]:
    cached = cached_state(last_run=LastRun("r", "then", "then", version, "apply", "ok"))
    return read_state(InMemoryStateStore(cached), None, state_table=STATE_TABLE, target=target())


def test_sst_man024_fires() -> None:
    state, diagnostics = read_written_by("99.1.0")
    [diagnostic] = diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN024", Severity.WARNING)
    assert diagnostic.message == f"target/sst/state.verify.json was written by SST 99.1.0; this is {SST_VERSION}"
    assert "semantic_view:v" in state.applied


def test_sst_man024_silent() -> None:
    assert list(read_written_by(SST_VERSION)[1]) == [] and list(read_written_by("0.3.1")[1]) == []
