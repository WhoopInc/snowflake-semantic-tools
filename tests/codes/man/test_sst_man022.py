"""SST-MAN022: the local state file is present and cannot be read."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.fs.local import StateFileStore
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.stored_documents import cached_state, state_refusal, written


def test_sst_man022_fires(tmp_path: Path) -> None:
    path = written(tmp_path, "[1, 2", "state.verify.json")
    diagnostic = state_refusal(path)
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN022", Severity.ERROR)
    assert diagnostic.message.startswith(f"{path} is present and unreadable: ")


def test_sst_man022_silent(tmp_path: Path) -> None:
    store = StateFileStore(tmp_path / "state.verify.json")
    store.write_local(cached_state())
    assert store.read_local() == cached_state()
