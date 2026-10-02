"""SST-MAN023: the local state file declares a schema SST does not read."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.fs.local import StateFileStore
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.stored_documents import cached_state, state_refusal, written


def test_sst_man023_fires(tmp_path: Path) -> None:
    path = written(tmp_path, {**cached_state().as_dict(), "schema_version": 99}, "state.verify.json")
    diagnostic = state_refusal(path)
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN023", Severity.ERROR)
    assert diagnostic.message == f"{path} declares schema 99"


def test_sst_man023_silent(tmp_path: Path) -> None:
    path = written(tmp_path, cached_state().as_dict(), "state.verify.json")
    assert StateFileStore(path).read_local() == cached_state()
