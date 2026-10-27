"""SST-ASK012: `sst ask --transcript` could not write its transcript file."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

from snowflake_semantic_tools.adapters.fs.local import TranscriptFiles
from snowflake_semantic_tools.app.ask import find_agent
from snowflake_semantic_tools.app.ask_session import AskSession, start_session
from snowflake_semantic_tools.app.ask_transcript import TranscriptRecorder, TranscriptStore
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.domain.ask.transcript import TranscriptHead
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

_NOW = datetime(2026, 10, 19, 12, tzinfo=UTC)


def _session(tmp_path: Path) -> AskSession:
    result = compiling.compile_result(project_paths(project_copy(tmp_path)), None, DBT_MANIFEST)
    agent = find_agent(result, "jaffle_minimal_agent")
    assert isinstance(agent, CompiledAgent)
    started = start_session(
        FakeSnowflake(), agent, result, {}, scope=SchemaScope.parse("SCR.X"), session="ab", now=_NOW, published=True
    )
    assert not isinstance(started, Diagnostic)
    return started


def _recorder(store: TranscriptStore) -> TranscriptRecorder:
    return TranscriptRecorder(store, TranscriptHead("ab12cd34", _NOW.isoformat(), "1.0", "dev"), lambda: _NOW)


def test_sst_ask012_fires(tmp_path: Path) -> None:
    (tmp_path / "sst_target").write_text("a file where the folder goes", encoding="utf-8")
    recorder = _recorder(TranscriptFiles(tmp_path / "sst_target" / "ask", root=tmp_path))
    diagnostic = recorder.spec(_session(tmp_path / "project"))
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-ASK012", Severity.WARNING)
    assert diagnostic.message.startswith(
        "the transcript 20261019T120000Z-jaffle_minimal_agent-ab12cd34.json could not be written: "
    )


def test_sst_ask012_silent(tmp_path: Path) -> None:
    written: dict[str, Mapping[str, object]] = {}

    class Kept:
        def write(self, name: str, document: Mapping[str, object]) -> str:
            written[name] = document
            return name

    assert _recorder(Kept()).spec(_session(tmp_path)) is None and written
