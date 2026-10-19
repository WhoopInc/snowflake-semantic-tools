"""`sst ask --transcript`: how a run's transcript grows, and where it is written."""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.fs.local import TranscriptFiles
from snowflake_semantic_tools.adapters.paths import UnsafeWrite
from snowflake_semantic_tools.app.ask import find_agent
from snowflake_semantic_tools.app.ask_session import AskSession, start_session
from snowflake_semantic_tools.app.ask_transcript import TranscriptRecorder
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.domain.ask.conversation import Turn
from snowflake_semantic_tools.domain.ask.transcript import TranscriptHead
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake

NOW = datetime(2026, 10, 19, 12, tzinfo=UTC)
HEAD = TranscriptHead("ab12cd34", NOW.isoformat(), "1.0.0", "dev", published=False)


class _Store:
    def __init__(self, *, refuse: int = 0) -> None:
        self.written: dict[str, Mapping[str, object]] = {}
        self.refuse = refuse

    def write(self, name: str, document: Mapping[str, object]) -> str:
        if self.refuse:
            self.refuse -= 1
            raise OSError("disk full")
        self.written[name] = document
        return name


def _session(tmp_path: Path, name: str = "jaffle_analytics_agent") -> AskSession:
    result = compiling.compile_result(project_paths(project_copy(tmp_path)), None, DBT_MANIFEST)
    agent = find_agent(result, name)
    assert isinstance(agent, CompiledAgent)
    started = start_session(
        FakeSnowflake(), agent, result, {}, scope=SchemaScope.parse("SCR.X"), session="ab", now=NOW, published=True
    )
    assert not isinstance(started, Diagnostic)
    return started


def test_the_recorder_numbers_each_new_spec_per_agent_and_writes_as_it_grows(tmp_path: Path) -> None:
    store = _Store()
    recorder = TranscriptRecorder(store, HEAD, lambda: NOW)
    assert recorder.save() is None and store.written == {}
    session = _session(tmp_path)
    assert recorder.spec(session) is None and recorder.spec(session) is None
    assert recorder.turn(session, "q", Turn(answer="a"), NOW) is None
    session.conversation.spec_json = json.dumps({"instructions": {"response": "French."}})
    assert recorder.spec(session, ("instructions.response",)) is None
    other = _session(tmp_path / "other", "jaffle_delivery_agent")
    assert recorder.turn(other, "q2", Turn(), NOW) is None
    assert [(item.agent, item.revision) for item in recorder.revisions] == [
        ("agent:jaffle_analytics_agent", 1),
        ("agent:jaffle_analytics_agent", 2),
        ("agent:jaffle_delivery_agent", 1),
    ]
    assert [item.revision for item in recorder.turns] == [1, 1]
    [document] = store.written.values()
    assert document["turn_count"] == 2 and recorder.path == next(iter(store.written))


def test_a_write_that_fails_warns_once_and_is_tried_again(tmp_path: Path) -> None:
    store = _Store(refuse=2)
    recorder = TranscriptRecorder(store, HEAD, lambda: NOW)
    session = _session(tmp_path)
    warned = recorder.spec(session)
    assert warned is not None and warned.code == "SST-ASK012" and "disk full" in warned.message
    assert recorder.turn(session, "q", Turn(), NOW) is None and recorder.path is None
    assert recorder.save() is None and recorder.path is not None


def test_transcript_files_are_written_indented_inside_the_build_directory(tmp_path: Path) -> None:
    files = TranscriptFiles(tmp_path / "sst_target" / "ask", root=tmp_path)
    where = files.write("t.json", {"b": 1, "a": "é"})
    assert where == str(tmp_path / "sst_target" / "ask" / "t.json")
    assert Path(where).read_text(encoding="utf-8") == '{\n  "b": 1,\n  "a": "é"\n}\n'
    for name in ("../t.json", "a/b.json", "", ".."):
        with pytest.raises(UnsafeWrite):
            files.write(name, {})
    (tmp_path / "outside").mkdir()
    (tmp_path / "linked").symlink_to(tmp_path / "outside")
    with pytest.raises(UnsafeWrite):
        TranscriptFiles(tmp_path / "linked", root=tmp_path).write("t.json", {})
    assert dataclasses.is_dataclass(HEAD)
