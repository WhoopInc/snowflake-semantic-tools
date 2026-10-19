"""The full-screen `sst ask` conversation, driven headless with Textual's Pilot against the in-memory Snowflake."""

from __future__ import annotations

import asyncio
import dataclasses
import json
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest
from textual.widgets import Collapsible, OptionList

from snowflake_semantic_tools.app.ask import agent_scope, compiled_agents
from snowflake_semantic_tools.app.ask_transcript import TranscriptRecorder
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.cli.tui.ask_app import AskApp, AskBackend, Summary, _table
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.domain.ask.events import (
    AgentEvent,
    Chart,
    ResultTable,
    RunError,
    Status,
    TextDelta,
    ThinkingDelta,
    ToolResult,
    ToolUse,
    Warning,
)
from snowflake_semantic_tools.domain.ask.transcript import TranscriptHead
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult
from tests.helpers.cli_projects import common, invoke_with_port
from tests.helpers.project_inputs import dev_target
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake, failed


def _backend(tmp_path: Path, port: FakeSnowflake, *, published: bool = False) -> tuple[AskBackend, CompileResult]:
    paths = project_paths(project_copy(tmp_path))
    result = compiling.compile_result(paths, None, DBT_MANIFEST)

    def scope(agent: object) -> SchemaScope:
        found = agent_scope(agent, dev_target())  # type: ignore[arg-type]
        assert not isinstance(found, Diagnostic)
        return found

    backend = AskBackend(port, lambda: result, dict, scope, published)
    return backend, result


async def _until(pilot: object, condition: Callable[[], bool]) -> None:
    for _ in range(400):
        if condition():
            return
        await pilot.pause(0.01)  # type: ignore[attr-defined]
    raise AssertionError("timed out")


def _run(app: AskApp, steps: Callable[[AskApp, object], object]) -> Summary | None:
    async def drive() -> None:
        async with app.run_test(size=(140, 50)) as pilot:
            await steps(app, pilot)  # type: ignore[misc]

    asyncio.run(drive())
    return app.return_value


def test_a_conversation_picks_an_agent_streams_its_blocks_and_ends_with_a_summary(tmp_path: Path) -> None:
    port = FakeSnowflake(state={})
    port.agent_streams = {
        "Why?": [
            Status("planning", "Planning the next steps"),
            ThinkingDelta("Look at "),
            ThinkingDelta("orders."),
            ToolUse("t", "SALES", "cortex_analyst_text_to_sql", {"q": "x"}),
            ToolResult(
                "t",
                "SALES",
                "cortex_analyst_text_to_sql",
                "success",
                "SELECT 1",
                ResultTable(("N",), (("7",),)),
                "DB.S.V",
            ),
            Chart('{"mark":"bar"}'),
            Warning("a tool was skipped"),
            TextDelta("Because "),
            TextDelta("orders."),
        ],
        "Fail?": [RunError("refused", "399569")],
    }
    backend, result = _backend(tmp_path, port)
    app = AskApp(backend, agents=compiled_agents(result), first=None)

    async def steps(app: AskApp, pilot: object) -> None:
        await pilot.click("#picker-filter")  # type: ignore[attr-defined]
        # Esc on the first picker has no conversation to go back to, so it does nothing.
        await pilot.press("escape")  # type: ignore[attr-defined]
        assert app.query_one("#picker").display and not app.query_one("#question").display
        await pilot.press(*"analytics")  # type: ignore[attr-defined]
        await pilot.press("enter")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app._session is not None and not app._busy)
        assert "copies of:" in app.sub_title and "agent not published" in app.sub_title
        question = app.query_one("#question")
        question.value = "Why?"  # type: ignore[attr-defined]
        await pilot.press("enter")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app._summary.turns == 1 and not app._busy)
        assert app._last_answer == "Because orders." and app._last_chart == '{"mark":"bar"}'
        assert [block.title for block in app.query(".block").results(Collapsible)] == [
            "thinking",
            "tool SALES (cortex_analyst_text_to_sql)",
            "result SALES: success · DB.S.V",
        ]
        await pilot.press("ctrl+t")  # type: ignore[attr-defined]
        assert not any(block.display for block in app.query(".block"))
        await pilot.press("ctrl+t")  # type: ignore[attr-defined]
        for command in ("/copy", "/copy-chart", "", "Fail?"):
            question.value = command  # type: ignore[attr-defined]
            await pilot.press("enter")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app._summary.turns == 2 and not app._busy)
        await pilot.press("ctrl+r")  # type: ignore[attr-defined]
        await _until(pilot, lambda: not app._busy and "compiled again" in app.status_text)
        await pilot.press("ctrl+c")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.return_value is not None)

    summary = _run(app, steps)
    assert summary is not None and summary.turns == 2
    assert [item.code for item in summary.diagnostics] == ["SST-ASK010"]
    # The failed turn ended with no final response, so only the first counted its tokens.
    assert summary.line().startswith("agent:jaffle_analytics_agent: 2 turn(s), 10 tokens; removed ")
    assert summary.as_dict()["turns"] == 2
    assert port.threads == {} and len(port.stream_runs) == 2


def test_a_named_agent_opens_at_once_and_ctrl_a_picks_another(tmp_path: Path) -> None:
    port = FakeSnowflake(state={})
    backend, result = _backend(tmp_path, port, published=True)
    agents = compiled_agents(result)
    app = AskApp(backend, agents=agents, first=agents[0])

    async def steps(app: AskApp, pilot: object) -> None:
        await _until(pilot, lambda: app._session is not None and not app._busy)
        first = app._session
        assert "published dependencies" in app.sub_title
        await pilot.press("ctrl+a")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.query_one("#picker").display)
        picker = app.query_one("#picker", OptionList)
        assert picker.highlighted == 0 and not app.query_one("#question").display
        assert "Esc goes back" in app.status_text
        # Esc goes back to the conversation, which never ended.
        await pilot.press("escape")  # type: ignore[attr-defined]
        assert not picker.display and app._session is first and app.status_text == "ready"
        # Picking the agent that is open goes back too.
        await pilot.press("ctrl+a", "enter")  # type: ignore[attr-defined]
        assert not picker.display and app._session is first
        # The arrows move the highlight, and the filter puts it back on the first match.
        await pilot.press("ctrl+a", "down", "down", "up", "pagedown")  # type: ignore[attr-defined]
        assert picker.highlighted == len(agents) - 1
        await pilot.press("pageup", "down")  # type: ignore[attr-defined]
        assert picker.highlighted == 1
        await pilot.press(*"agent")  # type: ignore[attr-defined]
        assert picker.highlighted == 0
        await pilot.press("down", "enter")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app._session is not None and app._session is not first and not app._busy)
        assert app._session is not None and app._session.agent.artifact_key == agents[1].artifact_key
        app.query_one("#question").value = "/exit"  # type: ignore[attr-defined]
        await pilot.press("enter")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.return_value is not None)

    summary = _run(app, steps)
    assert summary is not None and len(summary.agents) == 2 and summary.turns == 0


def test_a_session_that_cannot_start_says_so_and_quits(tmp_path: Path) -> None:
    port = FakeSnowflake(state={})
    port.execute_results = [failed("no CREATE SEMANTIC VIEW")]
    backend, result = _backend(tmp_path, port)
    agents = compiled_agents(result)
    app = AskApp(backend, agents=agents, first=agents[0])

    async def steps(app: AskApp, pilot: object) -> None:
        await _until(pilot, lambda: bool(app._summary.diagnostics))
        await pilot.press("ctrl+r")  # type: ignore[attr-defined]
        await pilot.press("ctrl+d")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.return_value is not None)

    summary = _run(app, steps)
    assert summary is not None and [item.code for item in summary.diagnostics] == ["SST-ASK008"]
    assert summary.line().startswith("no agent:")


def test_an_answer_streamed_in_small_pieces_shows_each_block_once(tmp_path: Path) -> None:
    from textual.widgets._markdown import MarkdownBulletList

    answer = "Here:\n\n" + "".join(f"- item number {index}\n" for index in range(6)) + "\nDone."
    port = FakeSnowflake(state={})
    port.agent_streams = {"q": [TextDelta(answer[start : start + 3]) for start in range(0, len(answer), 3)]}
    backend, result = _backend(tmp_path, port)
    agents = compiled_agents(result)
    app = AskApp(backend, agents=agents, first=agents[0])

    async def steps(app: AskApp, pilot: object) -> None:
        await _until(pilot, lambda: app._session is not None and not app._busy)
        app.query_one("#question").value = "q"  # type: ignore[attr-defined]
        await pilot.press("enter")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app._summary.turns == 1 and not app._busy)
        assert app._answer is not None
        lists = [child for child in app._answer.walk_children() if isinstance(child, MarkdownBulletList)]
        assert len(lists) == 1 and app._answer.source == answer
        await pilot.press("ctrl+d")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.return_value is not None)

    _run(app, steps)


def test_a_long_result_says_how_many_rows_it_leaves_out() -> None:
    from textual.app import App

    async def drive() -> int:
        async with App().run_test():
            return _table(ResultTable(("A", "B"), tuple((str(index), "x") for index in range(60)))).row_count

    assert asyncio.run(drive()) == 51


def test_sst_ask_without_a_question_on_a_terminal_opens_the_conversation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    opened: list[object] = []

    def run(self: AskApp) -> Summary:
        opened.append(self)
        return Summary(["agent:jaffle_analytics_agent"], 3, 30, 2)

    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.ask._interactive", lambda: True)
    monkeypatch.setattr(AskApp, "run", run)
    project = project_copy(tmp_path)
    port = FakeSnowflake(state={})
    result = invoke_with_port(monkeypatch, port, ["ask", *common(project), "--target", "dev", "-o", "json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["data"] == {
        "agents": ["agent:jaffle_analytics_agent"],
        "turns": 3,
        "tokens": 30,
        "removed": 2,
        "transcript": None,
    }
    named = invoke_with_port(monkeypatch, port, ["ask", "jaffle_analytics_agent", *common(project), "--target", "dev"])
    assert named.exit_code == 0 and "3 turn(s), 30 tokens" in named.stdout
    assert len(opened) == 2
    monkeypatch.setattr(AskApp, "run", lambda self: None)
    none = invoke_with_port(monkeypatch, port, ["ask", *common(project), "--target", "dev"])
    assert none.exit_code == 0 and "no conversation" in none.stdout


def test_cancel_while_a_reply_runs_and_a_rebuild_that_fails_or_finds_no_agent(tmp_path: Path) -> None:
    import threading

    gate = threading.Event()

    class Slow(FakeSnowflake):
        def run(self, body: Mapping[str, object]) -> Iterator[AgentEvent]:
            yield from super().run(body) if gate.wait(5) else iter(())

    port = Slow(state={})
    port.agent_streams = {"q": [ToolResult("t", "S", "generic", "error"), TextDelta("x")]}
    backend, result = _backend(tmp_path, port)
    agents = compiled_agents(result)
    app = AskApp(backend, agents=agents, first=agents[0])

    async def steps(app: AskApp, pilot: object) -> None:
        await _until(pilot, lambda: app._session is not None and not app._busy)
        await pilot.press("ctrl+a")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.query_one("#picker").display)
        await pilot.click("#picker-filter")  # type: ignore[attr-defined]
        await pilot.press(*"nothing-matches", "enter")  # type: ignore[attr-defined]
        assert app.query_one("#picker").display and app._session is not None
        app.query_one("#picker-filter").value = ""  # type: ignore[attr-defined]
        await pilot.press("enter")  # type: ignore[attr-defined]
        await _until(pilot, lambda: not app.query_one("#picker").display)
        app.query_one("#question").value = "q"  # type: ignore[attr-defined]
        await pilot.press("enter")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app._busy)
        await pilot.press("ctrl+c")  # type: ignore[attr-defined]
        assert app.status_text == "cancelling…"
        # Ctrl-A waits for no reply; Ctrl-R waits for this one to end, then compiles again.
        await pilot.press("ctrl+a", "ctrl+r")  # type: ignore[attr-defined]
        assert not app.query_one("#picker").display
        assert app.status_text == "will compile again when this reply ends"
        gate.set()
        await _until(pilot, lambda: not app._busy and "compiled again; nothing changed" in app.status_text)
        assert app._summary.turns == 1
        assert [block.title for block in app.query(".block").results(Collapsible)] == ["result S: error"]
        app._backend = AskBackend(port, lambda: dataclasses.replace(result, compiled=()), dict, backend.scope)
        await pilot.press("ctrl+r")  # type: ignore[attr-defined]
        await _until(pilot, lambda: not app._busy and app._summary.turns == 1 and "no longer" in _notes(app))
        # The rebuild drops each copy first, then copies again: the first copy is refused.
        assert app._session is not None
        drops = [ExecResult(True, ("q",)) for _ in app._session.scratch.copies]
        port.execute_results = [*drops, failed("no CREATE")]
        app._backend = backend
        await pilot.press("ctrl+r")  # type: ignore[attr-defined]
        await _until(pilot, lambda: not app._busy and bool(app._summary.diagnostics))
        assert "runs as compiled" in app.status_text and "a copy was refused" in app.sub_title
        await pilot.press("ctrl+d")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.return_value is not None)

    summary = _run(app, steps)
    assert summary is not None and [item.code for item in summary.diagnostics] == ["SST-ASK008"]
    assert port.cancelled


def _notes(app: AskApp) -> str:
    return " ".join(str(widget.render()) for widget in app.query(".failure"))


class _Store:
    """Transcripts kept in memory; `refuse` makes every write fail."""

    def __init__(self, *, refuse: bool = False) -> None:
        self.written: dict[str, Mapping[str, object]] = {}
        self.refuse = refuse

    def write(self, name: str, document: Mapping[str, object]) -> str:
        if self.refuse:
            raise OSError("read-only")
        self.written[name] = document
        return f"/kept/{name}"


def _recorder(store: _Store) -> TranscriptRecorder:
    head = TranscriptHead("ab12", "2026-10-19T12:00:00+00:00", "1.0", "dev")
    return TranscriptRecorder(store, head, lambda: datetime(2026, 10, 19, 12, 5, tzinfo=UTC))


def test_a_kept_transcript_records_each_turn_and_spec_and_says_where_it_is(tmp_path: Path) -> None:
    port = FakeSnowflake(state={})
    port.agent_streams = {"Why?": [TextDelta("Because.")]}
    backend, result = _backend(tmp_path, port)
    store = _Store()
    backend = dataclasses.replace(backend, transcript=_recorder(store))
    agents = compiled_agents(result)
    app = AskApp(backend, agents=agents, first=agents[0], theme="nord")

    async def steps(app: AskApp, pilot: object) -> None:
        await _until(pilot, lambda: app._session is not None and not app._busy)
        assert app.theme == "nord" and len(store.written) == 1
        question = app.query_one("#question")
        for text in ("Why?", "/path"):
            question.value = text  # type: ignore[attr-defined]
            await pilot.press("enter")  # type: ignore[attr-defined]
            await _until(pilot, lambda: app._summary.turns == 1 and not app._busy)
        assert "transcript: /kept/20261019T120000Z-jaffle_analytics_agent-ab12.json" in _notices(app)
        await pilot.press("ctrl+r")  # type: ignore[attr-defined]
        await _until(pilot, lambda: not app._busy and "compiled again" in app.status_text)
        await pilot.press("ctrl+d")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.return_value is not None)

    summary = _run(app, steps)
    assert summary is not None and summary.transcript == "/kept/20261019T120000Z-jaffle_analytics_agent-ab12.json"
    assert summary.line().endswith(f"; transcript: {summary.transcript}")
    [document] = store.written.values()
    turns, revisions = document["turns"], document["spec_revisions"]
    assert isinstance(turns, list) and isinstance(revisions, list)
    assert [turn["answer"] for turn in turns] == ["Because."]
    # Compiling again changed nothing: the copies keep their names, so the spec stays revision 1.
    assert [item["revision"] for item in revisions] == [1]


def test_a_transcript_that_cannot_be_written_is_noted_once_and_the_conversation_goes_on(tmp_path: Path) -> None:
    port = FakeSnowflake(state={})
    backend, result = _backend(tmp_path, port, published=True)
    backend = dataclasses.replace(backend, transcript=_recorder(_Store(refuse=True)))
    agents = compiled_agents(result)
    app = AskApp(backend, agents=agents, first=agents[0])

    async def steps(app: AskApp, pilot: object) -> None:
        await _until(pilot, lambda: app._session is not None and not app._busy)
        assert app.theme == "sst-dark"
        app.query_one("#question").value = "q"  # type: ignore[attr-defined]
        await pilot.press("enter")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app._summary.turns == 1 and not app._busy)
        app.query_one("#question").value = "/path"  # type: ignore[attr-defined]
        await pilot.press("enter")  # type: ignore[attr-defined]
        assert "transcript: " not in _notices(app)
        await pilot.press("ctrl+d")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.return_value is not None)

    summary = _run(app, steps)
    assert summary is not None and [item.code for item in summary.diagnostics] == ["SST-ASK012"]
    assert summary.transcript is None


def test_without_a_transcript_path_says_how_to_keep_one(tmp_path: Path) -> None:
    port = FakeSnowflake(state={})
    backend, result = _backend(tmp_path, port, published=True)
    agents = compiled_agents(result)
    app = AskApp(backend, agents=agents, first=agents[0])

    async def steps(app: AskApp, pilot: object) -> None:
        await _until(pilot, lambda: app._session is not None and not app._busy)
        app.query_one("#question").value = "/path"  # type: ignore[attr-defined]
        await pilot.press("enter")  # type: ignore[attr-defined]
        assert "no transcript; --transcript keeps one" in _notices(app)
        await pilot.press("ctrl+d")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.return_value is not None)

    _run(app, steps)


def _notices(app: AskApp) -> str:
    return " ".join(str(widget.render()) for widget in app.query(".notice"))
