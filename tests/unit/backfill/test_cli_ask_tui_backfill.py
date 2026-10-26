"""The full-screen `sst ask` conversation at its edges, driven headless with Textual's Pilot."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path

from textual.widgets import OptionList

from snowflake_semantic_tools.app.ask import agent_scope, compiled_agents
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.cli.tui.ask_app import AskApp, AskBackend, Summary
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.domain.ask.events import AgentEvent, TextDelta
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from tests.helpers.project_inputs import dev_target
from tests.helpers.projects import project_paths
from tests.helpers.reference_project import DBT_MANIFEST, project_copy
from tests.helpers.snowflake_fake import FakeSnowflake, failed


def _scope(agent: object) -> SchemaScope:
    found = agent_scope(agent, dev_target())  # type: ignore[arg-type]
    assert not isinstance(found, Diagnostic)
    return found


def _compiled(tmp_path: Path) -> CompileResult:
    return compiling.compile_result(project_paths(project_copy(tmp_path)), None, DBT_MANIFEST)


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


def test_an_agent_chosen_in_the_list_itself_opens(tmp_path: Path) -> None:
    result = _compiled(tmp_path)
    backend = AskBackend(FakeSnowflake(state={}), lambda: result, dict, _scope, published=True)
    app = AskApp(backend, agents=compiled_agents(result), first=None)

    async def steps(app: AskApp, pilot: object) -> None:
        picker = app.query_one("#picker", OptionList)
        picker.focus()
        await pilot.press("enter")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app._session is not None and not app._busy)
        assert app._session is not None and app._session.agent.artifact_key == compiled_agents(result)[0].artifact_key
        await pilot.press("ctrl+d")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.return_value is not None)

    summary = _run(app, steps)
    assert summary is not None and len(summary.agents) == 1


def test_a_question_sent_while_a_reply_runs_is_dropped(tmp_path: Path) -> None:
    gate = threading.Event()

    class Slow(FakeSnowflake):
        def run(self, body: Mapping[str, object]) -> Iterator[AgentEvent]:
            yield from super().run(body) if gate.wait(5) else iter(())

    port = Slow(state={})
    port.agent_streams = {"first": [TextDelta("one")], "second": [TextDelta("two")]}
    result = _compiled(tmp_path)
    agents = compiled_agents(result)
    app = AskApp(AskBackend(port, lambda: result, dict, _scope, published=True), agents=agents, first=agents[0])

    async def steps(app: AskApp, pilot: object) -> None:
        await _until(pilot, lambda: app._session is not None and not app._busy)
        question = app.query_one("#question")
        question.value = "first"  # type: ignore[attr-defined]
        await pilot.press("enter")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app._busy)
        question.value = "second"  # type: ignore[attr-defined]
        await pilot.press("enter")  # type: ignore[attr-defined]
        gate.set()
        await _until(pilot, lambda: not app._busy and app._summary.turns == 1)
        await pilot.press("ctrl+d")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.return_value is not None)

    summary = _run(app, steps)
    assert summary is not None and summary.turns == 1
    assert len(port.stream_runs) == 1


def test_ctrl_r_while_a_rebuild_runs_does_nothing_and_a_copy_left_behind_is_noted(tmp_path: Path) -> None:
    gate = threading.Event()
    compiles: list[int] = []
    holding: list[bool] = []
    result = _compiled(tmp_path)

    def compile_again() -> CompileResult:
        if holding:
            compiles.append(1)
            gate.wait(5)
        return result

    port = FakeSnowflake(state={})
    agents = compiled_agents(result)
    app = AskApp(AskBackend(port, compile_again, dict, _scope), agents=agents, first=agents[0])

    async def steps(app: AskApp, pilot: object) -> None:
        await _until(pilot, lambda: app._session is not None and not app._busy)
        assert app._session is not None and app._session.scratch.copies
        port.execute_results = [failed("cannot drop") for _ in app._session.scratch.copies]
        holding.append(True)
        await pilot.press("ctrl+r")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app._busy)
        await pilot.press("ctrl+r")  # type: ignore[attr-defined]
        assert len(compiles) == 1
        gate.set()
        await _until(pilot, lambda: not app._busy and "compiled again" in app.status_text)
        await pilot.press("ctrl+d")  # type: ignore[attr-defined]
        await _until(pilot, lambda: app.return_value is not None)

    summary = _run(app, steps)
    assert len(compiles) == 1
    assert summary is not None and "SST-ASK009" in [item.code for item in summary.diagnostics]
