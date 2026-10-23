"""The full-screen `sst ask` conversation: pick an agent, then talk to it as it compiles now.

Textual draws it; this module is the only one that imports Textual, and `sst ask` imports it
only when a conversation opens, so no other command pays for it. Every Snowflake call runs on a
worker thread: starting a session (the sweep and the copies), each turn's stream, Ctrl-R's
rebuild, and ending the session on the way out. The screen updates from the events as they
arrive: the answer streams in, reasoning and each tool call and result sit in collapsible
blocks, and a result set shows as a table.

Keys: Enter sends; Ctrl-C cancels the turn in flight, and quits when none is; Ctrl-T shows or
hides the reasoning and tool blocks; Ctrl-A shows the picker over the conversation, where the
arrows choose, Enter opens (the agent already open goes back to it) and Esc goes back; Ctrl-R
compiles again and rebuilds the copies, keeping the thread, and says what changed (pressed
during a reply, it waits for the reply to end); Ctrl-P's palette changes the theme; Ctrl-D or
`/exit` quits; `/copy` copies the last answer, `/copy-chart` the last chart's specification,
and `/path` prints where the transcript is, when `--transcript` keeps one.
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.widgets import Collapsible, DataTable, Footer, Input, Markdown, OptionList, Static
from textual.widgets.option_list import Option

from snowflake_semantic_tools.app.ask_conversation import turn_diagnostics
from snowflake_semantic_tools.app.ask_session import (
    AskPort,
    AskSession,
    Rebuilt,
    end_session,
    rebuild,
    start_session,
)
from snowflake_semantic_tools.app.ask_transcript import TranscriptRecorder
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.cli.tui.themes import DEFAULT_THEME, THEMES
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
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.state import AppliedEntry

# Rows a result table shows before it says how many more there are.
_TABLE_ROWS = 50


class _AnswerStream:
    """Append an answer's fragments to its `Markdown` in order, one append at a time.

    `Markdown.append` is a coroutine that reparses the document's last block; two that run at
    once both reparse it, and the block then shows twice. One task drains what arrived, joined,
    awaiting each append before the next, so fragments that come faster than they draw are
    batched rather than raced. Textual's own `MarkdownStream` cancels its task on `stop()` and
    appends the rest while the canceled append may still run, which is the same race.
    """

    def __init__(self, widget: Markdown) -> None:
        self._widget = widget
        self._pending: list[str] = []
        self._task: asyncio.Task[None] | None = None

    def write(self, fragment: str) -> None:
        """Queue `fragment`, and start draining unless a drain already runs."""
        self._pending.append(fragment)
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._drain())

    async def _drain(self) -> None:
        while self._pending:
            text, self._pending = "".join(self._pending), []
            await self._widget.append(text)

    async def close(self) -> None:
        """Wait until every queued fragment shows."""
        while self._task is not None and not self._task.done():
            await self._task


class _PickerFilter(Input):
    """The picker's filter box: typing filters, and the arrow and page keys move the highlight below."""

    BINDINGS = [
        Binding("down", "move('cursor_down')", show=False),
        Binding("up", "move('cursor_up')", show=False),
        Binding("pagedown", "move('page_down')", show=False),
        Binding("pageup", "move('page_up')", show=False),
    ]

    def action_move(self, how: str) -> None:
        """Move the picker's highlight as its own key `how` would."""
        getattr(self.app.query_one("#picker", OptionList), f"action_{how}")()


@dataclass(frozen=True, slots=True)
class AskBackend:
    """What the conversation reaches Snowflake and the project through, as `sst ask` wires it.

    Attributes:
        compile: Compiles the project again, for Ctrl-R.
        applied: Reads the target's state entries; None when unreadable.
        scope: Where an agent's copies go.
        published: Copy nothing, as `--published` asks.
        transcript: Keeps the run's transcript, as `--transcript` asks; None keeps none.
    """

    port: AskPort
    compile: Callable[[], CompileResult]
    applied: Callable[[], Mapping[str, AppliedEntry] | None]
    scope: Callable[[CompiledAgent], SchemaScope]
    published: bool = False
    transcript: TranscriptRecorder | None = None


@dataclass
class Summary:
    """What the conversation did, which `sst ask` reports once the screen closes."""

    agents: list[str] = field(default_factory=list)
    turns: int = 0
    tokens: int = 0
    removed: int = 0
    diagnostics: list[Diagnostic] = field(default_factory=list)
    transcript: str | None = None

    def line(self) -> str:
        """Say it in one line: the agents, the turns, the tokens, what was removed, and the transcript."""
        agents = ", ".join(self.agents) or "no agent"
        kept = f"; transcript: {self.transcript}" if self.transcript else ""
        return f"{agents}: {self.turns} turn(s), {self.tokens} tokens; removed {self.removed} scratch object(s){kept}"

    def as_dict(self) -> dict[str, object]:
        """Return the summary as `--output json` would carry it."""
        return {
            "agents": self.agents,
            "turns": self.turns,
            "tokens": self.tokens,
            "removed": self.removed,
            "transcript": self.transcript,
        }


class AskApp(App[Summary]):
    """Pick an agent, then hold a conversation with it on scratch copies of what you edited."""

    TITLE = "sst ask"
    CSS = """
    #picker-filter { dock: top; }
    #chat { height: 1fr; }
    #status { height: 1; color: $text-muted; }
    .question { color: $accent; margin: 1 0 0 0; }
    .answer { margin: 0 0 1 0; }
    .notice { color: $warning; }
    .failure { color: $error; }
    """
    BINDINGS = [
        Binding("ctrl+c", "interrupt", "cancel / quit", priority=True),
        Binding("ctrl+d", "leave", "quit", priority=True),
        # Priority, so the question box, which binds Ctrl-A itself, does not take them first.
        Binding("ctrl+t", "toggle_blocks", "tools", priority=True),
        Binding("ctrl+a", "switch_agent", "agent", priority=True),
        Binding("ctrl+r", "recompile", "recompile", priority=True),
        Binding("escape", "picker_back", "back"),
    ]

    def __init__(
        self,
        backend: AskBackend,
        *,
        agents: tuple[CompiledAgent, ...],
        first: CompiledAgent | None,
        theme: str = DEFAULT_THEME,
    ) -> None:
        super().__init__()
        for custom in THEMES.values():
            self.register_theme(custom)
        self._theme_name = theme
        self._backend = backend
        self._agents = {agent.artifact_key: agent for agent in agents}
        self._first = first
        self._compiled: CompileResult | None = None
        self._session: AskSession | None = None
        self._busy = False
        self._replying = False
        self._recompile_queued = False
        self._refused = False
        self._blocks_shown = True
        self._summary = Summary()
        self._answer: Markdown | None = None
        self._stream: _AnswerStream | None = None
        self._last_answer = ""
        self._last_chart = ""
        self._question = ""
        self._asked_at = datetime.now(UTC)
        self._thinking_view: Static | None = None
        self._thinking_text = ""
        self._picker_shown = False
        self.status_text = ""

    def compose(self) -> ComposeResult:
        """Lay out the picker, the conversation, the status line and the input."""
        yield _PickerFilter(placeholder="type to filter agents; ↑ ↓ choose, Enter opens", id="picker-filter")
        yield OptionList(*(Option(key, id=key) for key in self._agents), id="picker")
        yield VerticalScroll(id="chat")
        yield Static("", id="status")
        yield Input(placeholder="ask a question; /exit quits", id="question", disabled=True)
        yield Footer()

    def on_mount(self) -> None:
        """Take the theme, then open the first agent, or show the picker."""
        self.theme = self._theme_name
        if self._first is not None:
            self._open(self._first)
        else:
            self._picking(True)

    def _picking(self, shown: bool) -> None:
        """Show the picker over the conversation, or hide it; a session open behind it goes on."""
        self._picker_shown = shown
        self.query_one("#picker-filter", Input).display = shown
        self.query_one("#picker", OptionList).display = shown
        self.query_one("#chat").display = not shown
        question = self.query_one("#question", Input)
        question.display = not shown
        if shown:
            picker = self.query_one("#picker-filter", Input)
            with picker.prevent(Input.Changed):
                picker.value = ""
            self._list("")
            picker.focus()
            back = " · Esc goes back" if self._session is not None else ""
            self._say(f"↑ ↓ choose · Enter opens{back}")
        elif not question.disabled:
            question.focus()
        self.refresh_bindings()

    def _list(self, wanted: str) -> None:
        """List the agents whose key holds `wanted`, case aside, the first highlighted."""
        picker = self.query_one("#picker", OptionList)
        picker.clear_options()
        folded = wanted.casefold()
        picker.add_options([Option(key, id=key) for key in self._agents if folded in key.casefold()])
        picker.highlighted = 0 if picker.option_count else None

    @on(Input.Changed, "#picker-filter")
    def _filter(self, event: Input.Changed) -> None:
        self._list(event.value)

    @on(Input.Submitted, "#picker-filter")
    def _pick_highlighted(self) -> None:
        picker = self.query_one("#picker", OptionList)
        if picker.highlighted is not None:
            self._choose(self._agents[str(picker.get_option_at_index(picker.highlighted).id)])

    @on(OptionList.OptionSelected, "#picker")
    def _picked(self, event: OptionList.OptionSelected) -> None:
        self._choose(self._agents[str(event.option.id)])

    def _choose(self, agent: CompiledAgent) -> None:
        """Open `agent`: back to the conversation when it is the one open, else end that and start anew."""
        session = self._session
        if session is not None and session.agent.artifact_key == agent.artifact_key:
            self.action_picker_back()
        elif session is not None:
            self._picking(False)
            self._end_then(lambda: self._open(agent))
        else:
            self._open(agent)

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        """Offer Esc only on a picker with a conversation behind it to go back to."""
        if action == "picker_back":
            return self._session is not None and self._picker_shown
        return True

    def action_picker_back(self) -> None:
        """Leave the picker for the conversation behind it; on the first picker, Esc does nothing."""
        if self._session is None or not self._picker_shown:
            return
        self._picking(False)
        self._say(self._ready_line())

    def _open(self, agent: CompiledAgent) -> None:
        """Start a session for `agent` on a worker: sweep, copy, and point it at the copies."""
        self._picking(False)
        self._say(f"starting {agent.artifact_key}…")
        self._busy = True
        self.run_worker(lambda: self._start(agent), thread=True, exclusive=True, group="session")

    def _start(self, agent: CompiledAgent) -> None:
        backend = self._backend
        started = start_session(
            backend.port,
            agent,
            self._compiled or backend.compile(),
            backend.applied(),
            scope=backend.scope(agent),
            session=secrets.token_hex(4),
            now=datetime.now(UTC),
            published=backend.published,
            progress=lambda line: self.call_from_thread(self._say, line),
        )
        self.call_from_thread(self._started, started)

    def _started(self, started: AskSession | Diagnostic) -> None:
        self._busy = False
        if isinstance(started, Diagnostic):
            self._summary.diagnostics.append(started)
            self._note(started.message, "failure")
            self._say("could not start; Ctrl-A picks another agent, Ctrl-D quits")
            return
        self._session = started
        self._refused = False
        self._summary.agents.append(started.agent.artifact_key)
        self._summary.removed += len(started.swept.removed)
        self._title()
        for note in started.notes:
            self._note(note.message, "notice")
        self._record(lambda kept: kept.spec(started))
        self._say(self._ready_line())
        field_ = self.query_one("#question", Input)
        field_.disabled = False
        field_.focus()
        self.refresh_bindings()

    def _ready_line(self) -> str:
        return "ready" if self._session is not None else "no agent open; Ctrl-A picks one"

    def _title(self) -> None:
        """Head the screen with the agent, what it runs on, and whether it is the published agent."""
        session = self._session
        if session is None:
            return
        if self._refused:
            copied = "nothing (a copy was refused; runs on what is published)"
        else:
            copied = ", ".join(session.edited.keys) or "nothing (no edits reach it)"
        mode = "published dependencies" if session.published else f"copies of: {copied}"
        state = f" · {session.agent_state}" if session.agent_state else ""
        self.sub_title = f"{session.agent.artifact_key} · {mode}{state}"

    @on(Input.Submitted, "#question")
    def _submitted(self, event: Input.Submitted) -> None:
        """Run a slash command, or ask the question when a session is open and no reply runs."""
        text = event.value.strip()
        event.input.value = ""
        if not text:
            return
        if text == "/exit":
            self.action_leave()
        elif text == "/copy":
            self.copy_to_clipboard(self._last_answer)
            self._say("copied the last answer")
        elif text == "/copy-chart":
            self.copy_to_clipboard(self._last_chart)
            self._say("copied the last chart's specification" if self._last_chart else "no chart yet")
        elif text == "/path":
            kept = self._backend.transcript
            where = kept.path if kept is not None else None
            self._note(f"transcript: {where}" if where else "no transcript; --transcript keeps one", "notice")
        elif self._session is not None and not self._busy:
            self._ask(text)

    def _record(self, write: Callable[[TranscriptRecorder], Diagnostic | None]) -> None:
        """Have the transcript, when one is kept, record and write; its first failure is noted."""
        kept = self._backend.transcript
        failed = write(kept) if kept is not None else None
        if failed is not None:
            self._summary.diagnostics.append(failed)
            self._note(failed.message, "notice")
        self._summary.transcript = kept.path if kept is not None else None

    def _ask(self, question: str) -> None:
        chat = self.query_one("#chat", VerticalScroll)
        chat.mount(Static(f"› {question}", classes="question"))
        self._answer = Markdown("", classes="answer")
        self._stream = _AnswerStream(self._answer)
        chat.mount(self._answer)
        self._busy = True
        self._replying = True
        self._question, self._asked_at = question, datetime.now(UTC)
        self.run_worker(lambda: self._turn(question), thread=True, exclusive=True, group="turn")

    def _turn(self, question: str) -> None:
        assert self._session is not None
        for event in self._session.conversation.ask(question):
            self.call_from_thread(self._show, event)
        self.call_from_thread(self._ended)

    def _show(self, event: AgentEvent) -> None:
        """Draw one event: stream the answer, and add a block for what else it says."""
        chat = self.query_one("#chat", VerticalScroll)
        if isinstance(event, TextDelta) and self._stream is not None:
            self._stream.write(event.text)
        elif isinstance(event, Status) and event.message:
            self._say(event.message)
        elif isinstance(event, ThinkingDelta):
            self._thinking_text += event.text
            self._thinking(chat).update(self._thinking_text)
        elif isinstance(event, ToolUse):
            chat.mount(
                self._block(f"tool {event.name} ({event.type})", Static(str(dict(event.input)))), before=self._answer
            )
        elif isinstance(event, ToolResult):
            children: list[Static | DataTable[object]] = [Static(event.sql)] if event.sql else []
            if event.table is not None:
                children.append(_table(event.table))
            title = f"result {event.name}: {event.status}{f' · {event.semantic_model}' if event.semantic_model else ''}"
            chat.mount(self._block(title, *(children or [Static(event.status)])), before=self._answer)
        elif isinstance(event, Chart):
            self._last_chart = event.spec
            self._note("vega-lite chart (/copy-chart copies it)", "notice")
        elif isinstance(event, Warning):
            self._note(event.message, "notice")
        elif isinstance(event, RunError):
            self._note(f"error {event.code}: {event.message}", "failure")
        chat.scroll_end(animate=False)

    def _thinking(self, chat: VerticalScroll) -> Static:
        """Return the current turn's reasoning text, adding its block on the turn's first reasoning."""
        if self._thinking_view is None:
            self._thinking_view = Static("")
            chat.mount(self._block("thinking", self._thinking_view), before=self._answer)
        return self._thinking_view

    def _block(self, title: str, *children: Static | DataTable[object]) -> Collapsible:
        block = Collapsible(*children, title=title, collapsed=True)
        block.add_class("block")
        block.display = self._blocks_shown
        return block

    def _ended(self) -> None:
        """End the turn once its answer has drawn in full."""
        self.call_later(self._finish_turn)

    async def _finish_turn(self) -> None:
        if self._stream is not None:
            await self._stream.close()
            self._stream = None
        self._busy = False
        self._replying = False
        session = self._session
        assert session is not None, "a turn runs only in a session, which no key ends while it runs"
        turn = session.conversation.turn
        self._thinking_view, self._thinking_text = None, ""
        self._last_answer = turn.answer.strip()
        self._summary.turns += 1
        self._summary.tokens += turn.tokens
        found = turn_diagnostics(session.agent.artifact_key, turn)
        self._summary.diagnostics.extend(found)
        for item in found:
            self._note(item.message, "failure")
        self._record(lambda kept: kept.turn(session, self._question, turn, self._asked_at))
        self._say(f"{turn.status or 'ended'} · {turn.tokens} tokens")
        if self._recompile_queued:
            self._recompile_queued = False
            self.action_recompile()

    def _say(self, text: str) -> None:
        self.status_text = text
        self.query_one("#status", Static).update(text)

    def _note(self, text: str, kind: str) -> None:
        chat = self.query_one("#chat", VerticalScroll)
        chat.mount(Static(text, classes=kind))
        chat.scroll_end(animate=False)

    def action_interrupt(self) -> None:
        """Cancel the turn in flight; with none, quit."""
        if self._busy and self._session is not None:
            self._say("canceling…")
            self.run_worker(self._session.conversation.cancel, thread=True, group="cancel")
        elif not self._busy:
            self.action_leave()

    def action_toggle_blocks(self) -> None:
        """Show or hide every reasoning and tool block."""
        self._blocks_shown = not self._blocks_shown
        for block in self.query(".block"):
            block.display = self._blocks_shown

    def action_switch_agent(self) -> None:
        """Show the picker; the conversation goes on behind it until another agent opens."""
        if self._busy or self._picker_shown:
            return
        self._picking(True)

    def action_recompile(self) -> None:
        """Compile again and rebuild the copies; the thread, and so the conversation, goes on.

        Pressed while a reply streams, it waits for the reply to end; while a session starts or
        a rebuild runs, it does nothing.
        """
        session = self._session
        if session is None:
            return
        if self._replying:
            self._recompile_queued = True
            self._say("will compile again when this reply ends")
            return
        if self._busy:
            return
        self._busy = True
        self._say("compiling again…")
        self.run_worker(lambda: self._rebuilt(session), thread=True, exclusive=True, group="session")

    def _rebuilt(self, session: AskSession) -> None:
        compiled = self._backend.compile()
        agent = next((item for item in compiled.compiled if item.artifact_key == session.agent.artifact_key), None)
        done = None
        if isinstance(agent, CompiledAgent):
            self._compiled = compiled
            done = rebuild(session, agent, compiled, self._backend.applied())
        self.call_from_thread(self._after_rebuild, done)

    def _after_rebuild(self, done: Rebuilt | None) -> None:
        """Say what compiling again did: what changed, or that nothing did, or why it could not."""
        self._busy = False
        if done is not None:
            self._refused = done.failed is not None
        self._title()
        if done is None:
            self._note("the agent no longer compiles; the conversation keeps the specification it had", "failure")
            self._say("the agent no longer compiles")
            return
        session = self._session
        for item in done.left:
            self._summary.diagnostics.append(item)
            self._note(item.message, "notice")
        if done.failed is not None:
            self._summary.diagnostics.append(done.failed)
            self._note(done.failed.message, "failure")
            self._say("a copy was refused; the agent now runs as compiled, on what is published")
        else:
            changed = ", ".join((*done.spec, *done.dependencies))
            line = f"compiled again; changed: {changed}" if done.changed else "compiled again; nothing changed"
            self._note(line, "notice")
            self._say(line)
        if session is not None:
            self._record(lambda kept: kept.spec(session, (*done.spec, *done.dependencies)))

    def action_leave(self) -> None:
        """End the session, then close with the summary, the transcript written a last time."""

        def close() -> None:
            self._record(lambda kept: kept.save())
            self.exit(self._summary)

        self._end_then(close)

    def _end_then(self, then: Callable[[], None]) -> None:
        session, self._session = self._session, None
        if session is None:
            then()
            return
        self._say("removing the copies and the thread…")
        self.query_one("#question", Input).disabled = True

        def end() -> None:
            left = end_session(session)
            self._summary.diagnostics.extend(left)
            self._summary.removed += len(session.edited.keys)
            self.call_from_thread(then)

        self.run_worker(end, thread=True, group="end")


def _table(found: ResultTable) -> DataTable[object]:
    """Lay a result set out as a table, its first rows only, saying how many more there are."""
    table: DataTable[object] = DataTable(show_cursor=False)
    table.add_columns(*found.columns)
    for row in found.rows[:_TABLE_ROWS]:
        table.add_row(*row)
    if len(found.rows) > _TABLE_ROWS:
        table.add_row(f"… {len(found.rows) - _TABLE_ROWS} more rows", *([""] * (len(found.columns) - 1)))
    return table
