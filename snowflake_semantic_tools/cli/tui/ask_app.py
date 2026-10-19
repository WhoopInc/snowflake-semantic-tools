"""The full-screen `sst ask` conversation: pick an agent, then talk to it as it compiles now.

Textual draws it; this module is the only one that imports Textual, and `sst ask` imports it
only when a conversation opens, so no other command pays for it. Every Snowflake call runs on a
worker thread: starting a session (the sweep and the copies), each turn's stream, Ctrl-R's
rebuild, and ending the session on the way out. The screen updates from the events as they
arrive: the answer streams in, reasoning and each tool call and result sit in collapsible
blocks, and a result set shows as a table.

Keys: Enter sends; Ctrl-C cancels the turn in flight, and quits when none is; Ctrl-T shows or
hides the reasoning and tool blocks; Ctrl-A picks another agent (a new thread and new copies);
Ctrl-R compiles again and rebuilds the copies, keeping the thread; Ctrl-D or `/exit` quits;
`/copy` copies the last answer, `/copy-chart` the last chart's specification.
"""

from __future__ import annotations

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
from snowflake_semantic_tools.app.ask_session import AskPort, AskSession, end_session, rebuild, start_session
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
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


@dataclass(frozen=True, slots=True)
class AskBackend:
    """What the conversation reaches Snowflake and the project through, as `sst ask` wires it.

    Attributes:
        compile: Compiles the project again, for Ctrl-R.
        applied: Reads the target's state entries; None when unreadable.
        scope: Where an agent's copies go.
        published: Copy nothing, as `--published` asks.
    """

    port: AskPort
    compile: Callable[[], CompileResult]
    applied: Callable[[], Mapping[str, AppliedEntry] | None]
    scope: Callable[[CompiledAgent], SchemaScope]
    published: bool = False


@dataclass
class Summary:
    """What the conversation did, which `sst ask` reports once the screen closes."""

    agents: list[str] = field(default_factory=list)
    turns: int = 0
    tokens: int = 0
    removed: int = 0
    diagnostics: list[Diagnostic] = field(default_factory=list)

    def line(self) -> str:
        """Say it in one line: the agents, the turns, the tokens, and what was removed on the way out."""
        agents = ", ".join(self.agents) or "no agent"
        return f"{agents}: {self.turns} turn(s), {self.tokens} tokens; removed {self.removed} scratch object(s)"

    def as_dict(self) -> dict[str, object]:
        """Return the summary as `--output json` would carry it."""
        return {"agents": self.agents, "turns": self.turns, "tokens": self.tokens, "removed": self.removed}


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
    ]

    def __init__(self, backend: AskBackend, *, agents: tuple[CompiledAgent, ...], first: CompiledAgent | None) -> None:
        super().__init__()
        self._backend = backend
        self._agents = {agent.artifact_key: agent for agent in agents}
        self._first = first
        self._compiled: CompileResult | None = None
        self._session: AskSession | None = None
        self._busy = False
        self._blocks_shown = True
        self._summary = Summary()
        self._answer: Markdown | None = None
        self._last_answer = ""
        self._last_chart = ""
        self._thinking_view: Static | None = None
        self._thinking_text = ""
        self.status_text = ""

    def compose(self) -> ComposeResult:
        """Lay out the picker, the conversation, the status line and the input."""
        yield Input(placeholder="type to filter agents", id="picker-filter")
        yield OptionList(*(Option(key, id=key) for key in self._agents), id="picker")
        yield VerticalScroll(id="chat")
        yield Static("", id="status")
        yield Input(placeholder="ask a question; /exit quits", id="question", disabled=True)
        yield Footer()

    def on_mount(self) -> None:
        """Open the first agent, or show the picker."""
        if self._first is not None:
            self._open(self._first)
        else:
            self._picking(True)

    def _picking(self, shown: bool) -> None:
        self.query_one("#picker-filter", Input).display = shown
        self.query_one("#picker", OptionList).display = shown
        self.query_one("#chat").display = not shown
        if shown:
            picker = self.query_one("#picker-filter", Input)
            picker.value = ""
            picker.focus()

    @on(Input.Changed, "#picker-filter")
    def _filter(self, event: Input.Changed) -> None:
        picker = self.query_one("#picker", OptionList)
        picker.clear_options()
        wanted = event.value.casefold()
        picker.add_options([Option(key, id=key) for key in self._agents if wanted in key.casefold()])

    @on(Input.Submitted, "#picker-filter")
    def _pick_first(self) -> None:
        picker = self.query_one("#picker", OptionList)
        if picker.option_count:
            self._open(self._agents[str(picker.get_option_at_index(0).id)])

    @on(OptionList.OptionSelected, "#picker")
    def _picked(self, event: OptionList.OptionSelected) -> None:
        self._open(self._agents[str(event.option.id)])

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
        self._summary.agents.append(started.agent.artifact_key)
        self._summary.removed += len(started.swept.removed)
        copied = ", ".join(started.edited.keys) or "nothing (no edits reach it)"
        mode = "published dependencies" if started.published else f"copies of: {copied}"
        self.sub_title = f"{started.agent.artifact_key} · {mode}"
        for note in started.notes:
            self._note(note.message, "notice")
        self._say("ready")
        field_ = self.query_one("#question", Input)
        field_.disabled = False
        field_.focus()

    @on(Input.Submitted, "#question")
    def _submitted(self, event: Input.Submitted) -> None:
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
        elif self._session is not None and not self._busy:
            self._ask(text)

    def _ask(self, question: str) -> None:
        chat = self.query_one("#chat", VerticalScroll)
        chat.mount(Static(f"› {question}", classes="question"))
        self._answer = Markdown("", classes="answer")
        chat.mount(self._answer)
        self._busy = True
        self.run_worker(lambda: self._turn(question), thread=True, exclusive=True, group="turn")

    def _turn(self, question: str) -> None:
        assert self._session is not None
        for event in self._session.conversation.ask(question):
            self.call_from_thread(self._show, event)
        self.call_from_thread(self._ended)

    def _show(self, event: AgentEvent) -> None:
        """Draw one event: stream the answer, and add a block for what else it says."""
        chat = self.query_one("#chat", VerticalScroll)
        if isinstance(event, TextDelta) and self._answer is not None:
            self._answer.append(event.text)
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
        self._busy = False
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
        self._say(f"{turn.status or 'ended'} · {turn.tokens} tokens")

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
            self._say("cancelling…")
            self.run_worker(self._session.conversation.cancel, thread=True, group="cancel")
        elif not self._busy:
            self.action_leave()

    def action_toggle_blocks(self) -> None:
        """Show or hide every reasoning and tool block."""
        self._blocks_shown = not self._blocks_shown
        for block in self.query(".block"):
            block.display = self._blocks_shown

    def action_switch_agent(self) -> None:
        """End this session, then pick another agent."""
        if self._busy:
            return
        self._end_then(lambda: self._picking(True))

    def action_recompile(self) -> None:
        """Compile again and rebuild the copies; the thread, and so the conversation, goes on."""
        session = self._session
        if session is None or self._busy:
            return
        self._busy = True
        self._say("compiling again…")
        self.run_worker(lambda: self._rebuilt(session), thread=True, exclusive=True, group="session")

    def _rebuilt(self, session: AskSession) -> None:
        compiled = self._backend.compile()
        agent = next((item for item in compiled.compiled if item.artifact_key == session.agent.artifact_key), None)
        failed = None
        if isinstance(agent, CompiledAgent):
            self._compiled = compiled
            failed = rebuild(session, agent, compiled, self._backend.applied())
        self.call_from_thread(self._after_rebuild, failed, agent is not None)

    def _after_rebuild(self, failed: Diagnostic | None, found: bool) -> None:
        self._busy = False
        if failed is not None:
            self._summary.diagnostics.append(failed)
            self._note(failed.message, "failure")
        elif not found:
            self._note("the agent no longer compiles", "failure")
        else:
            copied = ", ".join(self._session.edited.keys) if self._session else ""
            self._say(f"compiled again; copies of: {copied or 'nothing'}")

    def action_leave(self) -> None:
        """End the session, then close with the summary."""
        self._end_then(lambda: self.exit(self._summary))

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
