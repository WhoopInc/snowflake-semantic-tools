"""`sst ask`: talk to an agent as it compiles now, on scratch copies of your edits, publishing nothing.

With a question, one turn runs and its answer streams to the terminal as it is generated, or is
reported once it ends under `--output json`. Without one, on a terminal, a full-screen
conversation opens (`cli.tui.ask_app`), with a picker when no agent is named. Either way the
project compiles in memory; each view, tool, skill and sub-agent the agent reaches that
compiles differently from what the target's state records is copied into the agent's scratch
schema, and the agent's specification, pointed at the copies, runs over the Cortex Agents REST
API on a thread. The thread is deleted and the copies dropped when the session ends.
"""

from __future__ import annotations

import contextlib
import secrets
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.app.ask import agent_scope, compiled_agents, find_agent
from snowflake_semantic_tools.app.ask_conversation import turn_diagnostics
from snowflake_semantic_tools.app.ask_scratch import sweep
from snowflake_semantic_tools.app.ask_session import AskSession, end_session, start_session
from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import defer_target_option, target_option
from snowflake_semantic_tools.cli.output import print_progress, render_policy
from snowflake_semantic_tools.cli.render.progress import progress_paused
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.project import connect, published_entries
from snowflake_semantic_tools.domain.ask.conversation import Turn
from snowflake_semantic_tools.domain.ask.events import Status, TextDelta, ToolResult, ToolUse
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import SchemaScope, TargetIdentity

# How long one turn may run when the command line says nothing: within a background run's own
# 15-minute budget, and well past the 5 to 30 seconds the measured turns took.
DEFAULT_TURN_TIMEOUT_SECONDS = 600


@click.command(cls=SstCommand, name="ask")
@click.argument("agent_name", metavar="[AGENT]", required=False)
@click.argument("question", metavar="[QUESTION]", required=False)
@target_option()
@defer_target_option()
@click.option("--published", is_flag=True)
@click.option("--cleanup", is_flag=True)
@click.option("--show-tools", is_flag=True)
@click.option("--timeout", "timeout", type=click.IntRange(1, 900), default=DEFAULT_TURN_TIMEOUT_SECONDS)
@command_body("ask")
def ask(
    paths: ProjectPaths,
    agent_name: str | None,
    question: str | None,
    target_name: str | None,
    manifest_path: Path | None,
    published: bool,
    cleanup: bool,
    show_tools: bool,
    timeout: int,
) -> CommandResult:
    """Talk to AGENT as it compiles now, publishing nothing; with QUESTION, ask it once. `-` reads stdin.

    Connects: each view, search service, tool, skill and sub-agent the agent reaches that
    compiles differently from what the target's state records is copied into the agent's
    schema, or `agents.+scratch_schema`, as `<NAME>__SST_ASK_<id>`, and the agent's
    specification, pointed at the copies, runs over the Cortex Agents REST API, streamed, on a
    thread. The copies are dropped and the thread deleted when the session ends; a later
    session removes what one that died left, and `--cleanup` removes it now. `--published`
    copies nothing and warns of each edit the agent does not see (SST-ASK001). Takes no lock and
    writes no state.

    Without QUESTION, on a terminal, a conversation opens, with a picker when AGENT is not
    named; without a terminal, QUESTION is required.

    Exit 0 when the agent answered; 1 when the project does not compile, a copy is refused
    (SST-ASK008), a tool is unreachable (SST-ASK010) or the run fails (SST-ASK002); 3 for an
    empty question, or none without a terminal; 4 when no agent of that name compiles
    (SST-ASK003); and 5 when Snowflake is unreachable or refuses the session.
    """
    if question is None and not cleanup and not _interactive():
        raise SstUsageError("QUESTION is required without a terminal; name the agent and the question")
    compiled = compiling.compile_result(paths, target_name, manifest_path)
    if not compiled.success:
        return CommandResult(ERROR, compiled.diagnostics)
    if cleanup:
        return _cleanup(paths, target_name, compiled)
    if question is None:
        return _conversation(paths, target_name, manifest_path, compiled, agent_name, published)
    assert agent_name is not None, "click fills AGENT before QUESTION"
    asked = _question(question)
    agent = _agent(compiled, agent_name)
    return _one_shot(paths, target_name, compiled, agent, asked, published=published, show=show_tools, timeout=timeout)


def _interactive() -> bool:
    """Report whether a conversation can open: stdin and stdout are a terminal, and the output a person's."""
    policy = render_policy()
    return sys.stdin.isatty() and sys.stdout.isatty() and policy.output == "table" and not policy.agent


def _question(question: str) -> str:
    """Return the question, read from stdin when it is `-`.

    Raises:
        SstUsageError: the question is empty or only whitespace.
    """
    text = sys.stdin.read() if question == "-" else question
    if not text.strip():
        raise SstUsageError("the question is empty")
    return text.strip()


def _agent(compiled: CompileResult, name: str) -> CompiledAgent:
    """Return the compiled agent `name` names.

    Raises:
        ProjectError: no agent of that name compiles (SST-ASK003).
    """
    agent = find_agent(compiled, name)
    if isinstance(agent, Diagnostic):
        raise ProjectError(agent.message, diagnostics=(agent,))
    return agent


def _scope(agent: CompiledAgent, identity: TargetIdentity) -> SchemaScope:
    """Return where the agent's copies go.

    Raises:
        ProjectError: `agents.+scratch_schema` renders to no schema (SST-CFG008).
    """
    scope = agent_scope(agent, identity)
    if isinstance(scope, Diagnostic):
        raise ProjectError(scope.message, diagnostics=(scope,))
    return scope


def _one_shot(
    paths: ProjectPaths,
    target_name: str | None,
    compiled: CompileResult,
    agent: CompiledAgent,
    question: str,
    *,
    published: bool,
    show: bool,
    timeout: int,
) -> CommandResult:
    """Ask once: start a session, stream the turn, end the session, and report the answer."""
    profile, port = connect(paths, target_name)
    try:
        scope = _scope(agent, profile.identity)
        applied = published_entries(port, profile, paths.config_name)
        started = start_session(
            port,
            agent,
            compiled,
            applied,
            scope=scope,
            session=secrets.token_hex(4),
            now=datetime.now(UTC),
            published=published,
            progress=print_progress,
        )
        if isinstance(started, Diagnostic):
            return CommandResult(ERROR, DiagnosticBag((started,)))
        try:
            turn = _streamed(started, question, show=show, timeout=timeout)
        finally:
            left = end_session(started)
    finally:
        port.close()
    found = (*started.notes, *started.swept.diagnostics, *turn_diagnostics(agent.artifact_key, turn), *left)
    bag = DiagnosticBag(found)
    return CommandResult(
        ERROR if bag.has_errors or turn.status != "completed" else OK,
        bag,
        _data(started, question, turn),
        human=lambda: _print_answer(turn),
    )


def _streamed(session: AskSession, question: str, *, show: bool, timeout: int) -> Turn:
    """Run one turn, writing its answer to stdout as it streams when stdout is a terminal for a person.

    A turn still running after `timeout` seconds is cancelled.
    """
    streaming = _streams_to_terminal()
    timer = threading.Timer(timeout, session.conversation.cancel)
    timer.daemon = True
    timer.start()
    try:
        with progress_paused() if streaming else contextlib.nullcontext():
            for event in session.conversation.ask(question):
                if streaming and isinstance(event, TextDelta):
                    click.echo(event.text, nl=False)
                elif streaming and isinstance(event, Status) and event.message:
                    click.echo(click.style(f"\r{event.message}…\x1b[K", dim=True), nl=False, err=True)
                elif show and isinstance(event, ToolUse):
                    click.echo(f"tool_use {event.name} ({event.type})", err=True)
                elif show and isinstance(event, ToolResult):
                    click.echo(f"tool_result {event.name}: {event.status} {event.semantic_model}".rstrip(), err=True)
    finally:
        timer.cancel()
    turn = session.conversation.turn
    if streaming:
        click.echo("\r\x1b[K", nl=False, err=True)
        click.echo("")
    return turn


def _streams_to_terminal() -> bool:
    """Report whether an answer streams as it is generated: stdout is a terminal, for a person."""
    policy = render_policy()
    return sys.stdout.isatty() and policy.output == "table" and not policy.agent


def _print_answer(turn: Turn) -> None:
    """Print the answer, unless it already streamed to the terminal."""
    if _streams_to_terminal():
        return
    if turn.error is None:
        click.echo(turn.answer.strip() or "(the agent returned no text)")


def _data(session: AskSession, question: str, turn: Turn) -> dict[str, object]:
    """Project one asked turn for `--output json`: the answer, each tool, the copies, and the run."""
    return {
        "agent": session.agent.artifact_key,
        "agent_target": session.agent.target.sql,
        "scope": session.scope.sql,
        "question": question,
        "answer": turn.answer.strip(),
        "status": turn.status,
        "run_id": turn.run_id,
        "tokens": turn.tokens,
        "copied": list(session.edited.keys),
        "published": session.published,
        "tool_uses": [{"name": use.name, "type": use.type, "input": dict(use.input)} for use in turn.tool_uses],
        "tool_results": [
            {"name": result.name, "status": result.status, "sql": result.sql, "semantic_view": result.semantic_model}
            for result in turn.tool_results
        ],
        "error": turn.error.message if turn.error else None,
    }


def _cleanup(paths: ProjectPaths, target_name: str | None, compiled: CompileResult) -> CommandResult:
    """Remove every scratch copy and thread earlier sessions left, in every agent's scratch schema."""
    agents = compiled_agents(compiled)
    profile, port = connect(paths, target_name)
    try:
        scopes = tuple(dict.fromkeys(_scope(agent, profile.identity) for agent in agents))
        swept = sweep(port, port, scopes, now=datetime.now(UTC), ttl=timedelta(0))
    finally:
        port.close()
    return CommandResult(
        ERROR if DiagnosticBag(swept.diagnostics).has_errors else OK,
        DiagnosticBag(swept.diagnostics),
        {"removed": list(swept.removed), "count": len(swept.removed), "schemas": [scope.sql for scope in scopes]},
        human=lambda: click.echo("\n".join(swept.removed) if swept.removed else "nothing was left behind"),
        headline=f"removed {len(swept.removed)} left behind",
    )


def _conversation(
    paths: ProjectPaths,
    target_name: str | None,
    manifest_path: Path | None,
    compiled: CompileResult,
    agent_name: str | None,
    published: bool,
) -> CommandResult:
    """Open the full-screen conversation; its summary is what the command reports once it closes."""
    agents = compiled_agents(compiled)
    if not agents:
        raise SstUsageError("no agent compiles in this project")
    first = _agent(compiled, agent_name) if agent_name is not None else None
    from snowflake_semantic_tools.cli.tui.ask_app import AskApp, AskBackend

    profile, port = connect(paths, target_name)
    try:
        backend = AskBackend(
            port=port,
            compile=lambda: compiling.compile_result(paths, target_name, manifest_path),
            applied=lambda: published_entries(port, profile, paths.config_name),
            scope=lambda agent: _scope(agent, profile.identity),
            published=published,
        )
        with progress_paused():
            summary = AskApp(backend, agents=agents, first=first).run()
    finally:
        port.close()
    found = DiagnosticBag(summary.diagnostics if summary else ())
    data = summary.as_dict() if summary else {"turns": 0}
    return CommandResult(
        ERROR if found.has_errors else OK,
        found,
        data,
        human=lambda: click.echo(summary.line() if summary else "no conversation"),
    )
