"""`sst ask`: ask an agent a question as it compiles now, before anything about it is published.

The project compiles in memory, and the named agent's rendered specification -- what
`sst compile --emit-agent-spec` writes -- runs as a temporary agent on one session, which is
dropped once it answers. No lock is taken and nothing is written, in Snowflake or on disk.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.app.ask import (
    DEFAULT_RUN_TIMEOUT_SECONDS,
    AskAgent,
    AskOutcome,
    agent_scope,
    find_agent,
    reply_json,
    stale_dependencies,
)
from snowflake_semantic_tools.app.state import table_state
from snowflake_semantic_tools.cli.exit_codes import ERROR, OK
from snowflake_semantic_tools.cli.globals import SstCommand
from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.cli.options import defer_target_option, target_option
from snowflake_semantic_tools.cli.runner import CommandResult, command_body
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.cli.wiring.project import connect
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.state import AppliedEntry


@click.command(cls=SstCommand, name="ask")
@click.argument("agent_name", metavar="AGENT")
@click.argument("question", metavar="QUESTION")
@target_option()
@defer_target_option()
@click.option("--show-tools", is_flag=True)
@click.option("--timeout", "timeout", type=click.IntRange(1, 3600), default=DEFAULT_RUN_TIMEOUT_SECONDS)
@command_body("ask")
def ask(
    paths: ProjectPaths,
    agent_name: str,
    question: str,
    target_name: str | None,
    manifest_path: Path | None,
    show_tools: bool,
    timeout: int,
) -> CommandResult:
    """Ask AGENT one QUESTION as it compiles now, publishing nothing; `-` reads the question from stdin.

    Connects: the agent's compiled specification runs as a temporary agent in its own schema,
    or in `agents.+scratch_schema`, and is dropped once it answers. Every view, search service
    and tool it names is the published one, so each that compiles differently from what the
    target's state records is a warning (SST-ASK001): the answer does not reflect that edit.
    Takes no lock and writes no state.

    Exit 0 when the agent answered; 1 when the project does not compile or the agent answered
    with an error (SST-ASK002); 3 for an empty question; 4 when no agent of that name compiles
    (SST-ASK003); and 5 when Snowflake is unreachable, refuses a privilege, or the run times out.
    """
    asked = _question(question)
    compiled = compiling.compile_result(paths, target_name, manifest_path)
    if not compiled.success:
        return CommandResult(ERROR, compiled.diagnostics)
    agent = find_agent(compiled, agent_name)
    if isinstance(agent, Diagnostic):
        raise ProjectError(agent.message, diagnostics=(agent,))
    profile, port = connect(paths, target_name)
    try:
        scope = agent_scope(agent, profile.identity)
        if isinstance(scope, Diagnostic):
            raise ProjectError(scope.message, diagnostics=(scope,))
        applied = _applied(port, profile.state_table, profile.identity, paths.config_name)
        outcome = AskAgent(port).run(
            agent,
            asked,
            scope=scope,
            timeout=timeout,
            notes=stale_dependencies(agent, compiled, applied),
        )
    finally:
        port.close()
    return CommandResult(
        OK if outcome.answered else ERROR,
        outcome.diagnostics,
        _data(outcome),
        human=lambda: _print_reply(outcome, show_tools=show_tools),
    )


def _question(question: str) -> str:
    """Return the question, read from stdin when it is `-`.

    Raises:
        SstUsageError: the question is empty or only whitespace.
    """
    text = sys.stdin.read() if question == "-" else question
    if not text.strip():
        raise SstUsageError("the question is empty")
    return text.strip()


def _applied(
    port: StatePort, state_table: QualifiedName, target: TargetIdentity, config_path: str
) -> dict[str, AppliedEntry] | None:
    """Read the target's state entries from the state table, never the local cache; None when unreadable.

    The read is only a check, so a failure to make it does not stop the question; it is
    reported, with every dependency, as SST-ASK001.
    """
    try:
        state = table_state(port, state_table, target, config_path)
    except SnowflakePortError:
        return None
    return None if state is None else dict(state.applied)


def _data(outcome: AskOutcome) -> dict[str, object]:
    return {
        "agent": outcome.agent.artifact_key,
        "agent_target": outcome.agent.target.sql,
        "scope": outcome.scope.sql,
        "question": outcome.question,
        **reply_json(outcome.reply),
    }


def _print_reply(outcome: AskOutcome, *, show_tools: bool) -> None:
    """Print the agent's answer; with `show_tools`, each tool call and its result first, in reply order."""
    reply = outcome.reply
    if show_tools:
        for use in reply.tool_uses:
            click.echo(f"tool_use {use.name} ({use.type}): {json.dumps(dict(use.input), sort_keys=True)}")
        for result in reply.tool_results:
            click.echo(f"tool_result {result.name}: {result.status}")
    if reply.error is None:
        click.echo(reply.answer or "(the agent returned no text)")
