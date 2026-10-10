"""Ask the user to confirm, only where they can see the question and answer it.

Every question `sst` asks goes to stderr, which keeps stdout for the command's own output, and
is asked only when stdin and stderr are both terminals. A question written into a redirected
file, or answered from a pipe, would wait for an answer that never comes: off a terminal a
command that would ask refuses at once with SST-PRT109, naming `--yes`, or, as `sst init` does,
takes its default.
"""

from __future__ import annotations

import sys
from typing import IO, Any

import click

from snowflake_semantic_tools.cli.group import SstUsageError
from snowflake_semantic_tools.domain.diagnostics import D

_NO_TERMINAL = "there is no terminal to ask on: stdin and stderr must both be terminals"


def can_ask() -> bool:
    """Report whether a question on stderr reaches someone who can answer it on stdin."""
    return _is_terminal(sys.stdin) and _is_terminal(sys.stderr)


def require_answerable(command: str) -> None:
    """Refuse `command` at once when it would have to ask for confirmation and cannot.

    Raises:
        SstUsageError: stdin or stderr is not a terminal; it carries SST-PRT109.

    Diagnostics:
        SST-PRT109: there is no terminal to ask on, so the run needs --yes; raised.
    """
    if not can_ask():
        diagnostic = D("SST-PRT109", subject="cli", command=command)
        raise SstUsageError(_NO_TERMINAL, diagnostic=diagnostic)


def confirm(question: str) -> None:
    """Ask `question` on stderr; a declined or unanswered question aborts the run, which exits 130.

    Call `require_answerable` first: this asks whether or not anyone can see the question.

    Raises:
        click.exceptions.Abort: the user declined, or input ended.
    """
    click.confirm(question, abort=True, err=True)


def ask(question: str, default: str) -> str:
    """Ask `question` on stderr, offering `default`; call it only when `can_ask` is true.

    Raises:
        click.exceptions.Abort: input ended.
    """
    return str(click.prompt(question, default=default, err=True))


def _is_terminal(stream: IO[Any] | None) -> bool:
    """Report whether `stream` is a terminal; a closed or absent stream is not."""
    try:
        return stream is not None and stream.isatty()
    except (ValueError, OSError):
        return False
