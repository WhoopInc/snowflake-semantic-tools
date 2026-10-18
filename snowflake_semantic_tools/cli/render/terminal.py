"""What the terminal a run writes to can show: colour, Unicode, its width, live redraws, and links.

`detect_terminal` decides once per run, from the environment and the two streams, so every
renderer agrees and a test can drive it with a fake environment. `agent_marker` says which
coding agent, if any, is running `sst`, for the default output format.
"""

from __future__ import annotations

import dataclasses
import os
import shutil
import sys
from collections.abc import Mapping
from typing import TextIO

# Environment variables a coding agent sets in the shells it runs; `SST_AGENT=1` names any other.
AGENT_MARKERS = ("SST_AGENT", "CLAUDECODE", "COCO_AGENT", "CURSOR_AGENT", "CODEX_SANDBOX", "GEMINI_CLI")
# Variables that force colour on where no terminal is attached, as CI log viewers render ANSI.
FORCE_COLOR_VARIABLES = ("FORCE_COLOR", "CLICOLOR_FORCE")
# Terminals that render OSC 8 hyperlinks, by `$TERM_PROGRAM`.
_HYPERLINK_PROGRAMS = frozenset(("iTerm.app", "WezTerm", "vscode", "ghostty"))
COLOR_CHOICES = ("auto", "always", "never")
DEFAULT_WIDTH = 100
MIN_WIDTH = 60


@dataclasses.dataclass(frozen=True)
class Terminal:
    """What one run may draw on stderr, where diagnostics, progress and the verdict go.

    Attributes:
        color: ANSI colour on stderr.
        color_out: ANSI colour on stdout, where a command's report (a plan, a table) goes.
        unicode: Symbols such as `✓` and `→`; ASCII stand-ins when the stream cannot encode them.
        width: Columns to fit to, at least `MIN_WIDTH`.
        live: stderr is a terminal, so a progress line may be redrawn in place.
        hyperlinks: OSC 8 links render; only where colour is on and the terminal is known to.
    """

    color: bool = False
    color_out: bool = False
    unicode: bool = True
    width: int = DEFAULT_WIDTH
    live: bool = False
    hyperlinks: bool = False


PLAIN = Terminal()


def detect_terminal(
    environ: Mapping[str, str],
    stdout: TextIO,
    stderr: TextIO,
    *,
    choice: str = "auto",
    colorable: bool = True,
) -> Terminal:
    """Decide what this run's terminal can show.

    Colour follows `choice` (`--color`): `never` and `always` decide alone; `auto` is off under a
    non-empty `$NO_COLOR` or `TERM=dumb`, on under a non-empty `$FORCE_COLOR` or `$CLICOLOR_FORCE`
    other than `0`, and otherwise on for a stream that is a terminal.

    Args:
        colorable: False for an output format that is never coloured (`plain`, `agent`, `csv`).
    """
    err_tty, out_tty = _isatty(stderr), _isatty(stdout)
    color = colorable and _wants_color(environ, choice, err_tty)
    color_out = colorable and _wants_color(environ, choice, out_tty)
    encoding = (getattr(stderr, "encoding", None) or "").lower().replace("-", "")
    return Terminal(
        color=color,
        color_out=color_out,
        unicode=encoding.startswith("utf"),
        width=_width(environ),
        live=err_tty and environ.get("TERM") != "dumb",
        hyperlinks=color and err_tty and _links(environ),
    )


def agent_marker(environ: Mapping[str, str], stdout: TextIO) -> str | None:
    """Return the agent variable that makes `agent` this run's default output, else None.

    Only a run whose stdout is not a terminal counts: a person typing in an agent's terminal
    pane still sees `table`. A marker set to `0` or empty is not set.
    """
    if _isatty(stdout):
        return None
    return next((name for name in AGENT_MARKERS if environ.get(name, "") not in ("", "0")), None)


def system_terminal(*, choice: str = "auto", colorable: bool = True) -> Terminal:
    """Detect the terminal of this process, from its environment and standard streams."""
    return detect_terminal(os.environ, sys.stdout, sys.stderr, choice=choice, colorable=colorable)


def _wants_color(environ: Mapping[str, str], choice: str, tty: bool) -> bool:
    if choice in ("always", "never"):
        return choice == "always"
    if environ.get("NO_COLOR") or environ.get("TERM") == "dumb":
        return False
    if any(environ.get(name, "") not in ("", "0") for name in FORCE_COLOR_VARIABLES):
        return True
    return tty


def _width(environ: Mapping[str, str]) -> int:
    columns = environ.get("COLUMNS", "")
    if columns.isdigit():
        return max(int(columns), MIN_WIDTH)
    return max(shutil.get_terminal_size((DEFAULT_WIDTH, 24)).columns, MIN_WIDTH)


def _links(environ: Mapping[str, str]) -> bool:
    if environ.get("TMUX"):
        return False
    return environ.get("TERM_PROGRAM", "") in _HYPERLINK_PROGRAMS or "kitty" in environ.get("TERM", "")


def _isatty(stream: TextIO) -> bool:
    isatty = getattr(stream, "isatty", None)
    try:
        return bool(isatty()) if callable(isatty) else False
    except (OSError, ValueError):
        return False
