"""Log records on stderr: at `--log-level` and above, over the progress line, credentials refused.

SST logs nothing itself; the records are the libraries' it runs, the Snowflake connector above
all, so each is printed with the logger that wrote it. Unless `--log-level` is given, a
library's warning is not printed but counted: the connector warns of what it recovers from
itself, such as a retried request, which is not the user's to act on.
"""

from __future__ import annotations

import logging

import click

from snowflake_semantic_tools.cli.output import scrubbed
from snowflake_semantic_tools.cli.render.progress import active_display


def configure_logging(level: str, *, given: bool = True) -> None:
    """Send log records at `level` and above to stderr, through progress and the credential check.

    The libraries SST runs, the Snowflake connector above all, log every request at INFO, so the
    default `warn` keeps them quiet and an explicit `info` shows them. When the level was not
    `given`, a warning is counted instead of printed, so `hidden_warnings` can say how many were
    held back; an error is always printed. Replaces only the handler this function installed,
    so calling it again, as each run in one process does, never stacks handlers or removes
    another's. A record is printed as `warn  snowflake.connector: message`.
    """
    root = logging.getLogger()
    for handler in [item for item in root.handlers if isinstance(item, _LogHandler)]:
        root.removeHandler(handler)
    root.addHandler(_LogHandler(hold_warnings=not given))
    root.setLevel(_LOG_NUMBERS[level])


def hidden_warnings() -> int:
    """Return how many library warnings this run's handler held back; 0 when it holds none back."""
    handlers = [item for item in logging.getLogger().handlers if isinstance(item, _LogHandler)]
    return sum(handler.held for handler in handlers)


_LOG_NUMBERS = {"debug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}
_LOG_PREFIX = {logging.DEBUG: "debug", logging.INFO: "info ", logging.WARNING: "warn ", logging.ERROR: "error"}


class _LogHandler(logging.Handler):
    """Print a log record on stderr above the progress line, a credential in it refused.

    With `hold_warnings`, a WARNING is counted in `held` and not printed.
    """

    def __init__(self, *, hold_warnings: bool = False) -> None:
        super().__init__()
        self.hold_warnings = hold_warnings
        self.held = 0

    def emit(self, record: logging.LogRecord) -> None:
        if self.hold_warnings and record.levelno == logging.WARNING:
            self.held += 1
            return
        try:
            prefix = _LOG_PREFIX.get(record.levelno, "error")
            text = f"{prefix} {record.name}: {record.getMessage()}"
        except Exception:  # a record whose arguments do not format must not end the run
            self.handleError(record)
            return
        text = scrubbed(text, "log output")
        display = active_display()
        if display is not None:
            display.line(text)
        else:
            click.echo(text, err=True)
