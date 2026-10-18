"""Log records on stderr: at `--log-level` and above, over the progress line, credentials refused.

SST logs nothing itself; the records are the libraries' it runs, the Snowflake connector above
all, so each is printed with the logger that wrote it.
"""

from __future__ import annotations

import logging

import click

from snowflake_semantic_tools.cli.output import scrubbed
from snowflake_semantic_tools.cli.render.progress import active_display


def configure_logging(level: str) -> None:
    """Send log records at `level` and above to stderr, through progress and the credential check.

    The libraries SST runs, the Snowflake connector above all, log every request at INFO, so the
    default `warn` keeps them quiet and an explicit `info` shows them. Replaces only the handler
    this function installed, so calling it again, as each run in one process does, never stacks
    handlers or removes another's. A record is printed as `warn  snowflake.connector: message`.
    """
    root = logging.getLogger()
    for handler in [item for item in root.handlers if isinstance(item, _LogHandler)]:
        root.removeHandler(handler)
    root.addHandler(_LogHandler())
    root.setLevel(_LOG_NUMBERS[level])


_LOG_NUMBERS = {"debug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}
_LOG_PREFIX = {logging.DEBUG: "debug", logging.INFO: "info ", logging.WARNING: "warn ", logging.ERROR: "error"}


class _LogHandler(logging.Handler):
    """Print a log record on stderr above the progress line, a credential in it refused."""

    def emit(self, record: logging.LogRecord) -> None:
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
