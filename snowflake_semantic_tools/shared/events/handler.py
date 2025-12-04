"""
Event Handler

Routes events to appropriate outputs (CLI and structured logs via Python logging).
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

import click

from .types import BaseEvent, EventLevel


class EventHandler:
    """Handle events and route to CLI output and structured logs."""

    def __init__(
        self,
        show_cli_output: bool = True,
        verbose: bool = False,
        quiet: bool = False,
        log_format: str = "text",
        show_timestamps: bool = True,
    ):
        """Initialize event handler with output configuration."""
        self.show_cli_output = show_cli_output
        self.verbose = verbose
        self.quiet = quiet
        self.log_format = log_format
        self.show_timestamps = show_timestamps
        self.logger = logging.getLogger("sst.events")

    def fire_event(self, event: BaseEvent) -> None:
        """Process and route an event to CLI and structured logs."""
        # 1. CLI Output (user-facing, clean)
        if self.show_cli_output:
            self._render_cli(event)

        # 2. Structured Logging (always logs, for debugging/analysis)
        self._log_structured(event)

    def _should_show_in_cli(self, level: EventLevel) -> bool:
        """Check if event should be displayed based on quiet/verbose settings."""
        # In quiet mode, only show errors
        if self.quiet:
            return level == EventLevel.ERROR

        # Skip DEBUG events unless verbose mode
        if level == EventLevel.DEBUG:
            return self.verbose

        return True

    def _render_cli(self, event: BaseEvent) -> None:
        """Render event to CLI with colors and timestamps."""
        level = event.get_level()

        # Check if we should show this event
        if not self._should_show_in_cli(level):
            return

        message = event.message()

        # Add timestamp if enabled
        if self.show_timestamps:
            timestamp = datetime.now().strftime("%H:%M:%S")
            message = f"{timestamp}  {message}"

        # Color coding - only color the status brackets, not entire line
        if level == EventLevel.ERROR:
            # For errors, color the whole line red
            click.secho(message, fg="red", err=True)
        elif level == EventLevel.WARNING:
            # For warnings, color the whole line yellow
            click.secho(message, fg="yellow")
        elif level == EventLevel.DEBUG:
            # Plain text for debug (readable)
            click.echo(message)
        elif "[CREATED" in message or "[GENERATED" in message or "[OK in" in message:
            # Only color the status bracket, not the whole line
            import re

            # Match patterns like [CREATED in 13.2s] or [OK in 2.3s]
            colored_message = re.sub(
                r"\[(CREATED|GENERATED|OK)([^\]]*)\]",
                lambda m: click.style(f"[{m.group(1)}{m.group(2)}]", fg="green"),
                message,
            )
            click.echo(colored_message)
        else:  # INFO
            click.echo(message)

    def _log_structured(self, event: BaseEvent) -> None:
        """Log event to structured logs (always, regardless of CLI settings)."""
        level = event.get_level()
        log_data = event.log_dict().copy()

        # Add timestamp and level to structured logs
        log_data["timestamp"] = datetime.now().isoformat()
        log_data["level"] = level.value

        # Format log message
        log_message = self._format_log_message(log_data)

        # Route to appropriate logger level
        log_level_map = {
            EventLevel.DEBUG: self.logger.debug,
            EventLevel.INFO: self.logger.info,
            EventLevel.WARNING: self.logger.warning,
            EventLevel.ERROR: self.logger.error,
        }
        log_method = log_level_map.get(level, self.logger.info)
        log_method(log_message, extra=log_data)

    def _format_log_message(self, log_data: Dict[str, Any]) -> str:
        """Format log message as JSON or human-readable text."""
        if self.log_format == "json":
            return json.dumps(log_data)

        # Human-readable structured format
        event_name = log_data.get("event", "unknown")
        excluded_keys = {"event", "timestamp", "level"}
        details = [f"{k}={v}" for k, v in log_data.items() if k not in excluded_keys]

        return f"{event_name}: {', '.join(details)}" if details else event_name


# Global event handler instance
_event_handler: Optional[EventHandler] = None


def setup_events(
    verbose: bool = False,
    quiet: bool = False,
    log_format: str = "text",
    show_cli_output: bool = True,
    show_timestamps: bool = True,
) -> None:
    """
    Initialize global event handler.

    Called at the start of each CLI command to configure event output.

    Args:
        verbose: Show DEBUG level events in CLI
        quiet: Only show ERROR level events (suppresses INFO/WARNING/DEBUG)
        log_format: Format for structured logs ("text" or "json")
        show_cli_output: Enable user-facing CLI output
        show_timestamps: Show timestamps in CLI output
    """
    global _event_handler
    _event_handler = EventHandler(
        show_cli_output=show_cli_output,
        verbose=verbose,
        quiet=quiet,
        log_format=log_format,
        show_timestamps=show_timestamps,
    )


def fire_event(event: BaseEvent) -> None:
    """
    Fire an event through the global handler.

    This is the main entry point for emitting events throughout the codebase.
    If the handler hasn't been setup yet (via setup_events()), it will be
    auto-initialized with default settings.

    Args:
        event: Event to fire
    """
    global _event_handler
    if _event_handler is None:
        # Auto-initialize with defaults if not setup
        # This allows events to work even if setup_events() wasn't called
        setup_events()
    _event_handler.fire_event(event)
