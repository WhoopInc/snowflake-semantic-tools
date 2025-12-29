#!/usr/bin/env python3
"""
Logging Utilities

Professional logging infrastructure integrated with event system.
Provides structured logging for debugging alongside clean CLI output via events.

**Log Files:**
- `logs/sst.log` - Technical/debug logs (parser, database, etc.)
- `logs/sst_events.log` - Structured event logs (user operations)

Usage:
    # For internal debugging/technical logs
    logger = get_logger(__name__)
    logger.debug("Technical detail")  # → logs/sst.log

    # For user-facing output
    from snowflake_semantic_tools.shared.events import fire_event, ModelEnriched
    fire_event(ModelEnriched(...))  # → CLI + logs/sst_events.log
"""

import logging
import threading
from typing import Optional

_global_max_width = 32


class DynamicFormatter(logging.Formatter):
    """Custom formatter that dynamically adjusts module name width for perfect alignment."""

    def __init__(self):
        super().__init__()

    def format(self, record):
        """Format log record with dynamic module name alignment."""
        global _global_max_width

        # Update global max width if this module name is longer
        module_length = len(record.name)
        if module_length > _global_max_width:
            _global_max_width = module_length

        # Use the global max width for consistent alignment across all loggers
        width = _global_max_width

        # Build format string with consistent width
        format_str = f"%(asctime)s | %(levelname)-8s | %(name)-{width}s | %(message)s"

        # Apply the format
        formatter = logging.Formatter(format_str, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module.

    Args:
        name: Logger name (typically module name)

    Returns:
        Configured logger instance
    """
    # Extract module name (remove package prefix for cleaner output)
    display_name = name.split(".")[-1] if "." in name else name
    return logging.getLogger(display_name)


# Setup logging with dynamic formatter
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

# Clear any existing handlers
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Console handler - ONLY for CRITICAL errors (events handle user output)
# This keeps terminal clean and consistent
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.CRITICAL)  # Only critical errors to console
console_handler.setFormatter(DynamicFormatter())
root_logger.addHandler(console_handler)

# File handler - Technical/debug logs go here (excludes events)
try:
    import os

    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)

    file_handler = logging.FileHandler(os.path.join(log_dir, "sst.log"))
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(DynamicFormatter())
    root_logger.addHandler(file_handler)

    # Configure event logger to write to separate file with simpler format
    event_logger = logging.getLogger("sst.events")
    # Only configure if not already configured (avoid duplicate handlers)
    if not event_logger.handlers:
        event_file = logging.FileHandler(os.path.join(log_dir, "sst_events.log"))
        event_file.setLevel(logging.DEBUG)
        # Simple format for events (they already format themselves)
        event_file.setFormatter(logging.Formatter("%(message)s"))
        event_logger.addHandler(event_file)
        event_logger.propagate = False  # Don't also log to root logger
except Exception:
    # If we can't create log file, that's okay - events still work
    pass

# Silence noisy third-party loggers
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
logging.getLogger("git").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# Suppress pandas SQLAlchemy warnings
import warnings

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")
