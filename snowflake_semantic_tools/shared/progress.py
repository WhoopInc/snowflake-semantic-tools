"""
Progress Reporting Framework

Provides a clean, testable way for services to report progress to CLI
without tight coupling. Services remain testable while CLI gets rich
progress information.

Design Principles:
- Services don't depend on CLI (clean separation)
- Progress callbacks are optional (default to no-op)
- Type-safe with protocols
- Easy to test
- Supports both simple messages and structured progress

Usage in Services:
    def execute(self, config, progress_callback=None):
        progress = progress_callback or ProgressCallback()
        progress.info("Starting extraction...")
        progress.stage("Parsing YAML files")
        progress.detail(f"Found {count} files")

Usage in CLI:
    output = CLIOutput(verbose=verbose)
    callback = CLIProgressCallback(output)
    service.execute(config, progress_callback=callback)
"""

from dataclasses import dataclass
from typing import Callable, Optional, Protocol


class ProgressCallback(Protocol):
    """
    Protocol for progress reporting callbacks.

    Services use this interface to report progress without depending
    on specific CLI implementation. CLI provides concrete implementation.
    """

    def info(self, message: str, indent: int = 0) -> None:
        """Report informational message."""
        ...

    def stage(self, stage_name: str, current: Optional[int] = None, total: Optional[int] = None) -> None:
        """Report start of a new stage with optional numbering."""
        ...

    def detail(self, message: str, indent: int = 1) -> None:
        """Report detailed progress (respects verbose mode)."""
        ...

    def item_progress(
        self, current: int, total: int, item_name: str, status: str = "RUN", duration: Optional[float] = None
    ) -> None:
        """Report progress on individual items."""
        ...

    def blank_line(self) -> None:
        """Insert a blank line for visual separation."""
        ...

    def warning(self, message: str) -> None:
        """Report warning."""
        ...

    def error(self, message: str) -> None:
        """Report error."""
        ...


class NoOpProgressCallback:
    """
    Default no-op implementation for when no progress reporting is needed.

    Used by default in services and tests to avoid None checks.
    All methods are no-ops.
    """

    def info(self, message: str, indent: int = 0) -> None:
        pass

    def stage(self, stage_name: str, current: Optional[int] = None, total: Optional[int] = None) -> None:
        pass

    def detail(self, message: str, indent: int = 1) -> None:
        pass

    def item_progress(
        self, current: int, total: int, item_name: str, status: str = "RUN", duration: Optional[float] = None
    ) -> None:
        pass

    def blank_line(self) -> None:
        pass

    def warning(self, message: str) -> None:
        pass

    def error(self, message: str) -> None:
        pass


class CLIProgressCallback:
    """
    CLI implementation of progress callback using CLIOutput.

    Bridges the service layer and CLI layer, translating service
    progress reports into user-friendly CLI output.
    """

    def __init__(self, cli_output: "CLIOutput"):  # type: ignore
        """
        Initialize with CLIOutput instance.

        Args:
            cli_output: CLIOutput instance for formatting
        """
        self.output = cli_output

    def info(self, message: str, indent: int = 0) -> None:
        """Report informational message."""
        self.output.info(message, indent=indent)

    def stage(self, stage_name: str, current: Optional[int] = None, total: Optional[int] = None) -> None:
        """Report start of a new stage."""
        if current and total:
            self.output.blank_line()
            self.output.info(f"Step {current}/{total}: {stage_name}")
        else:
            self.output.blank_line()
            self.output.info(stage_name)

    def detail(self, message: str, indent: int = 1) -> None:
        """Report detailed progress (only in verbose mode)."""
        self.output.debug(message, indent=indent)

    def item_progress(
        self, current: int, total: int, item_name: str, status: str = "RUN", duration: Optional[float] = None
    ) -> None:
        """Report progress on individual items."""
        self.output.progress(current, total, item_name, status, duration=duration)

    def blank_line(self) -> None:
        """Insert a blank line for visual separation."""
        self.output.blank_line()

    def warning(self, message: str) -> None:
        """Report warning."""
        self.output.warning(message)

    def error(self, message: str) -> None:
        """Report error."""
        self.output.error(message)


# Convenience function for creating default callback
def default_progress_callback() -> NoOpProgressCallback:
    """
    Create default no-op progress callback.

    Returns:
        NoOpProgressCallback instance
    """
    return NoOpProgressCallback()
