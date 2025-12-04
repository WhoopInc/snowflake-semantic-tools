"""
CLI Output Formatter

Provides dbt-style CLI output with immediate feedback, progress indicators,
and consistent formatting across all commands.

Design Philosophy:
- Immediate feedback (no silent delays)
- Timestamps on everything (HH:MM:SS format)
- Progress indicators with status icons
- Color coding when terminal supports it
- Clean, professional appearance

This complements (not replaces) the existing event system:
- Events: Structured logging for debugging/analysis
- CLIOutput: User-facing CLI formatting

Inspired by dbt-core's excellent CLI UX.
"""

import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import click


class CLIOutput:
    """
    Standardized CLI output handler matching dbt's professional format.

    Features:
    - Timestamped messages (HH:MM:SS)
    - Progress indicators (X of Y item_name .... [STATUS])
    - Status icons with colors ([OK], [ERROR], [WARN], [RUN])
    - Section headers and summaries
    - Respects quiet mode (suppresses non-error output)
    - Respects verbose mode (shows additional detail)

    Usage:
        output = CLIOutput(verbose=True, quiet=False)
        output.info("Running with sst=0.1.0")
        output.progress(1, 10, "customers", "RUN")
        output.progress(1, 10, "customers", "OK", duration=2.3)
        output.success("Validation completed", duration=15.2)
    """

    def __init__(self, verbose: bool = False, quiet: bool = False, use_colors: bool = True):
        """
        Initialize CLI output handler.

        Args:
            verbose: Show additional debug information
            quiet: Suppress all non-error output
            use_colors: Enable color output (auto-disabled if not TTY)
        """
        self.verbose = verbose
        self.quiet = quiet
        self.use_colors = use_colors and sys.stdout.isatty()
        self.start_time = time.time()

    def timestamp(self) -> str:
        """Get current timestamp in HH:MM:SS format."""
        return datetime.now().strftime("%H:%M:%S")

    def info(self, message: str, indent: int = 0) -> None:
        """
        Print timestamped info message.

        Args:
            message: Message to display
            indent: Indentation level (0, 1, 2, etc.)

        Example:
            output.info("Running with sst=0.1.0")
            output.info("Found 628 models", indent=1)
        """
        if self.quiet:
            return

        prefix = "  " * indent
        click.echo(f"{self.timestamp()}  {prefix}{message}")

    def debug(self, message: str, indent: int = 0) -> None:
        """
        Print debug message (only in verbose mode).

        Args:
            message: Debug message to display
            indent: Indentation level
        """
        if self.quiet or not self.verbose:
            return

        prefix = "  " * indent
        msg = f"{self.timestamp()}  {prefix}{message}"
        # NO COLOR - plain text is most readable across all terminals
        click.echo(msg)

    def success(self, message: str, duration: Optional[float] = None) -> None:
        """
        Print success message with optional duration.

        Args:
            message: Success message
            duration: Optional duration in seconds

        Example:
            output.success("Validation completed", duration=15.2)
            # Output: 13:24:45  Validation completed [OK in 15.2s]
        """
        if self.quiet:
            return

        status = self._format_status("OK", duration)
        msg = f"{message} {status}"

        if self.use_colors:
            msg = f"{message} " + click.style(status, fg="green")

        click.echo(f"{self.timestamp()}  {msg}")

    def error(self, message: str, duration: Optional[float] = None) -> None:
        """
        Print error message with optional duration.

        Args:
            message: Error message
            duration: Optional duration in seconds

        Example:
            output.error("Validation failed", duration=5.1)
            # Output: 13:24:45  Validation failed [ERROR in 5.1s]
        """
        status = self._format_status("ERROR", duration)
        msg = f"{message} {status}"

        if self.use_colors:
            msg = f"{message} " + click.style(status, fg="red")

        click.echo(f"{self.timestamp()}  {msg}", err=True)

    def warning(self, message: str) -> None:
        """
        Print warning message.

        Args:
            message: Warning message
        """
        if self.quiet:
            return

        msg = f"{message} [WARN]"
        if self.use_colors:
            msg = f"{message} " + click.style("[WARN]", fg="yellow")

        click.echo(f"{self.timestamp()}  {msg}")

    def progress(
        self,
        current: int,
        total: int,
        item_name: str,
        status: str = "RUN",
        duration: Optional[float] = None,
        max_name_len: int = 40,
    ) -> None:
        """
        Show progress for item processing with dbt-style formatting.

        Args:
            current: Current item number (1-based)
            total: Total number of items
            item_name: Name of current item
            status: Status ("RUN", "OK", "ERROR", "WARN", "SKIP")
            duration: Optional duration in seconds (shown for OK/ERROR)
            max_name_len: Maximum length for item name before truncation

        Example:
            output.progress(1, 628, "customers", "RUN")
            # Output: 13:24:13    1 of 628  customers ...................... [RUN]

            output.progress(1, 628, "customers", "OK", duration=2.3)
            # Output: 13:24:15    1 of 628  customers ...................... [OK in 2.3s]
        """
        if self.quiet:
            return

        # Format numbers with right-alignment
        num_width = len(str(total))
        current_str = str(current).rjust(num_width)
        total_str = str(total).rjust(num_width)

        # Truncate long names
        if len(item_name) > max_name_len:
            item_name = item_name[: max_name_len - 3] + "..."

        # Calculate dots for alignment (ensure at least 1 dot)
        dots_count = max(1, max_name_len - len(item_name))
        dots = "." * dots_count

        # Format status with optional duration
        status_str = self._format_status(status, duration)

        # Build the line with only the status bracket colored
        line = f"{self.timestamp()}  {current_str} of {total_str}  {item_name} {dots} "

        if self.use_colors:
            status_color = {"RUN": "yellow", "OK": "green", "ERROR": "red", "WARN": "yellow", "SKIP": "cyan"}.get(
                status, "white"
            )
            # Color only the status bracket
            line += click.style(status_str, fg=status_color)
        else:
            line += status_str

        click.echo(line)

    def header(self, title: str, separator: str = "=") -> None:
        """
        Print section header with separator line.

        Args:
            title: Header title
            separator: Character to use for separator line

        Example:
            output.header("VALIDATION SUMMARY")
            # Output:
            # 13:24:45
            # 13:24:45  VALIDATION SUMMARY
            # 13:24:45  ==================
        """
        if self.quiet:
            return

        click.echo()  # Blank line before header
        click.echo(f"{self.timestamp()}  {title}")
        click.echo(f"{self.timestamp()}  {separator * len(title)}")

    def summary(self, stats: Dict[str, Any], title: str = "SUMMARY") -> None:
        """
        Print final summary with statistics.

        Args:
            stats: Dictionary of key-value pairs to display
            title: Summary section title

        Example:
            output.summary({
                'Status': 'PASSED',
                'Models': 628,
                'Errors': 0,
                'Warnings': 12
            })
        """
        if self.quiet:
            return

        self.header(title)

        for key, value in stats.items():
            click.echo(f"{self.timestamp()}  {key}: {value}")

    def done_line(
        self, passed: int = 0, warned: int = 0, errored: int = 0, skipped: int = 0, total: Optional[int] = None
    ) -> None:
        """
        Print dbt-style "Done." summary line.

        Args:
            passed: Number of successful items
            warned: Number of items with warnings
            errored: Number of failed items
            skipped: Number of skipped items
            total: Total items (auto-calculated if not provided)

        Example:
            output.done_line(passed=616, warned=12, errored=0, total=628)
            # Output: 13:24:45  Done. PASS=616 WARN=12 ERROR=0 SKIP=0 TOTAL=628
        """
        if self.quiet:
            return

        if total is None:
            total = passed + warned + errored + skipped

        # Build components with color coding
        components = []

        if self.use_colors:
            if passed > 0:
                components.append(click.style(f"PASS={passed}", fg="green"))
            else:
                components.append(f"PASS={passed}")

            if warned > 0:
                components.append(click.style(f"WARN={warned}", fg="yellow"))
            else:
                components.append(f"WARN={warned}")

            if errored > 0:
                components.append(click.style(f"ERROR={errored}", fg="red"))
            else:
                components.append(f"ERROR={errored}")

            if skipped > 0:
                components.append(click.style(f"SKIP={skipped}", fg="cyan"))
            else:
                components.append(f"SKIP={skipped}")

            components.append(f"TOTAL={total}")
            done_msg = "Done. " + " ".join(components)
        else:
            done_msg = f"Done. PASS={passed} WARN={warned} ERROR={errored} SKIP={skipped} TOTAL={total}"

        click.echo()
        click.echo(f"{self.timestamp()}  {done_msg}")

    def elapsed_time(self) -> float:
        """
        Get elapsed time since CLIOutput was initialized.

        Returns:
            Elapsed time in seconds
        """
        return time.time() - self.start_time

    def blank_line(self) -> None:
        """Print a blank line (respects quiet mode)."""
        if not self.quiet:
            click.echo()

    def rule(self, char: str = "-", width: int = 70) -> None:
        """
        Print a horizontal rule.

        Args:
            char: Character to use for the rule
            width: Width of the rule
        """
        if not self.quiet:
            click.echo(f"{self.timestamp()}  {char * width}")

    def _format_status(self, status: str, duration: Optional[float] = None) -> str:
        """
        Format status string with optional duration.

        Args:
            status: Status text (OK, ERROR, RUN, etc.)
            duration: Optional duration in seconds

        Returns:
            Formatted status string like "[OK in 2.3s]" or "[RUN]"
        """
        if duration is not None:
            return f"[{status} in {duration:.1f}s]"
        return f"[{status}]"

    def section(self, title: str) -> None:
        """
        Print a section title (less prominent than header).

        Args:
            title: Section title

        Example:
            output.section("Scanning dbt models...")
        """
        if not self.quiet:
            click.echo()
            click.echo(f"{self.timestamp()}  {title}")


class ProgressBatcher:
    """
    Helper for batching progress updates when processing many items.

    For 600+ models, showing every progress line is too verbose.
    This class batches updates to show progress at intervals.

    Usage:
        batcher = ProgressBatcher(total=628, batch_size=50)
        for i, model in enumerate(models, 1):
            if batcher.should_show(i):
                output.progress(i, 628, model.name, "OK")
    """

    def __init__(self, total: int, batch_size: int = 50):
        """
        Initialize progress batcher.

        Args:
            total: Total number of items
            batch_size: Show progress every N items
        """
        self.total = total
        self.batch_size = batch_size
        self.last_shown = 0

    def should_show(self, current: int) -> bool:
        """
        Check if progress should be shown for current item.

        Always shows:
        - First item
        - Last item
        - Every Nth item (based on batch_size)
        - Items with errors/warnings (caller's responsibility to check)

        Args:
            current: Current item number (1-based)

        Returns:
            True if progress should be shown
        """
        # Always show first and last
        if current == 1 or current == self.total:
            self.last_shown = current
            return True

        # Show every batch_size items
        if current - self.last_shown >= self.batch_size:
            self.last_shown = current
            return True

        return False

    def should_batch(self) -> bool:
        """
        Check if batching should be used based on total count.

        Returns:
            True if batching is recommended (total > 100)
        """
        return self.total > 100
