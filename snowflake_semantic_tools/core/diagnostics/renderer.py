"""
Diagnostic Renderer

Produces ruff/rustc-style diagnostic output with source snippets,
caret highlighting, colored severity, and inline help text.

Output format:
    error[SST-V002]: Metric 'total_revenue' references unknown table 'orders_v2'
      --> snowflake_semantic_models/metrics/metrics.yml:42:15
       |
    42 |     - {{ ref('orders_v2') }}
       |               ^^^^^^^^^^ table not found in dbt models
       |
       = help: Did you mean 'orders'? Available: orders, customers, products

Respects:
    - NO_COLOR env var (disables ANSI colors)
    - Non-TTY stdout (auto-disables colors when piping)
    - --output plain mode (minimal formatting)
"""

import os
import sys
from typing import TYPE_CHECKING, Dict, List, Optional

from snowflake_semantic_tools.core.diagnostics.source_reader import SourceReader

if TYPE_CHECKING:
    from snowflake_semantic_tools.core.models.validation import ValidationIssue, ValidationSeverity


class DiagnosticRenderer:
    """Renders validation issues as ruff/rustc-style diagnostics."""

    SEVERITY_COLORS = {
        "error": "\033[1;31m",
        "warning": "\033[1;33m",
        "info": "\033[1;36m",
        "success": "\033[1;32m",
    }
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"
    BLUE = "\033[1;34m"
    CYAN = "\033[36m"

    def __init__(self, use_colors: Optional[bool] = None):
        if use_colors is None:
            self.use_colors = self._should_use_colors()
        else:
            self.use_colors = use_colors

    @staticmethod
    def _should_use_colors() -> bool:
        if os.environ.get("NO_COLOR"):
            return False
        if not sys.stdout.isatty():
            return False
        return True

    def _color(self, text: str, color: str) -> str:
        if not self.use_colors:
            return text
        return f"{color}{text}{self.RESET}"

    def _severity_prefix(self, severity: "ValidationSeverity") -> str:
        name = severity.value
        color = self.SEVERITY_COLORS.get(name, "")
        if self.use_colors and color:
            return f"{color}{name}{self.RESET}"
        return name

    def render(self, issue: "ValidationIssue") -> str:
        """
        Render a single diagnostic issue as a formatted string.

        Args:
            issue: The validation issue to render

        Returns:
            Multi-line formatted diagnostic string
        """
        lines = []

        severity_name = issue.severity.value
        rule_id = issue.rule_id or ""
        code_bracket = f"[{rule_id}]" if rule_id else ""

        header = f"{severity_name}{code_bracket}: {issue.message}"
        if self.use_colors:
            color = self.SEVERITY_COLORS.get(severity_name, "")
            header = f"{color}{severity_name}{code_bracket}{self.RESET}: {self.BOLD}{issue.message}{self.RESET}"
        lines.append(header)

        if issue.file_path and issue.file_path.strip():
            location = self._format_location(issue.file_path, issue.line_number, issue.column_number)
            arrow = self._color("-->", self.BLUE)
            lines.append(f"  {arrow} {location}")

        if issue.file_path and issue.line_number:
            snippet_lines = self._render_source_snippet(issue)
            if snippet_lines:
                lines.extend(snippet_lines)

        if issue.suggestion:
            lines.append(self._color("   |", self.BLUE))
            help_prefix = self._color("   = help:", self.CYAN)
            lines.append(f"{help_prefix} {issue.suggestion}")

        return "\n".join(lines)

    def render_batch(self, issues: List["ValidationIssue"], group_by_file: bool = True) -> str:
        """
        Render multiple diagnostics, optionally grouped by file.

        Args:
            issues: List of validation issues
            group_by_file: Whether to group issues by source file

        Returns:
            Multi-line formatted string with all diagnostics
        """
        if not issues:
            return ""

        if not group_by_file:
            return "\n\n".join(self.render(issue) for issue in issues)

        grouped: Dict[str, List["ValidationIssue"]] = {}
        for issue in issues:
            key = issue.file_path or "(unknown)"
            grouped.setdefault(key, []).append(issue)

        parts = []
        for file_path, file_issues in grouped.items():
            sorted_issues = sorted(file_issues, key=lambda i: i.line_number or 0)
            for issue in sorted_issues:
                parts.append(self.render(issue))

        return "\n\n".join(parts)

    def _format_location(self, file_path: str, line: Optional[int], column: Optional[int]) -> str:
        try:
            display_path = os.path.relpath(file_path)
        except ValueError:
            display_path = file_path

        if line and column:
            return f"{display_path}:{line}:{column}"
        elif line:
            return f"{display_path}:{line}"
        return display_path

    def _render_source_snippet(self, issue: "ValidationIssue") -> List[str]:
        if not issue.file_path or not issue.line_number:
            return []

        source_lines = SourceReader.get_source_lines(
            issue.file_path, issue.line_number, context_before=0, context_after=0
        )

        if not source_lines:
            return []

        lines = []
        pipe = self._color("|", self.BLUE)
        gutter_width = max(len(str(ln)) for ln, _ in source_lines) + 1

        lines.append(f"{'':>{gutter_width}} {pipe}")

        for line_num, line_content in source_lines:
            num_str = self._color(f"{line_num:>{gutter_width}}", self.BLUE)
            lines.append(f"{num_str} {pipe} {line_content}")

        if issue.column_number and source_lines:
            _, line_content = source_lines[0]
            col = issue.column_number - 1
            highlight_len = self._calculate_highlight_length(line_content, col, issue)
            if highlight_len > 0:
                pointer = " " * col + "^" * highlight_len
                pointer_colored = self._color(pointer, self.SEVERITY_COLORS.get(issue.severity.value, ""))
                lines.append(f"{'':>{gutter_width}} {pipe} {pointer_colored}")

        lines.append(f"{'':>{gutter_width}} {pipe}")
        return lines

    def _calculate_highlight_length(self, line_content: str, col: int, issue: "ValidationIssue") -> int:
        context = issue.context or {}
        highlight_text = context.get("highlight_text", "")
        if highlight_text:
            return len(highlight_text)

        if col < len(line_content):
            remaining = line_content[col:]
            word_end = 0
            for ch in remaining:
                if ch in " \t,)]}'\"\n":
                    break
                word_end += 1
            return max(word_end, 1)

        return 1
