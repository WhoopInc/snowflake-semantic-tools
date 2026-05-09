"""
Validation Models

Data classes for capturing and reporting validation results during semantic model processing.

Provides a comprehensive framework for tracking validation issues at different severity
levels, from critical errors that block processing to informational messages that
suggest improvements.
"""

import logging
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

# Import events for real-time display (optional)
try:
    from snowflake_semantic_tools.shared.events import fire_event
    from snowflake_semantic_tools.shared.events.types import ValidationError as ValidationErrorEvent
    from snowflake_semantic_tools.shared.events.types import ValidationWarning as ValidationWarningEvent

    _EVENTS_AVAILABLE = True
except ImportError:
    _EVENTS_AVAILABLE = False

# Get logger for real-time file logging
_logger = logging.getLogger("validation")


class ValidationSeverity(Enum):
    """Severity levels for validation issues.

    Levels:
        ERROR: Critical issues that prevent semantic model generation
        WARNING: Issues that should be reviewed but don't block processing
        INFO: Helpful suggestions for improving model quality
        SUCCESS: Confirmation that validation checks passed
    """

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    SUCCESS = "success"


@dataclass
class ValidationIssue:
    """
    Base class for validation issues found during semantic model processing.

    Captures detailed information about validation problems including their
    location in source files and contextual data to help with debugging.
    Each issue represents a single validation finding that needs attention.

    Extended fields (all Optional for backward compatibility):
        rule_id: Stable error code (e.g., "SST-V002") for programmatic matching
        suggestion: Actionable fix text (e.g., "Did you mean 'orders'?")
        entity_name: The metric/table/relationship name being validated
        source_snippet: The offending source line for inline display
    """

    severity: ValidationSeverity
    message: str
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    column_number: Optional[int] = None
    context: Dict[str, Any] = field(default_factory=dict)
    rule_id: Optional[str] = None
    suggestion: Optional[str] = None
    entity_name: Optional[str] = None
    source_snippet: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation for JSON output."""
        result = {"severity": self.severity.value, "message": self.message}

        if self.rule_id:
            result["code"] = self.rule_id
        if self.file_path:
            result["file"] = self.file_path
        if self.line_number:
            result["line"] = self.line_number
        if self.column_number:
            result["column"] = self.column_number
        if self.suggestion:
            result["suggestion"] = self.suggestion
        if self.entity_name:
            result["entity"] = self.entity_name
        if self.source_snippet:
            result["source_snippet"] = self.source_snippet
        if self.context:
            result["context"] = self.context

        return result

    def __str__(self) -> str:
        """String representation of the issue."""
        parts = []

        severity = self.severity.value.upper()
        code = f"[{self.rule_id}]" if self.rule_id else ""
        parts.append(f"{severity}{code}")

        if self.file_path:
            location = self.file_path
            if self.line_number:
                location += f":{self.line_number}"
                if self.column_number:
                    location += f":{self.column_number}"
            parts.append(location)

        parts.append(self.message)

        result = " ".join(parts) if len(parts) > 1 else parts[0]

        if self.suggestion:
            result += f"\n  = help: {self.suggestion}"

        return result


@dataclass
class ValidationError(ValidationIssue):
    """
    Critical validation error that blocks semantic model generation.

    Examples:
        - Referenced table doesn't exist in dbt models
        - Invalid SQL syntax in metric expressions
        - Circular dependencies between relationships
        - Missing required fields in YAML
    """

    def __init__(self, message: str, **kwargs):
        super().__init__(severity=ValidationSeverity.ERROR, message=message, **kwargs)


@dataclass
class ValidationWarning(ValidationIssue):
    """
    Non-critical issue that should be reviewed for best practices.

    Examples:
        - Missing descriptions for tables or columns
        - No sample values provided for dimensions
        - Metrics without any synonyms defined
        - Tables without primary keys (limits relationships)
    """

    def __init__(self, message: str, **kwargs):
        super().__init__(severity=ValidationSeverity.WARNING, message=message, **kwargs)


@dataclass
class ValidationInfo(ValidationIssue):
    """
    Informational message suggesting improvements or providing context.

    Examples:
        - Could add custom instructions for better AI guidance
        - Consider adding verified queries for this domain
        - Table has many columns - consider creating dimensions
        - Detected common patterns that could use filters
    """

    def __init__(self, message: str, **kwargs):
        super().__init__(severity=ValidationSeverity.INFO, message=message, **kwargs)


@dataclass
class ValidationSuccess(ValidationIssue):
    """
    Confirmation that validation checks passed successfully.

    Examples:
        - All metrics have valid SQL expressions
        - All referenced tables exist in dbt models
        - Relationships properly defined with primary keys
        - Semantic view validated successfully
    """

    def __init__(self, message: str, **kwargs):
        super().__init__(severity=ValidationSeverity.SUCCESS, message=message, **kwargs)


@dataclass
class ValidationResult:
    """
    Container for all validation findings from semantic model processing.

    Aggregates all validation issues found during parsing and validation,
    providing convenient methods to query, filter, and report on issues
    by severity. Used to determine if processing can continue and to
    generate user-friendly validation reports.
    """

    issues: List[ValidationIssue] = field(default_factory=list)
    _fire_events: bool = field(default=True, init=False, repr=False)

    def disable_events(self):
        """Disable real-time event firing (for batch operations)."""
        self._fire_events = False

    def enable_events(self):
        """Enable real-time event firing."""
        self._fire_events = True

    @property
    def is_valid(self) -> bool:
        """Check if validation passed (no errors)."""
        return not self.has_errors

    @property
    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return any(i.severity == ValidationSeverity.ERROR for i in self.issues)

    @property
    def has_warnings(self) -> bool:
        """Check if there are any warnings."""
        return any(i.severity == ValidationSeverity.WARNING for i in self.issues)

    @property
    def error_count(self) -> int:
        """Get count of errors."""
        return len(self.get_errors())

    @property
    def warning_count(self) -> int:
        """Get count of warnings."""
        return len(self.get_warnings())

    @property
    def info_count(self) -> int:
        """Get count of info messages."""
        return len(self.get_info())

    @property
    def success_count(self) -> int:
        """Get count of success messages."""
        return len(self.get_successes())

    def get_errors(self) -> List[ValidationError]:
        """Get all errors."""
        return [i for i in self.issues if i.severity == ValidationSeverity.ERROR]

    def get_warnings(self) -> List[ValidationWarning]:
        """Get all warnings."""
        return [i for i in self.issues if i.severity == ValidationSeverity.WARNING]

    def get_info(self) -> List[ValidationInfo]:
        """Get all info messages."""
        return [i for i in self.issues if i.severity == ValidationSeverity.INFO]

    def get_successes(self) -> List[ValidationSuccess]:
        """Get all success messages."""
        return [i for i in self.issues if i.severity == ValidationSeverity.SUCCESS]

    def add_error(
        self,
        message: str,
        file_path: Optional[str] = None,
        line_number: Optional[int] = None,
        **kwargs,
    ):
        """Add an error to the result with real-time logging and event firing.

        Args:
            message: Error message describing the issue
            file_path: Optional path to the source file where the error was found
            line_number: Optional line number in the source file
            **kwargs: Additional context (e.g., context dict)
        """
        self.issues.append(ValidationError(message, file_path=file_path, line_number=line_number, **kwargs))

        # Log immediately to file (real-time debugging)
        _logger.error(message)

        # Fire event for real-time display (if events available)
        if _EVENTS_AVAILABLE and hasattr(self, "_fire_events") and self._fire_events:
            model_name = self._extract_model_name_from_context(kwargs, message)
            fire_event(ValidationErrorEvent(model_name=model_name, error_message=message))

    def add_warning(
        self,
        message: str,
        file_path: Optional[str] = None,
        line_number: Optional[int] = None,
        **kwargs,
    ):
        """Add a warning to the result with real-time logging and event firing.

        Args:
            message: Warning message describing the issue
            file_path: Optional path to the source file where the warning was found
            line_number: Optional line number in the source file
            **kwargs: Additional context (e.g., context dict)
        """
        self.issues.append(ValidationWarning(message, file_path=file_path, line_number=line_number, **kwargs))

        # Log immediately to file (real-time debugging)
        _logger.warning(message)

        # Fire event for real-time display (if events available)
        if _EVENTS_AVAILABLE and hasattr(self, "_fire_events") and self._fire_events:
            model_name = self._extract_model_name_from_context(kwargs, message)
            fire_event(ValidationWarningEvent(model_name=model_name, warning_message=message))

    def add_info(
        self,
        message: str,
        file_path: Optional[str] = None,
        line_number: Optional[int] = None,
        **kwargs,
    ):
        """Add an info message to the result.

        Args:
            message: Info message
            file_path: Optional path to the source file
            line_number: Optional line number in the source file
            **kwargs: Additional context
        """
        self.issues.append(ValidationInfo(message, file_path=file_path, line_number=line_number, **kwargs))

    def add_success(
        self,
        message: str,
        file_path: Optional[str] = None,
        line_number: Optional[int] = None,
        **kwargs,
    ):
        """Add a success message to the result.

        Args:
            message: Success message
            file_path: Optional path to the source file
            line_number: Optional line number in the source file
            **kwargs: Additional context
        """
        self.issues.append(ValidationSuccess(message, file_path=file_path, line_number=line_number, **kwargs))

    _MODEL_NAME_PATTERNS = [
        re.compile(r"Table '([^']+)'", re.IGNORECASE),
        re.compile(r"Relationship '([^']+)'", re.IGNORECASE),
        re.compile(r"Metric '([^']+)'", re.IGNORECASE),
        re.compile(r"Filter '([^']+)'", re.IGNORECASE),
        re.compile(r"Verified.query '([^']+)'", re.IGNORECASE),
        re.compile(r"Column '([^']+)' in table '([^']+)'", re.IGNORECASE),
    ]

    def _extract_model_name_from_context(self, kwargs: Dict, message: str) -> str:
        model_name = kwargs.get("context", {}).get("table_name", "unknown")
        if model_name == "unknown":
            for pattern in self._MODEL_NAME_PATTERNS:
                match = pattern.search(message)
                if match:
                    model_name = match.group(2) if match.lastindex == 2 else match.group(1)
                    break
        return model_name

    def merge(self, other: "ValidationResult"):
        """
        Merge another validation result into this one.

        Args:
            other: Another validation result to merge
        """
        self.issues.extend(other.issues)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "is_valid": self.is_valid,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "issues": [i.to_dict() for i in self.issues],
        }

    def enrich_line_numbers(self):
        """Post-process: look up source line numbers, column offsets, and highlight tokens for diagnostics."""
        from pathlib import Path

        from snowflake_semantic_tools.core.diagnostics.source_reader import SourceReader

        file_entity_cache: Dict[str, Dict[str, Optional[int]]] = {}
        for issue in self.issues:
            if issue.file_path and issue.entity_name and not issue.line_number:
                file_path = issue.file_path
                if not Path(file_path).is_absolute():
                    file_path = str(Path.cwd() / file_path)
                if file_path not in file_entity_cache:
                    file_entity_cache[file_path] = {}
                if issue.entity_name not in file_entity_cache[file_path]:
                    line = SourceReader.find_entity_line(file_path, issue.entity_name)
                    file_entity_cache[file_path][issue.entity_name] = line

        for issue in self.issues:
            if issue.file_path and issue.entity_name and not issue.line_number:
                file_path = issue.file_path
                if not Path(file_path).is_absolute():
                    file_path = str(Path.cwd() / file_path)
                line = file_entity_cache.get(file_path, {}).get(issue.entity_name)
                if line:
                    issue.line_number = line

            if issue.file_path and issue.line_number and not issue.column_number:
                file_path = issue.file_path
                if not Path(file_path).is_absolute():
                    file_path = str(Path.cwd() / file_path)

                highlight_token = self._extract_highlight_token(issue)
                if highlight_token:
                    source_lines = SourceReader.get_source_lines(
                        file_path, issue.line_number, context_before=0, context_after=5
                    )
                    for line_num, line_content in source_lines:
                        col = SourceReader.find_template_column(line_content, highlight_token)
                        if col is not None:
                            issue.line_number = line_num
                            issue.column_number = col + 1
                            if not issue.context:
                                issue.context = {}
                            issue.context["highlight_text"] = highlight_token
                            break

    @staticmethod
    def _extract_highlight_token(issue: "ValidationIssue") -> Optional[str]:
        """Extract the problematic token to highlight from an issue's context or message."""
        import re

        ctx = issue.context or {}

        if ctx.get("table") and issue.rule_id in ("SST-V002", "SST-V041", "SST-V071"):
            return str(ctx["table"]).lower()
        if ctx.get("column") and issue.rule_id in ("SST-V003", "SST-V043", "SST-V011"):
            return str(ctx["column"]).lower()
        if ctx.get("relationship") and issue.rule_id == "SST-V044":
            return str(ctx["relationship"]).lower()
        if ctx.get("column_type") and issue.rule_id == "SST-V007":
            return str(ctx["column_type"])
        if ctx.get("data_type") and issue.rule_id == "SST-V008":
            return str(ctx["data_type"])

        match = re.search(r"'([^']{2,40})'", issue.message)
        if match and issue.rule_id in ("SST-V002", "SST-V003", "SST-V041", "SST-V043", "SST-V035"):
            return match.group(1).lower()

        return None

    def print_summary(self, verbose: bool = False):
        """
        Print validation errors and warnings using ruff-style diagnostics.

        Args:
            verbose: If True, shows full warning list. If False, just summary.
        """
        from snowflake_semantic_tools.core.diagnostics import DiagnosticRenderer

        renderer = DiagnosticRenderer()
        error_count = len(self.get_errors())
        warning_count = len(self.get_warnings())

        if error_count > 0:
            errors = self.get_errors()[:20]
            print()
            print(renderer.render_batch(errors))
            if error_count > 20:
                print(f"\n  ... and {error_count - 20} more errors (run with --verbose)")

        if warning_count > 0 and verbose:
            warnings = self.get_warnings()[:30]
            print()
            print(renderer.render_batch(warnings))
            if warning_count > 30:
                print(f"\n  ... and {warning_count - 30} more warnings")

        print("\n" + "━" * 70)

        if error_count == 0 and warning_count == 0:
            status = "PASSED - No issues found"
        elif error_count == 0:
            status = "PASSED - With warnings"
        else:
            status = "FAILED - Fix errors before deployment"

        print(f"  {status}")
        print(f"  Errors: {error_count}  Warnings: {warning_count}")
        print("━" * 70)

        if error_count > 0:
            print("\n  Next: Fix errors above, then run 'sst validate' again")
        elif warning_count > 0 and not verbose:
            print(f"\n  {warning_count} warnings (run with --verbose to see details)")
            print("  Warnings don't block deployment — ready for 'sst extract'")
        elif warning_count == 0:
            print("\n  Ready for deployment: sst extract")

        print()
