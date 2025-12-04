"""
Validation Models

Data classes for capturing and reporting validation results during semantic model processing.

Provides a comprehensive framework for tracking validation issues at different severity
levels, from critical errors that block processing to informational messages that
suggest improvements.
"""

import logging
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
    """

    severity: ValidationSeverity
    message: str
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    column_number: Optional[int] = None
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {"severity": self.severity.value, "message": self.message}

        if self.file_path:
            result["file_path"] = self.file_path
        if self.line_number:
            result["line_number"] = self.line_number
        if self.column_number:
            result["column_number"] = self.column_number
        if self.context:
            result["context"] = self.context

        return result

    def __str__(self) -> str:
        """String representation of the issue."""
        location = ""
        if self.file_path:
            location = f"{self.file_path}"
            if self.line_number:
                location += f":{self.line_number}"
                if self.column_number:
                    location += f":{self.column_number}"
            location += " - "

        return f"[{self.severity.value.upper()}] {location}{self.message}"


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
    _fire_events: bool = field(default=True, init=False, repr=False)  # Fire events in real-time

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

    def add_error(self, message: str, **kwargs):
        """Add an error to the result with real-time logging and event firing."""
        self.issues.append(ValidationError(message, **kwargs))

        # Log immediately to file (real-time debugging)
        _logger.error(message)

        # Fire event for real-time display (if events available)
        if _EVENTS_AVAILABLE and hasattr(self, "_fire_events") and self._fire_events:
            # Extract model name from context or message
            model_name = kwargs.get("context", {}).get("table_name", "unknown")

            if model_name == "unknown":
                # Try to extract from message
                import re

                # Try different patterns
                patterns = [
                    r"Table '([^']+)'",
                    r"Relationship '([^']+)'",
                    r"Metric '([^']+)'",
                    r"Filter '([^']+)'",
                    r"Column '([^']+)' in table '([^']+)'",  # Extract table name
                ]
                for pattern in patterns:
                    match = re.search(pattern, message)
                    if match:
                        # For column errors, use table name (group 2)
                        model_name = match.group(2) if match.lastindex == 2 else match.group(1)
                        break

            fire_event(ValidationErrorEvent(model_name=model_name, error_message=message))

    def add_warning(self, message: str, **kwargs):
        """Add a warning to the result with real-time logging and event firing."""
        self.issues.append(ValidationWarning(message, **kwargs))

        # Log immediately to file (real-time debugging)
        _logger.warning(message)

        # Fire event for real-time display (if events available)
        if _EVENTS_AVAILABLE and hasattr(self, "_fire_events") and self._fire_events:
            # Extract model name from context or message
            model_name = kwargs.get("context", {}).get("table_name", "unknown")

            if model_name == "unknown":
                # Try to extract from message
                import re

                # Try different patterns
                patterns = [
                    r"Table '([^']+)'",
                    r"Relationship '([^']+)'",
                    r"Metric '([^']+)'",
                    r"Filter '([^']+)'",
                    r"Column '([^']+)' in table '([^']+)'",  # Extract table name
                ]
                for pattern in patterns:
                    match = re.search(pattern, message)
                    if match:
                        # For column errors, use table name (group 2)
                        model_name = match.group(2) if match.lastindex == 2 else match.group(1)
                        break

            fire_event(ValidationWarningEvent(model_name=model_name, warning_message=message))

    def add_info(self, message: str, **kwargs):
        """Add an info message to the result."""
        self.issues.append(ValidationInfo(message, **kwargs))

    def add_success(self, message: str, **kwargs):
        """Add a success message to the result."""
        self.issues.append(ValidationSuccess(message, **kwargs))

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

    def print_summary(self, verbose: bool = False):
        """
        Print validation errors and warnings with a nice summary.

        Args:
            verbose: If True, shows full warning list. If False, just summary.
        """
        # Print a clean summary box
        print("\n" + "━" * 70)
        print("VALIDATION SUMMARY")
        print("━" * 70)

        error_count = len(self.get_errors())
        warning_count = len(self.get_warnings())

        # Status indicator
        if error_count == 0 and warning_count == 0:
            status = "PASSED - No issues found"
        elif error_count == 0:
            status = "PASSED - With warnings"
        else:
            status = "FAILED - Fix errors before deployment"

        print(f"Status: {status}")
        print(f"Errors: {error_count}")
        print(f"Warnings: {warning_count}")

        # Show breakdown if there are issues
        if error_count > 0:
            print(f"\n{error_count} Error(s) - Must be fixed:")
            for i, error in enumerate(self.get_errors()[:10], 1):
                print(f"  {i}. {error}")
            if error_count > 10:
                print(f"  ... and {error_count - 10} more errors (run with --verbose for full list)")

        if warning_count > 0:
            # Dynamically categorize ALL warnings by pattern matching
            warnings = self.get_warnings()
            warning_str = [str(w).lower() for w in warnings]

            # Define comprehensive category patterns (order matters - most specific first)
            categories = [
                ("missing primary_key", lambda w: "missing critical metadata" in w and "primary_key" in w),
                ("missing description", lambda w: "missing required field: description" in w),
                (
                    "missing column_type",
                    lambda w: "missing required field: column_type" in w
                    or "missing required field: meta.sst.column_type" in w,
                ),
                (
                    "missing data_type",
                    lambda w: "missing required field: data_type" in w
                    or "missing required field: meta.sst.data_type" in w,
                ),
                ("apostrophes/quotes in synonyms", lambda w: "problematic characters" in w),
                ("no synonyms defined", lambda w: "no synonyms" in w and "defined" in w),
                ("enum missing sample values", lambda w: "is_enum=true but no sample_values" in w),
                ("duplicate metric expressions", lambda w: "identical expressions" in w),
                ("relationships missing primary keys", lambda w: "primary key" in w and "relationship" in w),
                ("circular dependencies", lambda w: "circular" in w),
                ("invalid column_type", lambda w: "invalid column_type" in w),
                ("hardcoded values in templates", lambda w: "hardcoded" in w),
                ("missing table references", lambda w: "table" in w and ("not found" in w or "does not exist" in w)),
            ]

            # Count by category
            category_counts = {}
            categorized_warnings = set()

            for category_name, pattern_func in categories:
                count = 0
                for i, w in enumerate(warning_str):
                    if i not in categorized_warnings and pattern_func(w):
                        count += 1
                        categorized_warnings.add(i)
                if count > 0:
                    category_counts[category_name] = count

            # Count uncategorized (truly "other")
            uncategorized = warning_count - len(categorized_warnings)

            print(f"\n{warning_count} Warning(s):")

            # Show categories in priority order
            for category, count in category_counts.items():
                # Add actionable hints for specific categories
                if category == "missing primary_key":
                    print(f"  - {count} tables skipped (missing primary_key metadata)")
                elif category == "apostrophes/quotes in synonyms":
                    print(f"  - {count} fields with apostrophes/quotes (run: sst format models/ --sanitize)")
                else:
                    print(f"  - {count} {category}")

            if uncategorized > 0:
                print(f"  - {uncategorized} other warnings (uncategorized)")

            if verbose:
                print("\nFull warning list:")
                for i, warning in enumerate(warnings[:50], 1):
                    print(f"  {i}. {warning}")
                if warning_count > 50:
                    print(f"  ... and {warning_count - 50} more warnings")
            else:
                print("\nRun with --verbose to see all warnings")

        print("━" * 70)

        # Action items
        if error_count > 0:
            print("\nNext steps:")
            print("  1. Fix errors listed above")
            print("  2. Run 'sst validate' again")
            print("  3. When clean, run 'sst extract' to deploy")
        elif warning_count > 0:
            print("\nOptional improvements:")
            print("  - Add synonyms to important tables for better AI queries")
            print("  - Enrich models missing metadata: sst enrich models/domain/")
            print("  - Warnings don't block deployment - ready for 'sst extract'")
        else:
            print("\nReady for deployment:")
            print("  sst extract --db YOUR_DB --schema YOUR_SCHEMA")

        print()
