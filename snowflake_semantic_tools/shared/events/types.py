"""
Event Type Definitions

Typed event classes for structured logging and clean CLI output.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class EventLevel(Enum):
    """Event severity levels."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class BaseEvent:
    """Base class for all SST events."""

    def message(self) -> str:
        """Human-readable message for CLI output."""
        raise NotImplementedError

    def log_dict(self) -> Dict[str, Any]:
        """Structured data for logging."""
        raise NotImplementedError

    def get_level(self) -> EventLevel:
        """Get event level - override in subclasses if dynamic."""
        return EventLevel.INFO


def format_duration(seconds: float) -> str:
    """Format duration with 1 decimal place."""
    return f"{seconds:.1f}s"


def format_progress(current: int, total: int) -> str:
    """Format progress indicator (e.g., " 1 of  5")."""
    return f"{current:2d} of {total:2d}"


# ============================================================================
# Enrichment Events
# ============================================================================


@dataclass
class EnrichmentStarted(BaseEvent):
    """Enrichment process started."""

    path: str
    model_count: int

    def get_level(self) -> EventLevel:
        return EventLevel.INFO

    def message(self) -> str:
        return f"Enriching {self.model_count} model(s) in {self.path}"

    def log_dict(self) -> Dict[str, Any]:
        return {"event": "enrichment_started", "path": self.path, "model_count": self.model_count}


@dataclass
class EnrichmentCompleted(BaseEvent):
    """Enrichment process completed."""

    total_models: int
    successful: int
    failed: int
    skipped: int
    duration_seconds: float

    def get_level(self) -> EventLevel:
        return EventLevel.INFO

    def message(self) -> str:
        status_parts = []
        if self.successful > 0:
            status_parts.append(f"{self.successful} enriched")
        if self.skipped > 0:
            status_parts.append(f"{self.skipped} skipped")
        if self.failed > 0:
            status_parts.append(f"{self.failed} failed")

        status = ", ".join(status_parts)
        return f"Completed in {format_duration(self.duration_seconds)} ({status})"

    def log_dict(self) -> Dict[str, Any]:
        return {
            "event": "enrichment_completed",
            "total_models": self.total_models,
            "successful": self.successful,
            "failed": self.failed,
            "skipped": self.skipped,
            "duration_seconds": self.duration_seconds,
        }


@dataclass
class ModelEnriched(BaseEvent):
    """Model successfully enriched."""

    model_name: str
    columns_updated: int
    duration_seconds: float
    current: int
    total: int

    def get_level(self) -> EventLevel:
        return EventLevel.INFO

    def message(self) -> str:
        # Use dots for alignment to match [RUN] line format (max 40 chars for name)
        max_name_len = 40
        name_display = self.model_name[:max_name_len] if len(self.model_name) > max_name_len else self.model_name
        dots_count = max(1, max_name_len - len(name_display))
        dots = "." * dots_count
        return f"{format_progress(self.current, self.total)}  {name_display} {dots} [OK in {format_duration(self.duration_seconds)}]"

    def log_dict(self) -> Dict[str, Any]:
        return {
            "event": "model_enriched",
            "model_name": self.model_name,
            "columns_updated": self.columns_updated,
            "duration_seconds": self.duration_seconds,
            "current": self.current,
            "total": self.total,
        }


@dataclass
class ModelEnrichmentSkipped(BaseEvent):
    """Model enrichment skipped."""

    model_name: str
    reason: str
    current: int
    total: int

    def get_level(self) -> EventLevel:
        return EventLevel.WARNING

    def message(self) -> str:
        # Use dots for alignment to match [RUN] line format
        max_name_len = 40
        name_display = self.model_name[:max_name_len] if len(self.model_name) > max_name_len else self.model_name
        dots_count = max(1, max_name_len - len(name_display))
        dots = "." * dots_count
        return f"{format_progress(self.current, self.total)}  {name_display} {dots} [SKIP - {self.reason}]"

    def log_dict(self) -> Dict[str, Any]:
        return {
            "event": "model_enrichment_skipped",
            "model_name": self.model_name,
            "reason": self.reason,
            "current": self.current,
            "total": self.total,
        }


# ============================================================================
# Validation Events
# ============================================================================


@dataclass
class ValidationStarted(BaseEvent):
    """Validation process started."""

    model_count: int

    def get_level(self) -> EventLevel:
        return EventLevel.INFO

    def message(self) -> str:
        return f"Validating {self.model_count} model(s)"

    def log_dict(self) -> Dict[str, Any]:
        return {"event": "validation_started", "model_count": self.model_count}


@dataclass
class ValidationCompleted(BaseEvent):
    """Validation process completed."""

    total_models: int
    error_count: int
    warning_count: int
    duration_seconds: float

    def get_level(self) -> EventLevel:
        return EventLevel.INFO

    def message(self) -> str:
        if self.error_count == 0 and self.warning_count == 0:
            return f"Validation passed ({self.total_models} models)"
        else:
            return f"Validation completed with {self.error_count} error(s), {self.warning_count} warning(s)"

    def log_dict(self) -> Dict[str, Any]:
        return {
            "event": "validation_completed",
            "total_models": self.total_models,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "duration_seconds": self.duration_seconds,
        }


@dataclass
class ValidationError(BaseEvent):
    """Validation error found."""

    model_name: str
    error_message: str

    def get_level(self) -> EventLevel:
        return EventLevel.ERROR

    def message(self) -> str:
        return f"ERROR in {self.model_name}: {self.error_message}"

    def log_dict(self) -> Dict[str, Any]:
        return {"event": "validation_error", "model_name": self.model_name, "error_message": self.error_message}


@dataclass
class ValidationWarning(BaseEvent):
    """Validation warning found."""

    model_name: str
    warning_message: str

    def get_level(self) -> EventLevel:
        return EventLevel.WARNING

    def message(self) -> str:
        return f"WARNING in {self.model_name}: {self.warning_message}"

    def log_dict(self) -> Dict[str, Any]:
        return {"event": "validation_warning", "model_name": self.model_name, "warning_message": self.warning_message}


# ============================================================================
# Configuration Events
# ============================================================================


@dataclass
class ConfigValidationError(BaseEvent):
    """Configuration validation error - required field missing."""

    field: str
    message_text: str  # Renamed from 'message' to avoid conflict with message() method

    def get_level(self) -> EventLevel:
        return EventLevel.ERROR

    def message(self) -> str:
        return f"Config error: Missing required field '{self.field}' - {self.message_text}"

    def log_dict(self) -> Dict[str, Any]:
        return {
            "event": "config_validation_error",
            "field": self.field,
            "description": self.message_text,  # Use 'description' instead of 'message' to avoid logging conflict
        }


@dataclass
class ConfigValidationWarning(BaseEvent):
    """Configuration validation warning - optional field missing but recommended."""

    field: str
    message_text: str  # Renamed from 'message' to avoid conflict with message() method
    default_value: Any = field(default=None)

    def get_level(self) -> EventLevel:
        return EventLevel.WARNING

    def message(self) -> str:
        return f"Config warning: '{self.field}' not set (using default: {self.default_value}) - {self.message_text}"

    def log_dict(self) -> Dict[str, Any]:
        return {
            "event": "config_validation_warning",
            "field": self.field,
            "description": self.message_text,  # Use 'description' instead of 'message' to avoid logging conflict
            "default_value": str(self.default_value),
        }


# ============================================================================
# Generation Events
# ============================================================================


@dataclass
class GenerationStarted(BaseEvent):
    """Semantic view generation started."""

    view_count: int

    def get_level(self) -> EventLevel:
        return EventLevel.INFO

    def message(self) -> str:
        return f"Generating {self.view_count} semantic view(s)"

    def log_dict(self) -> Dict[str, Any]:
        return {"event": "generation_started", "view_count": self.view_count}


@dataclass
class GenerationCompleted(BaseEvent):
    """Semantic view generation completed."""

    total_views: int
    successful: int
    failed: int
    duration_seconds: float

    def get_level(self) -> EventLevel:
        return EventLevel.INFO

    def message(self) -> str:
        return f"Generated {self.successful} of {self.total_views} view(s) in {format_duration(self.duration_seconds)}"

    def log_dict(self) -> Dict[str, Any]:
        return {
            "event": "generation_completed",
            "total_views": self.total_views,
            "successful": self.successful,
            "failed": self.failed,
            "duration_seconds": self.duration_seconds,
        }


@dataclass
class ViewGenerated(BaseEvent):
    """Semantic view successfully generated."""

    view_name: str
    table_count: int
    duration_seconds: float
    current: int
    total: int
    executed: bool

    def get_level(self) -> EventLevel:
        return EventLevel.INFO

    def message(self) -> str:
        action = "created" if self.executed else "generated"
        # Use dots for alignment to match [RUN] line format (max 40 chars for name)
        max_name_len = 40
        name_display = self.view_name[:max_name_len] if len(self.view_name) > max_name_len else self.view_name
        dots_count = max(1, max_name_len - len(name_display))
        dots = "." * dots_count
        return f"{format_progress(self.current, self.total)}  {name_display} {dots} [{action.upper()} in {format_duration(self.duration_seconds)}]"

    def log_dict(self) -> Dict[str, Any]:
        return {
            "event": "view_generated",
            "view_name": self.view_name,
            "table_count": self.table_count,
            "duration_seconds": self.duration_seconds,
            "current": self.current,
            "total": self.total,
            "executed": self.executed,
        }


@dataclass
class ViewGenerationFailed(BaseEvent):
    """Semantic view generation failed."""

    view_name: str
    error_message: str
    current: int
    total: int

    def get_level(self) -> EventLevel:
        return EventLevel.ERROR

    def message(self) -> str:
        # Use dots for alignment to match [RUN] line format
        max_name_len = 40
        name_display = self.view_name[:max_name_len] if len(self.view_name) > max_name_len else self.view_name
        dots_count = max(1, max_name_len - len(name_display))
        dots = "." * dots_count
        return f"{format_progress(self.current, self.total)}  {name_display} {dots} [FAILED - {self.error_message}]"

    def log_dict(self) -> Dict[str, Any]:
        return {
            "event": "view_generation_failed",
            "view_name": self.view_name,
            "error_message": self.error_message,
            "current": self.current,
            "total": self.total,
        }


# ============================================================================
# Snowflake Events
# ============================================================================


@dataclass
class SnowflakeConnected(BaseEvent):
    """Successfully connected to Snowflake."""

    account: str
    warehouse: str
    role: str

    def get_level(self) -> EventLevel:
        return EventLevel.DEBUG

    def message(self) -> str:
        return f"Connected to Snowflake (account={self.account}, warehouse={self.warehouse}, role={self.role})"

    def log_dict(self) -> Dict[str, Any]:
        return {"event": "snowflake_connected", "account": self.account, "warehouse": self.warehouse, "role": self.role}


@dataclass
class SnowflakeQueryExecuted(BaseEvent):
    """Snowflake query executed."""

    query: str
    row_count: int
    duration_seconds: float

    def get_level(self) -> EventLevel:
        return EventLevel.DEBUG

    def message(self) -> str:
        # Truncate query for display
        query_preview = self.query[:80].replace("\n", " ") + ("..." if len(self.query) > 80 else "")
        return f"Query returned {self.row_count} row(s) in {self.duration_seconds:.2f}s: {query_preview}"

    def log_dict(self) -> Dict[str, Any]:
        return {
            "event": "snowflake_query_executed",
            "query": self.query[:500],  # Truncate for structured logs
            "row_count": self.row_count,
            "duration_seconds": self.duration_seconds,
        }


@dataclass
class SnowflakeError(BaseEvent):
    """Snowflake error occurred."""

    operation: str
    error_message: str

    def get_level(self) -> EventLevel:
        return EventLevel.ERROR

    def message(self) -> str:
        return f"Snowflake error during {self.operation}: {self.error_message}"

    def log_dict(self) -> Dict[str, Any]:
        return {"event": "snowflake_error", "operation": self.operation, "error_message": self.error_message}


# ============================================================================
# Extraction Events
# ============================================================================


@dataclass
class ExtractionStarted(BaseEvent):
    """Metadata extraction started."""

    model_count: int

    def get_level(self) -> EventLevel:
        return EventLevel.INFO

    def message(self) -> str:
        return f"Extracting metadata from {self.model_count} model(s)"

    def log_dict(self) -> Dict[str, Any]:
        return {"event": "extraction_started", "model_count": self.model_count}


@dataclass
class ExtractionCompleted(BaseEvent):
    """Metadata extraction completed."""

    total_models: int
    tables_loaded: int
    duration_seconds: float

    def get_level(self) -> EventLevel:
        return EventLevel.INFO

    def message(self) -> str:
        return f"Loaded {self.tables_loaded} table(s) from {self.total_models} model(s) in {format_duration(self.duration_seconds)}"

    def log_dict(self) -> Dict[str, Any]:
        return {
            "event": "extraction_completed",
            "total_models": self.total_models,
            "tables_loaded": self.tables_loaded,
            "duration_seconds": self.duration_seconds,
        }


# ============================================================================
# Format Events
# ============================================================================


@dataclass
class FormatStarted(BaseEvent):
    """YAML formatting started."""

    file_count: int

    def get_level(self) -> EventLevel:
        return EventLevel.INFO

    def message(self) -> str:
        return f"Formatting {self.file_count} file(s)"

    def log_dict(self) -> Dict[str, Any]:
        return {"event": "format_started", "file_count": self.file_count}


@dataclass
class FormatCompleted(BaseEvent):
    """YAML formatting completed."""

    total_files: int
    files_updated: int
    duration_seconds: float

    def get_level(self) -> EventLevel:
        return EventLevel.INFO

    def message(self) -> str:
        if self.files_updated == 0:
            return f"All {self.total_files} file(s) already formatted"
        return (
            f"Formatted {self.files_updated} of {self.total_files} file(s) in {format_duration(self.duration_seconds)}"
        )

    def log_dict(self) -> Dict[str, Any]:
        return {
            "event": "format_completed",
            "total_files": self.total_files,
            "files_updated": self.files_updated,
            "duration_seconds": self.duration_seconds,
        }
