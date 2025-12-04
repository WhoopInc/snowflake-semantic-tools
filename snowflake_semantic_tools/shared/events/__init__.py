"""
Event System for SST

Professional event-based logging inspired by dbt-core.
Provides clean CLI output with structured logging underneath.
"""

from .handler import EventHandler, fire_event, setup_events
from .types import (  # Enrichment events; Validation events; Config events; Generation events; Snowflake events; Extraction events; Format events
    BaseEvent,
    ConfigValidationError,
    ConfigValidationWarning,
    EnrichmentCompleted,
    EnrichmentStarted,
    EventLevel,
    ExtractionCompleted,
    ExtractionStarted,
    FormatCompleted,
    FormatStarted,
    GenerationCompleted,
    GenerationStarted,
    ModelEnriched,
    ModelEnrichmentSkipped,
    SnowflakeConnected,
    SnowflakeError,
    SnowflakeQueryExecuted,
    ValidationCompleted,
    ValidationError,
    ValidationStarted,
    ValidationWarning,
    ViewGenerated,
    ViewGenerationFailed,
)

__all__ = [
    "BaseEvent",
    "EventLevel",
    "EnrichmentStarted",
    "EnrichmentCompleted",
    "ModelEnriched",
    "ModelEnrichmentSkipped",
    "ValidationStarted",
    "ValidationCompleted",
    "ValidationError",
    "ValidationWarning",
    "ConfigValidationError",
    "ConfigValidationWarning",
    "GenerationStarted",
    "GenerationCompleted",
    "ViewGenerated",
    "ViewGenerationFailed",
    "SnowflakeConnected",
    "SnowflakeQueryExecuted",
    "SnowflakeError",
    "ExtractionStarted",
    "ExtractionCompleted",
    "FormatStarted",
    "FormatCompleted",
    "EventHandler",
    "setup_events",
    "fire_event",
]
