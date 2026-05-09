"""
Diagnostic System for SST

Provides structured error reporting with stable error codes, source snippets,
and actionable suggestions. Inspired by rustc and ruff diagnostics.

Components:
    - error_codes: Registry of all SST error codes with metadata
    - renderer: Source-snippet formatter (ruff-style diagnostic output)
    - source_reader: File reading and entity line location
    - suggestions: Fuzzy matching and "did you mean?" helpers

Usage:
    from snowflake_semantic_tools.core.diagnostics import ERRORS, DiagnosticRenderer

    error_spec = ERRORS["SST-V002"]
    renderer = DiagnosticRenderer(use_colors=True)
    output = renderer.render(issue)
"""

from snowflake_semantic_tools.core.diagnostics.error_codes import (
    ERRORS,
    ErrorCategory,
    ErrorSpec,
)
from snowflake_semantic_tools.core.diagnostics.renderer import DiagnosticRenderer
from snowflake_semantic_tools.core.diagnostics.source_reader import SourceReader
from snowflake_semantic_tools.core.diagnostics.suggestions import (
    format_available_list,
    suggest_column,
    suggest_table,
    suggest_value,
)

__all__ = [
    "ERRORS",
    "ErrorCategory",
    "ErrorSpec",
    "DiagnosticRenderer",
    "SourceReader",
    "suggest_table",
    "suggest_column",
    "suggest_value",
    "format_available_list",
]
