#!/usr/bin/env python3
"""
Error Handling Utilities

Utilities for formatting YAML parsing errors and managing error state throughout the parsing process.
Provides clean, structured error reporting for better debugging and user experience.
"""

import re
from pathlib import Path
from typing import Any, Dict, List

import yaml

from snowflake_semantic_tools.shared import get_logger

logger = get_logger("yaml_parser.error_handler")


class ErrorTracker:
    """Tracks and manages parsing errors throughout the YAML parsing process."""

    def __init__(self):
        """Initialize the error tracker."""
        self.errors: List[str] = []

    def add_error(self, error_message: str) -> None:
        """
        Add an error message to the tracker.

        Args:
            error_message: The error message to track
        """
        self.errors.append(error_message)

    def get_error_count(self) -> int:
        """Get the total number of errors tracked."""
        return len(self.errors)

    def get_all_errors(self) -> List[str]:
        """Get all tracked error messages."""
        return self.errors.copy()

    def get_structured_errors(self) -> List[Dict[str, str]]:
        """Get structured error information for detailed reporting."""
        return [parse_error_message(error) for error in self.errors]

    def clear(self) -> None:
        """Clear all tracked errors."""
        self.errors.clear()


def format_yaml_error(yaml_error: yaml.YAMLError, file_path: Path) -> str:
    """
    Format YAML parsing errors into clean, readable messages.

    Args:
        yaml_error: The YAML parsing error
        file_path: Path to the file where the error occurred

    Returns:
        Formatted error message string
    """
    try:
        error_str = str(yaml_error)
        filename = file_path.name

        # Extract line number if available
        line_num = "unknown"
        if hasattr(yaml_error, "problem_mark") and yaml_error.problem_mark:
            line_num = str(yaml_error.problem_mark.line + 1)

        # Extract error type from the error message
        if "while parsing a block collection" in error_str:
            error_type = "Block collection parsing error"
        elif "while scanning a simple key" in error_str:
            error_type = "Simple key scanning error"
        elif "mapping values are not allowed here" in error_str:
            error_type = "Invalid mapping values"
        elif "while parsing a block mapping" in error_str:
            error_type = "Block mapping parsing error"
        elif "could not find expected" in error_str:
            error_type = "Missing expected character"
        else:
            error_type = "YAML syntax error"

        return f"YAML error in {filename} at line {line_num}: {error_type}"

    except Exception:
        # Fallback if error parsing fails
        return f"YAML parsing error in {file_path.name}: {str(yaml_error)[:100]}..."


def parse_error_message(error_msg: str) -> Dict[str, str]:
    """
    Parse error message to extract structured information.

    Args:
        error_msg: Raw error message string

    Returns:
        Dictionary with structured error information
    """
    # Extract file path
    file_match = re.search(r"in (.+?):", error_msg)
    file_path = file_match.group(1) if file_match else "Unknown"

    # Extract line number
    line_match = re.search(r"line (\d+)", error_msg)
    line_number = line_match.group(1) if line_match else "Unknown"

    # Extract error type - check for our formatted error types first
    if "Block collection parsing error" in error_msg:
        error_type = "YAML Syntax Error"
        specific_error = "Block collection parsing error"
    elif "Simple key scanning error" in error_msg:
        error_type = "YAML Syntax Error"
        specific_error = "Simple key scanning error"
    elif "Invalid mapping values" in error_msg:
        error_type = "YAML Syntax Error"
        specific_error = "Invalid mapping values"
    elif "Block mapping parsing error" in error_msg:
        error_type = "YAML Syntax Error"
        specific_error = "Block mapping parsing error"
    elif "Missing expected character" in error_msg:
        error_type = "YAML Syntax Error"
        specific_error = "Missing expected character"
    elif "YAML syntax error" in error_msg:
        error_type = "YAML Syntax Error"
        specific_error = "YAML syntax error"
    elif "YAML parsing error" in error_msg or "YAML error" in error_msg:
        error_type = "YAML Syntax Error"
        specific_error = "YAML parsing error"
    else:
        error_type = "Processing Error"
        specific_error = "File processing error"

    # Extract just the filename from full path
    filename = file_path.split("/")[-1] if "/" in file_path else file_path

    return {
        "file_path": file_path,
        "filename": filename,
        "line_number": line_number,
        "error_type": error_type,
        "specific_error": specific_error,
        "full_message": error_msg,
    }
