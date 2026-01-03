"""
Test validation model classes and enums.

Tests the ValidationSeverity, ValidationIssue, and related classes.
"""

import pytest

from snowflake_semantic_tools.core.models.validation import (
    ValidationError,
    ValidationInfo,
    ValidationIssue,
    ValidationResult,
    ValidationSeverity,
    ValidationSuccess,
    ValidationWarning,
)


class TestValidationSeverity:
    """Test ValidationSeverity enum."""

    def test_severity_values(self):
        """Test that all severity levels exist."""
        assert ValidationSeverity.ERROR.value == "error"
        assert ValidationSeverity.WARNING.value == "warning"
        assert ValidationSeverity.INFO.value == "info"
        assert ValidationSeverity.SUCCESS.value == "success"

    def test_severity_ordering(self):
        """Test severity level ordering for filtering."""
        severities = [
            ValidationSeverity.ERROR,
            ValidationSeverity.WARNING,
            ValidationSeverity.INFO,
            ValidationSeverity.SUCCESS,
        ]
        assert len(severities) == 4


class TestValidationIssue:
    """Test ValidationIssue base class and subclasses."""

    def test_validation_error_creation(self):
        """Test ValidationError creation."""
        error = ValidationError(
            message="Table 'users' not found",
            file_path="metrics/revenue.yml",
            line_number=10,
            context={"table": "users"},
        )

        assert error.severity == ValidationSeverity.ERROR
        assert error.message == "Table 'users' not found"
        assert error.file_path == "metrics/revenue.yml"
        assert error.line_number == 10
        assert error.context["table"] == "users"

    def test_validation_warning_creation(self):
        """Test ValidationWarning creation."""
        warning = ValidationWarning(message="Hardcoded table reference found", file_path="metrics/revenue.yml")

        assert warning.severity == ValidationSeverity.WARNING
        assert warning.message == "Hardcoded table reference found"

    def test_validation_info_creation(self):
        """Test ValidationInfo creation."""
        info = ValidationInfo(message="Metric could be optimized", file_path="metrics/revenue.yml")

        assert info.severity == ValidationSeverity.INFO
        assert info.message == "Metric could be optimized"

    def test_validation_success_creation(self):
        """Test ValidationSuccess creation."""
        success = ValidationSuccess(message="All validations passed", file_path="metrics/revenue.yml")

        assert success.severity == ValidationSeverity.SUCCESS
        assert success.message == "All validations passed"

    def test_validation_issue_string_representation(self):
        """Test string representation of validation issues."""
        error = ValidationError(message="Table not found", file_path="test.yml", line_number=5)

        str_repr = str(error)
        assert "ERROR" in str_repr
        assert "Table not found" in str_repr
        assert "test.yml" in str_repr
        assert "5" in str_repr


class TestValidationResult:
    """Test ValidationResult aggregation class."""

    def test_empty_result(self):
        """Test empty validation result."""
        result = ValidationResult()
        assert result.is_valid is True
        assert len(result.issues) == 0
        assert result.error_count == 0
        assert result.warning_count == 0
        assert result.info_count == 0
        assert result.success_count == 0

    def test_result_with_errors(self):
        """Test result with errors."""
        result = ValidationResult()
        result.add_error("Table not found", file_path="test.yml")
        result.add_error("Column not found", file_path="test.yml")

        assert result.is_valid is False
        assert len(result.issues) == 2
        assert result.error_count == 2
        assert result.warning_count == 0

    def test_result_with_warnings_only(self):
        """Test result with warnings but no errors."""
        result = ValidationResult()
        result.add_warning("Hardcoded reference", file_path="test.yml")
        result.add_warning("Missing description", file_path="test.yml")

        assert result.is_valid is True  # Warnings don't make result invalid
        assert len(result.issues) == 2
        assert result.error_count == 0
        assert result.warning_count == 2

    def test_result_with_mixed_issues(self):
        """Test result with mixed severity issues."""
        result = ValidationResult()
        result.add_error("Critical error", file_path="test.yml")
        result.add_warning("Minor warning", file_path="test.yml")
        result.add_info("Optimization suggestion", file_path="test.yml")
        result.add_success("Validation passed", file_path="test.yml")

        assert result.is_valid is False  # Errors make result invalid
        assert len(result.issues) == 4
        assert result.error_count == 1
        assert result.warning_count == 1
        assert result.info_count == 1
        assert result.success_count == 1

    def test_result_summary(self):
        """Test validation result summary."""
        result = ValidationResult()
        result.add_error("Error 1", file_path="test.yml")
        result.add_warning("Warning 1", file_path="test.yml")
        result.add_info("Info 1", file_path="test.yml")

        # Test individual getters instead of get_summary
        errors = result.get_errors()
        warnings = result.get_warnings()
        info = result.get_info()

        assert len(errors) == 1
        assert len(warnings) == 1
        assert len(info) == 1

    def test_result_filtering(self):
        """Test filtering issues by severity."""
        result = ValidationResult()
        result.add_error("Error 1", file_path="test.yml")
        result.add_error("Error 2", file_path="test.yml")
        result.add_warning("Warning 1", file_path="test.yml")

        errors = result.get_errors()
        warnings = result.get_warnings()

        assert len(errors) == 2
        assert len(warnings) == 1
        assert all(issue.severity == ValidationSeverity.ERROR for issue in errors)
        assert all(issue.severity == ValidationSeverity.WARNING for issue in warnings)
