"""Tests for error code registry."""

import pytest

from snowflake_semantic_tools.core.diagnostics.error_codes import (
    ERRORS,
    ErrorCategory,
    ErrorSpec,
    get_error,
    get_errors_by_category,
)


class TestErrorCodeRegistry:
    def test_registry_not_empty(self):
        assert len(ERRORS) > 0

    def test_all_codes_have_required_fields(self):
        for code, spec in ERRORS.items():
            assert spec.code == code
            assert spec.title
            assert isinstance(spec.category, ErrorCategory)
            assert spec.doc_anchor

    def test_no_duplicate_codes(self):
        codes = list(ERRORS.keys())
        assert len(codes) == len(set(codes))

    def test_code_format(self):
        for code in ERRORS:
            assert code.startswith("SST-")
            parts = code.split("-")
            assert len(parts) == 2
            assert parts[1][0] in "VPEGCDK"
            assert parts[1][1:].isdigit()

    def test_get_error_exists(self):
        spec = get_error("SST-V001")
        assert spec.title == "Missing required field"

    def test_get_error_not_found(self):
        with pytest.raises(KeyError):
            get_error("SST-Z999")

    def test_get_errors_by_category(self):
        validation_errors = get_errors_by_category(ErrorCategory.VALIDATION)
        assert all(v.category == ErrorCategory.VALIDATION for v in validation_errors.values())
        assert len(validation_errors) > 10

    def test_categories_have_codes(self):
        for category in ErrorCategory:
            errors = get_errors_by_category(category)
            assert len(errors) > 0, f"Category {category.value} has no error codes"

    def test_error_spec_url(self):
        spec = get_error("SST-V001")
        assert "error-codes.md" in spec.url
        assert "#sstv001" in spec.url
