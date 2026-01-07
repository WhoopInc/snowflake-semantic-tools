"""
Comprehensive tests for config_validator module.

Tests all config fields with fixtures for each possible configuration state.
"""

from pathlib import Path
from typing import Any, Dict

import pytest

from snowflake_semantic_tools.shared.config_validator import validate_and_report_config, validate_config
from snowflake_semantic_tools.shared.events import setup_events

# ============================================================================
# Fixtures: Complete Configurations
# ============================================================================


@pytest.fixture
def valid_complete_config() -> Dict[str, Any]:
    """Complete valid configuration with all fields."""
    return {
        "project": {"semantic_models_dir": "snowflake_semantic_models"},
        "validation": {"strict": False, "exclude_dirs": ["_intermediate", "staging"]},
        "enrichment": {
            "distinct_limit": 25,
            "sample_values_display_limit": 10,
            "synonym_model": "mistral-large2",
            "synonym_max_count": 4,
        },
    }


@pytest.fixture
def valid_minimal_config() -> Dict[str, Any]:
    """Minimal valid configuration with only required fields."""
    # Note: dbt_models_dir is no longer required - it's auto-detected from dbt_project.yml
    return {"project": {"semantic_models_dir": "semantic_models"}}


# ============================================================================
# Fixtures: Missing Required Fields
# ============================================================================


@pytest.fixture
def config_missing_semantic_models_dir() -> Dict[str, Any]:
    """Config missing project.semantic_models_dir."""
    # Note: dbt_models_dir is no longer required - auto-detected from dbt_project.yml
    return {"project": {}}


@pytest.fixture
def config_missing_project_section() -> Dict[str, Any]:
    """Config with project section completely missing."""
    return {"validation": {"strict": False}}


@pytest.fixture
def config_empty_project_section() -> Dict[str, Any]:
    """Config with empty project section."""
    return {"project": {}}


# ============================================================================
# Fixtures: Missing Optional Fields
# ============================================================================


@pytest.fixture
def config_missing_validation_strict() -> Dict[str, Any]:
    """Config missing validation.strict."""
    return {
        "project": {"semantic_models_dir": "semantic_models"},
        "validation": {"exclude_dirs": ["_intermediate"]},
    }


@pytest.fixture
def config_missing_validation_exclude_dirs() -> Dict[str, Any]:
    """Config missing validation.exclude_dirs."""
    return {
        "project": {"semantic_models_dir": "semantic_models"},
        "validation": {"strict": False},
    }


@pytest.fixture
def config_missing_enrichment_distinct_limit() -> Dict[str, Any]:
    """Config missing enrichment.distinct_limit."""
    return {
        "project": {"semantic_models_dir": "semantic_models"},
        "enrichment": {"sample_values_display_limit": 10, "synonym_model": "mistral-large2", "synonym_max_count": 4},
    }


@pytest.fixture
def config_missing_enrichment_sample_values_display_limit() -> Dict[str, Any]:
    """Config missing enrichment.sample_values_display_limit."""
    return {
        "project": {"semantic_models_dir": "semantic_models"},
        "enrichment": {"distinct_limit": 25, "synonym_model": "mistral-large2", "synonym_max_count": 4},
    }


@pytest.fixture
def config_missing_enrichment_synonym_model() -> Dict[str, Any]:
    """Config missing enrichment.synonym_model."""
    return {
        "project": {"semantic_models_dir": "semantic_models"},
        "enrichment": {"distinct_limit": 25, "sample_values_display_limit": 10, "synonym_max_count": 4},
    }


@pytest.fixture
def config_missing_enrichment_synonym_max_count() -> Dict[str, Any]:
    """Config missing enrichment.synonym_max_count."""
    return {
        "project": {"semantic_models_dir": "semantic_models"},
        "enrichment": {"distinct_limit": 25, "sample_values_display_limit": 10, "synonym_model": "mistral-large2"},
    }


@pytest.fixture
def config_missing_all_optional_fields() -> Dict[str, Any]:
    """Config missing all optional fields."""
    return {"project": {"semantic_models_dir": "semantic_models"}}


# ============================================================================
# Tests: validate_config - Required Fields
# ============================================================================


class TestValidateConfigRequiredFields:
    """Test validation of required configuration fields."""

    def test_valid_complete_config(self, valid_complete_config):
        """Valid complete config should pass validation."""
        is_valid, missing_required, missing_optional = validate_config(valid_complete_config)
        assert is_valid is True
        assert len(missing_required) == 0
        assert len(missing_optional) == 0

    def test_valid_minimal_config(self, valid_minimal_config):
        """Valid minimal config (only required fields) should pass."""
        is_valid, missing_required, missing_optional = validate_config(valid_minimal_config)
        assert is_valid is True
        assert len(missing_required) == 0
        # Should identify missing optional fields
        assert len(missing_optional) > 0

    def test_missing_semantic_models_dir(self, config_missing_semantic_models_dir):
        """Missing project.semantic_models_dir should fail validation."""
        is_valid, missing_required, _ = validate_config(config_missing_semantic_models_dir)
        assert is_valid is False
        assert "project.semantic_models_dir" in missing_required
        assert len(missing_required) == 1

    def test_missing_required_field(self):
        """Missing required semantic_models_dir should fail validation."""
        # Note: dbt_models_dir is no longer required - auto-detected from dbt_project.yml
        config = {"project": {}}
        is_valid, missing_required, _ = validate_config(config)
        assert is_valid is False
        assert "project.semantic_models_dir" in missing_required
        assert len(missing_required) == 1

    def test_missing_project_section(self, config_missing_project_section):
        """Missing project section entirely should fail validation."""
        is_valid, missing_required, _ = validate_config(config_missing_project_section)
        assert is_valid is False
        assert "project.semantic_models_dir" in missing_required
        # Note: dbt_models_dir is no longer required
        assert len(missing_required) == 1

    def test_empty_project_section(self, config_empty_project_section):
        """Empty project section should fail validation."""
        is_valid, missing_required, _ = validate_config(config_empty_project_section)
        assert is_valid is False
        assert "project.semantic_models_dir" in missing_required
        # Note: dbt_models_dir is no longer required
        assert len(missing_required) == 1


# ============================================================================
# Tests: validate_config - Optional Fields
# ============================================================================


class TestValidateConfigOptionalFields:
    """Test validation of optional configuration fields."""

    def test_missing_validation_strict(self, config_missing_validation_strict):
        """Missing validation.strict should be identified but not fail validation."""
        is_valid, missing_required, missing_optional = validate_config(config_missing_validation_strict)
        assert is_valid is True
        assert len(missing_required) == 0
        optional_fields = [field[0] for field in missing_optional]
        assert "validation.strict" in optional_fields

    def test_missing_validation_exclude_dirs(self, config_missing_validation_exclude_dirs):
        """Missing validation.exclude_dirs should be identified but not fail validation."""
        is_valid, missing_required, missing_optional = validate_config(config_missing_validation_exclude_dirs)
        assert is_valid is True
        assert len(missing_required) == 0
        optional_fields = [field[0] for field in missing_optional]
        assert "validation.exclude_dirs" in optional_fields

    def test_missing_enrichment_distinct_limit(self, config_missing_enrichment_distinct_limit):
        """Missing enrichment.distinct_limit should be identified but not fail validation."""
        is_valid, missing_required, missing_optional = validate_config(config_missing_enrichment_distinct_limit)
        assert is_valid is True
        assert len(missing_required) == 0
        optional_fields = [field[0] for field in missing_optional]
        assert "enrichment.distinct_limit" in optional_fields

    def test_missing_enrichment_sample_values_display_limit(
        self, config_missing_enrichment_sample_values_display_limit
    ):
        """Missing enrichment.sample_values_display_limit should be identified."""
        is_valid, missing_required, missing_optional = validate_config(
            config_missing_enrichment_sample_values_display_limit
        )
        assert is_valid is True
        assert len(missing_required) == 0
        optional_fields = [field[0] for field in missing_optional]
        assert "enrichment.sample_values_display_limit" in optional_fields

    def test_missing_enrichment_synonym_model(self, config_missing_enrichment_synonym_model):
        """Missing enrichment.synonym_model should be identified."""
        is_valid, missing_required, missing_optional = validate_config(config_missing_enrichment_synonym_model)
        assert is_valid is True
        assert len(missing_required) == 0
        optional_fields = [field[0] for field in missing_optional]
        assert "enrichment.synonym_model" in optional_fields

    def test_missing_enrichment_synonym_max_count(self, config_missing_enrichment_synonym_max_count):
        """Missing enrichment.synonym_max_count should be identified."""
        is_valid, missing_required, missing_optional = validate_config(config_missing_enrichment_synonym_max_count)
        assert is_valid is True
        assert len(missing_required) == 0
        optional_fields = [field[0] for field in missing_optional]
        assert "enrichment.synonym_max_count" in optional_fields

    def test_missing_all_optional_fields(self, config_missing_all_optional_fields):
        """Config missing all optional fields should pass but identify all missing."""
        is_valid, missing_required, missing_optional = validate_config(config_missing_all_optional_fields)
        assert is_valid is True
        assert len(missing_required) == 0
        # Should identify all 6 optional fields
        assert len(missing_optional) == 6
        optional_fields = [field[0] for field in missing_optional]
        assert "validation.strict" in optional_fields
        assert "validation.exclude_dirs" in optional_fields
        assert "enrichment.distinct_limit" in optional_fields
        assert "enrichment.sample_values_display_limit" in optional_fields
        assert "enrichment.synonym_model" in optional_fields
        assert "enrichment.synonym_max_count" in optional_fields


# ============================================================================
# Tests: validate_and_report_config - Event Firing
# ============================================================================


class TestValidateAndReportConfig:
    """Test validate_and_report_config event firing and error handling."""

    def setup_method(self):
        """Setup events system for each test."""
        setup_events(verbose=False, show_timestamps=False)

    def test_valid_config_returns_true(self, valid_complete_config):
        """Valid config should return True."""
        result = validate_and_report_config(valid_complete_config, fail_on_errors=False)
        assert result is True

    def test_invalid_config_returns_false_when_not_failing(self, config_missing_semantic_models_dir):
        """Invalid config should return False when fail_on_errors=False."""
        result = validate_and_report_config(config_missing_semantic_models_dir, fail_on_errors=False)
        assert result is False

    def test_invalid_config_raises_systemexit_when_failing(self, config_missing_semantic_models_dir):
        """Invalid config should raise SystemExit when fail_on_errors=True."""
        with pytest.raises(SystemExit):
            validate_and_report_config(config_missing_semantic_models_dir, fail_on_errors=True)

    def test_valid_config_with_missing_optional_returns_true(self, config_missing_all_optional_fields):
        """Config missing only optional fields should return True."""
        result = validate_and_report_config(config_missing_all_optional_fields, fail_on_errors=False)
        assert result is True  # Still valid since required fields are present

    def test_nested_field_handling(self):
        """Test that nested field paths are handled correctly."""
        # Config with deeply nested structure (future-proofing)
        config = {
            "project": {"semantic_models_dir": "semantic_models"},
            "nested": {"deep": {"value": "test"}},  # Not validated, but shouldn't break
        }
        is_valid, missing_required, _ = validate_config(config)
        assert is_valid is True
        assert len(missing_required) == 0


# ============================================================================
# Tests: Edge Cases
# ============================================================================


class TestConfigValidatorEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_config_dict(self):
        """Empty config dict should fail validation."""
        config = {}
        is_valid, missing_required, _ = validate_config(config)
        assert is_valid is False
        # Only semantic_models_dir is required now (dbt_models_dir auto-detected from dbt_project.yml)
        assert len(missing_required) == 1

    def test_none_values(self):
        """Config with None values should fail validation."""
        config = {"project": {"semantic_models_dir": None}}
        is_valid, missing_required, _ = validate_config(config)
        assert is_valid is False
        assert "project.semantic_models_dir" in missing_required

    def test_empty_string_values(self):
        """Config with empty string values should be considered present."""
        # Note: Empty strings are technically present, so validation passes
        # The code checks for key existence, not value truthiness
        config = {"project": {"semantic_models_dir": ""}}
        # Empty strings are present (validation only checks existence, not value)
        is_valid, missing_required, _ = validate_config(config)
        assert is_valid is True  # Keys exist, even if values are empty

    def test_config_path_logging(self, tmp_path, valid_minimal_config):
        """Config path should be used for logging context."""
        config_path = tmp_path / "sst_config.yml"
        is_valid, _, _ = validate_config(valid_minimal_config, config_path=config_path)
        assert is_valid is True  # Should still validate correctly

    def test_non_dict_project_section(self):
        """Config with project as non-dict should fail gracefully."""
        config = {"project": "not_a_dict"}
        is_valid, missing_required, _ = validate_config(config)
        assert is_valid is False
        # Only semantic_models_dir is required now (dbt_models_dir auto-detected)
        assert len(missing_required) == 1  # Can't navigate into non-dict

    def test_extra_unrecognized_fields(self, valid_complete_config):
        """Config with extra unrecognized fields should still validate."""
        config = valid_complete_config.copy()
        config["unknown_section"] = {"unknown_field": "value"}
        is_valid, missing_required, _ = validate_config(config)
        assert is_valid is True  # Unrecognized fields don't affect validation
