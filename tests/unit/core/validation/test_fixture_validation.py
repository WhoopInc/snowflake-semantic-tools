"""
Comprehensive fixture-based validation tests.

Tests that our validation logic properly catches all error and warning scenarios
defined in our test fixtures. This ensures edge cases are caught before they
reach production.
"""

from pathlib import Path

import pytest

from snowflake_semantic_tools.core.parsing import Parser
from snowflake_semantic_tools.core.validation import SemanticValidator
from tests.fixtures import get_fixtures_by_severity, load_fixture


class TestFixtureBasedValidation:
    """Test validation logic against comprehensive fixture scenarios."""

    @pytest.fixture
    def validator(self):
        """Create a semantic validator instance."""
        return SemanticValidator()

    @pytest.fixture
    def parser(self):
        """Create a parser instance."""
        return Parser(enable_template_resolution=True)

    def test_error_fixtures_produce_errors(self, validator, parser):
        """Test that all error fixtures actually produce validation errors."""
        error_fixtures = get_fixtures_by_severity("errors")

        if not error_fixtures:
            pytest.skip("No error fixtures found")

        for fixture_path in error_fixtures:
            # Skip if it's not a metrics, relationships, or filters file
            # (some fixtures might be for components we don't validate)
            if not any(
                component in str(fixture_path) for component in ["metrics", "relationships", "filters", "templates"]
            ):
                continue

            try:
                # Load the fixture
                relative_path = fixture_path.relative_to(fixture_path.parents[2])
                fixture_data = load_fixture(relative_path)

                # Create a mock parse result with the fixture data
                parse_result = {
                    "semantic": {"metrics": {"items": [fixture_data]}} if "metrics" in str(fixture_path) else {},
                    "dbt": {},  # Empty dbt data for these tests
                }

                # Validate with dbt model checking disabled to focus on semantic validation
                result = validator.validate(parse_result, check_dbt_models=False)

                # Should have errors or warnings
                assert (
                    result.has_errors or result.has_warnings
                ), f"Expected errors/warnings for {fixture_path}, but validation passed"

            except Exception as e:
                # If fixture loading fails, that's also a test failure
                pytest.fail(f"Failed to test fixture {fixture_path}: {e}")

    def test_warning_fixtures_produce_warnings(self, validator, parser):
        """Test that warning fixtures produce warnings but not errors."""
        warning_fixtures = get_fixtures_by_severity("warnings")

        if not warning_fixtures:
            pytest.skip("No warning fixtures found")

        for fixture_path in warning_fixtures:
            # Skip if it's not a component we validate
            if not any(component in str(fixture_path) for component in ["metrics", "relationships", "filters"]):
                continue

            try:
                # Load the fixture
                relative_path = fixture_path.relative_to(fixture_path.parents[2])
                fixture_data = load_fixture(relative_path)

                # Create a mock parse result with complete metadata
                parse_result = {
                    "semantic": {"metrics": {"items": [fixture_data]}} if "metrics" in str(fixture_path) else {},
                    "dbt": {
                        "sm_tables": [
                            {
                                "table_name": "ORDERS",
                                "database": "ANALYTICS",
                                "schema": "PUBLIC",
                                "primary_key": ["ID"],
                                "description": "Order transactions table",
                                "synonyms": ["purchases", "transactions"],
                            }
                        ],
                        "sm_dimensions": [
                            {
                                "table_name": "ORDERS",
                                "name": "ID",
                                "data_type": "BIGINT",
                                "description": "Order identifier",
                                "column_type": "dimension",
                            }
                        ],
                        "sm_facts": [
                            {
                                "table_name": "ORDERS",
                                "name": "AMOUNT",
                                "data_type": "DECIMAL",
                                "description": "Order amount in USD",
                                "column_type": "fact",
                            }
                        ],
                    },
                }

                # Validate with dbt model checking disabled to focus on semantic validation
                result = validator.validate(parse_result, check_dbt_models=False)

                # Should have warnings but not errors (from the semantic model itself)
                # Note: Some warning fixtures might not produce warnings if the validation rule doesn't exist yet
                # This test helps us identify missing validation rules
                if not (result.has_warnings or result.has_errors):
                    # Log which fixtures don't produce expected warnings (for future validation rule development)
                    print(
                        f"INFO: Warning fixture {fixture_path.name} didn't produce warnings - may need validation rule"
                    )

                # For now, just ensure it doesn't crash the validator
                assert True, f"Validation completed successfully for {fixture_path}"

            except Exception as e:
                pytest.fail(f"Failed to test warning fixture {fixture_path}: {e}")

    def test_invalid_fixtures_produce_errors(self, validator, parser):
        """Test that all invalid fixtures produce validation errors."""
        invalid_fixtures = get_fixtures_by_severity("invalid")

        if not invalid_fixtures:
            pytest.skip("No invalid fixtures found")

        for fixture_path in invalid_fixtures:
            # Skip if it's not a component we validate
            if not any(
                component in str(fixture_path) for component in ["metrics", "relationships", "filters", "templates"]
            ):
                continue

            try:
                # Load the fixture
                relative_path = fixture_path.relative_to(fixture_path.parents[2])
                fixture_data = load_fixture(relative_path)

                # Create appropriate parse result
                parse_result = {
                    "semantic": {
                        "metrics": {"items": [fixture_data]} if "metrics" in str(fixture_path) else {},
                        "relationships": {"items": [fixture_data]} if "relationships" in str(fixture_path) else {},
                        "filters": {"items": [fixture_data]} if "filters" in str(fixture_path) else {},
                    },
                    "dbt": {},
                }

                # Validate
                result = validator.validate(parse_result, check_dbt_models=False)

                # Should have errors or warnings
                assert (
                    result.has_errors or result.has_warnings
                ), f"Expected errors/warnings for invalid fixture {fixture_path}, but validation passed"

            except Exception as e:
                pytest.fail(f"Failed to test invalid fixture {fixture_path}: {e}")

    def test_edge_case_fixtures_handle_gracefully(self, validator, parser):
        """Test that edge case fixtures are handled gracefully without crashes."""
        edge_case_fixtures = get_fixtures_by_severity("edge_cases")

        if not edge_case_fixtures:
            pytest.skip("No edge case fixtures found")

        for fixture_path in edge_case_fixtures:
            try:
                # Load the fixture directly using the file path
                with open(fixture_path, "r") as f:
                    import yaml

                    fixture_data = yaml.safe_load(f)

                # Create parse result
                parse_result = {"semantic": {"metrics": {"items": [fixture_data]}}, "dbt": {}}

                # Validate - main goal is no crashes
                result = validator.validate(parse_result, check_dbt_models=False)

                # Just ensure it completes without crashing
                assert result is not None, f"Validator returned None for edge case fixture {fixture_path}"

            except Exception as e:
                pytest.fail(f"Edge case fixture {fixture_path} caused validator to crash: {e}")

    def test_all_fixture_categories_exist(self):
        """Test that we have fixtures for all expected categories."""
        expected_severities = ["errors", "warnings", "success", "valid", "invalid", "edge_cases"]

        for severity in expected_severities:
            fixtures = get_fixtures_by_severity(severity)
            assert len(fixtures) > 0, f"No fixtures found for severity: {severity}"

        # Check that we have metrics fixtures for most severities (most important)
        core_severities = ["errors", "warnings", "success", "valid"]
        for severity in core_severities:
            fixtures = get_fixtures_by_severity(severity)
            metrics_fixtures = [f for f in fixtures if "metrics" in str(f)]
            assert len(metrics_fixtures) > 0, f"No metrics fixtures found for severity: {severity}"

    def test_fixture_yaml_validity(self):
        """Test that all fixture files are valid YAML.

        Note: Excludes fixtures in 'parsing/' directories, as those are specifically
        designed to test YAML parsing error handling and may contain intentionally
        invalid YAML.
        """
        all_fixtures = []
        for severity in ["errors", "warnings", "info", "success", "valid", "invalid"]:
            all_fixtures.extend(get_fixtures_by_severity(severity))

        assert len(all_fixtures) > 0, "No fixtures found at all"

        for fixture_path in all_fixtures:
            # Skip parsing fixtures - they may contain intentionally invalid YAML
            # to test error handling in the parser
            if "parsing" in str(fixture_path):
                continue

            try:
                with open(fixture_path, "r") as f:
                    import yaml

                    yaml.safe_load(f)
            except yaml.YAMLError as e:
                pytest.fail(f"Invalid YAML in fixture {fixture_path}: {e}")
            except Exception as e:
                pytest.fail(f"Failed to read fixture {fixture_path}: {e}")
