"""
Tests for duplicate detection in semantic models.

This test file specifically addresses the bug where semantic views with
tables stored as JSON strings were incorrectly flagged as having identical
table lists due to character-level comparison instead of proper parsing.

Also tests detection of duplicate TABLE-LEVEL synonyms within semantic views,
which is a Snowflake constraint (GitHub issue #84).
"""

import json

import pytest

from snowflake_semantic_tools.core.validation.rules.duplicates import DuplicateValidator


class TestSemanticViewDuplicateDetection:
    """Test duplicate detection for semantic views."""

    @pytest.fixture
    def detector(self):
        """Create a duplicate detector instance."""
        return DuplicateValidator()

    def test_semantic_views_with_different_tables_json_string(self, detector):
        """
        Test that semantic views with different tables (stored as JSON strings)
        are NOT flagged as duplicates.

        This tests the bug fix where tables stored as JSON strings were being
        compared character-by-character instead of being parsed first.
        """
        # Simulate views with tables stored as JSON strings (as parser does)
        semantic_data = {
            "semantic_views": {
                "items": [
                    {
                        "name": "growth_trials_engagement",
                        "tables": json.dumps(
                            ["{{ table('single_customer_view') }}", "{{ table('user_cycle_active_periods') }}"]
                        ),
                    },
                    {
                        "name": "upcycle_lifecycle",
                        "tables": json.dumps(["{{ table('single_customer_view') }}", "{{ table('upgrade_upcycle') }}"]),
                    },
                ]
            }
        }

        result = detector.validate(semantic_data)

        # Should NOT have warnings about identical table lists
        # (they share single_customer_view but have different second tables)
        identical_warnings = [w for w in result.get_warnings() if "identical table lists" in w.message.lower()]

        assert (
            len(identical_warnings) == 0
        ), f"Expected no 'identical table lists' warnings, but got: {identical_warnings}"

    def test_semantic_views_with_truly_identical_tables_json_string(self, detector):
        """
        Test that semantic views with truly identical tables (stored as JSON strings)
        ARE correctly flagged as duplicates.
        """
        semantic_data = {
            "semantic_views": {
                "items": [
                    {"name": "view1", "tables": json.dumps(["{{ table('table_a') }}", "{{ table('table_b') }}"])},
                    {
                        "name": "view2",
                        "tables": json.dumps(
                            ["{{ table('table_b') }}", "{{ table('table_a') }}"]  # Same tables, different order
                        ),
                    },
                ]
            }
        }

        result = detector.validate(semantic_data)

        # SHOULD have warning about identical table lists (order-independent)
        identical_warnings = [w for w in result.get_warnings() if "identical table lists" in w.message.lower()]

        assert (
            len(identical_warnings) == 1
        ), f"Expected 1 'identical table lists' warning, but got {len(identical_warnings)}"
        assert "view1" in identical_warnings[0].message
        assert "view2" in identical_warnings[0].message

    def test_semantic_views_with_different_tables_list_format(self, detector):
        """
        Test that semantic views with different tables (stored as lists)
        are NOT flagged as duplicates.

        This ensures the fix also works when tables are provided as lists directly.
        """
        semantic_data = {
            "semantic_views": {
                "items": [
                    {"name": "view_a", "tables": ["{{ table('orders') }}", "{{ table('customers') }}"]},
                    {"name": "view_b", "tables": ["{{ table('orders') }}", "{{ table('products') }}"]},
                ]
            }
        }

        result = detector.validate(semantic_data)

        # Should NOT have warnings about identical table lists
        identical_warnings = [w for w in result.get_warnings() if "identical table lists" in w.message.lower()]

        assert (
            len(identical_warnings) == 0
        ), f"Expected no 'identical table lists' warnings, but got: {identical_warnings}"

    def test_semantic_views_with_invalid_json_string(self, detector):
        """
        Test that semantic views with invalid JSON strings don't crash
        and are treated as having no tables.
        """
        semantic_data = {
            "semantic_views": {
                "items": [
                    {"name": "view_with_invalid_json", "tables": "this is not valid JSON ["},
                    {"name": "view_with_valid_list", "tables": ["{{ table('some_table') }}"]},
                ]
            }
        }

        # Should not crash
        result = detector.validate(semantic_data)

        # No errors should be raised, just graceful handling
        assert result is not None

    def test_empty_semantic_views_no_warnings(self, detector):
        """Test that empty semantic views data produces no warnings."""
        semantic_data = {"semantic_views": {"items": []}}

        result = detector.validate(semantic_data)

        assert result.warning_count == 0
        assert result.error_count == 0

    def test_multiple_views_with_some_duplicates(self, detector):
        """
        Test detection with multiple views where only some have identical tables.
        """
        semantic_data = {
            "semantic_views": {
                "items": [
                    {"name": "unique_view", "tables": json.dumps(["{{ table('unique_table') }}"])},
                    {
                        "name": "duplicate_view_1",
                        "tables": json.dumps(["{{ table('shared_a') }}", "{{ table('shared_b') }}"]),
                    },
                    {
                        "name": "duplicate_view_2",
                        "tables": json.dumps(
                            ["{{ table('shared_b') }}", "{{ table('shared_a') }}"]  # Same tables, different order
                        ),
                    },
                    {"name": "another_unique_view", "tables": json.dumps(["{{ table('another_unique') }}"])},
                ]
            }
        }

        result = detector.validate(semantic_data)

        # Should have exactly 1 warning for the duplicate pair
        identical_warnings = [w for w in result.get_warnings() if "identical table lists" in w.message.lower()]

        assert len(identical_warnings) == 1
        assert "duplicate_view_1" in identical_warnings[0].message
        assert "duplicate_view_2" in identical_warnings[0].message
        assert "unique_view" not in identical_warnings[0].message
        assert "another_unique_view" not in identical_warnings[0].message


class TestTableSynonymDuplicateDetection:
    """
    Test detection of duplicate TABLE-LEVEL synonyms within semantic views.

    Snowflake requires that table synonyms are unique within a semantic view.
    This tests the validation that catches such duplicates before deployment.

    Note: Column synonyms are intentionally NOT checked - they can duplicate
    across tables because the same column concept may exist in multiple tables.
    """

    @pytest.fixture
    def detector(self):
        """Create a duplicate detector instance."""
        return DuplicateValidator()

    def test_duplicate_table_synonyms_in_same_view_detected(self, detector):
        """
        Test that duplicate table synonyms within a semantic view are detected.

        This is the primary issue from GitHub #84: when 'orders' and 'order_items'
        both have synonym 'order details', the semantic view generation fails.
        """
        semantic_data = {
            "semantic_views": {
                "items": [
                    {
                        "name": "sales_analytics",
                        "tables": ["ORDERS", "ORDER_ITEMS"],
                    },
                ]
            }
        }

        dbt_data = {
            "sm_tables": [
                {
                    "table_name": "ORDERS",
                    "synonyms": ["order details", "purchase transactions"],
                    "source_file": "models/orders.yml",
                },
                {
                    "table_name": "ORDER_ITEMS",
                    "synonyms": ["order details", "line items"],  # Duplicate!
                    "source_file": "models/order_items.yml",
                },
            ]
        }

        result = detector.validate(semantic_data, dbt_data)

        # Should have error for duplicate synonym
        errors = result.get_errors()
        duplicate_errors = [e for e in errors if "duplicate table synonym" in e.message.lower()]

        assert len(duplicate_errors) == 1, f"Expected 1 duplicate synonym error, got {len(duplicate_errors)}"
        assert "order details" in duplicate_errors[0].message.lower()
        assert "sales_analytics" in duplicate_errors[0].message

    def test_same_synonym_in_different_views_ok(self, detector):
        """
        Test that the same synonym in different semantic views is OK.

        Synonyms only need to be unique within a single view.
        """
        semantic_data = {
            "semantic_views": {
                "items": [
                    {"name": "view_a", "tables": ["ORDERS"]},
                    {"name": "view_b", "tables": ["ORDER_ITEMS"]},
                ]
            }
        }

        dbt_data = {
            "sm_tables": [
                {
                    "table_name": "ORDERS",
                    "synonyms": ["order details"],
                    "source_file": "models/orders.yml",
                },
                {
                    "table_name": "ORDER_ITEMS",
                    "synonyms": ["order details"],  # Same synonym but different view
                    "source_file": "models/order_items.yml",
                },
            ]
        }

        result = detector.validate(semantic_data, dbt_data)

        # Should NOT have error - different views
        errors = result.get_errors()
        duplicate_errors = [e for e in errors if "duplicate table synonym" in e.message.lower()]

        assert len(duplicate_errors) == 0, f"Expected no duplicate errors, got: {duplicate_errors}"

    def test_unique_table_synonyms_no_error(self, detector):
        """Test that unique table synonyms produce no errors."""
        semantic_data = {
            "semantic_views": {
                "items": [
                    {"name": "analytics", "tables": ["CUSTOMERS", "ORDERS"]},
                ]
            }
        }

        dbt_data = {
            "sm_tables": [
                {
                    "table_name": "CUSTOMERS",
                    "synonyms": ["customer master data", "client information"],
                    "source_file": "models/customers.yml",
                },
                {
                    "table_name": "ORDERS",
                    "synonyms": ["purchase history", "transaction records"],
                    "source_file": "models/orders.yml",
                },
            ]
        }

        result = detector.validate(semantic_data, dbt_data)

        errors = result.get_errors()
        duplicate_errors = [e for e in errors if "duplicate table synonym" in e.message.lower()]

        assert len(duplicate_errors) == 0

    def test_case_insensitive_duplicate_detection(self, detector):
        """Test that synonym comparison is case-insensitive."""
        semantic_data = {
            "semantic_views": {
                "items": [
                    {"name": "view", "tables": ["TABLE_A", "TABLE_B"]},
                ]
            }
        }

        dbt_data = {
            "sm_tables": [
                {
                    "table_name": "TABLE_A",
                    "synonyms": ["Order Details"],  # Title case
                    "source_file": "a.yml",
                },
                {
                    "table_name": "TABLE_B",
                    "synonyms": ["order details"],  # Lowercase - should be duplicate
                    "source_file": "b.yml",
                },
            ]
        }

        result = detector.validate(semantic_data, dbt_data)

        errors = result.get_errors()
        duplicate_errors = [e for e in errors if "duplicate table synonym" in e.message.lower()]

        assert len(duplicate_errors) == 1, "Case-insensitive duplicates should be detected"

    def test_tables_as_json_string(self, detector):
        """Test that tables stored as JSON strings are handled correctly."""
        semantic_data = {
            "semantic_views": {
                "items": [
                    {
                        "name": "view",
                        "tables": json.dumps(["ORDERS", "ORDER_ITEMS"]),  # JSON string
                    },
                ]
            }
        }

        dbt_data = {
            "sm_tables": [
                {
                    "table_name": "ORDERS",
                    "synonyms": ["sales data"],
                    "source_file": "orders.yml",
                },
                {
                    "table_name": "ORDER_ITEMS",
                    "synonyms": ["sales data"],  # Duplicate
                    "source_file": "order_items.yml",
                },
            ]
        }

        result = detector.validate(semantic_data, dbt_data)

        errors = result.get_errors()
        duplicate_errors = [e for e in errors if "duplicate table synonym" in e.message.lower()]

        assert len(duplicate_errors) == 1

    def test_empty_synonyms_no_error(self, detector):
        """Test that tables with no synonyms don't cause errors."""
        semantic_data = {
            "semantic_views": {
                "items": [
                    {"name": "view", "tables": ["TABLE_A", "TABLE_B"]},
                ]
            }
        }

        dbt_data = {
            "sm_tables": [
                {"table_name": "TABLE_A", "synonyms": [], "source_file": "a.yml"},
                {"table_name": "TABLE_B", "synonyms": [], "source_file": "b.yml"},
            ]
        }

        result = detector.validate(semantic_data, dbt_data)

        errors = result.get_errors()
        duplicate_errors = [e for e in errors if "duplicate table synonym" in e.message.lower()]

        assert len(duplicate_errors) == 0

    def test_no_dbt_data_no_error(self, detector):
        """Test that missing dbt_data doesn't cause errors."""
        semantic_data = {
            "semantic_views": {
                "items": [
                    {"name": "view", "tables": ["TABLE_A"]},
                ]
            }
        }

        # No dbt_data provided
        result = detector.validate(semantic_data, None)

        # Should complete without error
        assert result is not None

    def test_multiple_duplicate_synonyms_all_reported(self, detector):
        """Test that multiple different duplicate synonyms are all reported."""
        semantic_data = {
            "semantic_views": {
                "items": [
                    {"name": "view", "tables": ["A", "B", "C"]},
                ]
            }
        }

        dbt_data = {
            "sm_tables": [
                {"table_name": "A", "synonyms": ["dup1", "dup2"], "source_file": "a.yml"},
                {"table_name": "B", "synonyms": ["dup1", "unique"], "source_file": "b.yml"},
                {"table_name": "C", "synonyms": ["dup2", "another"], "source_file": "c.yml"},
            ]
        }

        result = detector.validate(semantic_data, dbt_data)

        errors = result.get_errors()
        duplicate_errors = [e for e in errors if "duplicate table synonym" in e.message.lower()]

        # Should have 2 errors: one for "dup1" and one for "dup2"
        assert len(duplicate_errors) == 2
