"""
Tests for duplicate detection in semantic models.

This test file specifically addresses the bug where semantic views with
tables stored as JSON strings were incorrectly flagged as having identical
table lists due to character-level comparison instead of proper parsing.
"""

import pytest
import json
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
            'semantic_views': {
                'items': [
                    {
                        'name': 'growth_trials_engagement',
                        'tables': json.dumps([
                            "{{ table('single_customer_view') }}",
                            "{{ table('user_cycle_active_periods') }}"
                        ])
                    },
                    {
                        'name': 'upcycle_lifecycle',
                        'tables': json.dumps([
                            "{{ table('single_customer_view') }}",
                            "{{ table('upgrade_upcycle') }}"
                        ])
                    }
                ]
            }
        }
        
        result = detector.validate(semantic_data)
        
        # Should NOT have warnings about identical table lists
        # (they share single_customer_view but have different second tables)
        identical_warnings = [
            w for w in result.get_warnings() 
            if 'identical table lists' in w.message.lower()
        ]
        
        assert len(identical_warnings) == 0, (
            f"Expected no 'identical table lists' warnings, but got: {identical_warnings}"
        )
    
    def test_semantic_views_with_truly_identical_tables_json_string(self, detector):
        """
        Test that semantic views with truly identical tables (stored as JSON strings)
        ARE correctly flagged as duplicates.
        """
        semantic_data = {
            'semantic_views': {
                'items': [
                    {
                        'name': 'view1',
                        'tables': json.dumps([
                            "{{ table('table_a') }}",
                            "{{ table('table_b') }}"
                        ])
                    },
                    {
                        'name': 'view2',
                        'tables': json.dumps([
                            "{{ table('table_b') }}",  # Same tables, different order
                            "{{ table('table_a') }}"
                        ])
                    }
                ]
            }
        }
        
        result = detector.validate(semantic_data)
        
        # SHOULD have warning about identical table lists (order-independent)
        identical_warnings = [
            w for w in result.get_warnings() 
            if 'identical table lists' in w.message.lower()
        ]
        
        assert len(identical_warnings) == 1, (
            f"Expected 1 'identical table lists' warning, but got {len(identical_warnings)}"
        )
        assert 'view1' in identical_warnings[0].message
        assert 'view2' in identical_warnings[0].message
    
    def test_semantic_views_with_different_tables_list_format(self, detector):
        """
        Test that semantic views with different tables (stored as lists)
        are NOT flagged as duplicates.
        
        This ensures the fix also works when tables are provided as lists directly.
        """
        semantic_data = {
            'semantic_views': {
                'items': [
                    {
                        'name': 'view_a',
                        'tables': [
                            "{{ table('orders') }}",
                            "{{ table('customers') }}"
                        ]
                    },
                    {
                        'name': 'view_b',
                        'tables': [
                            "{{ table('orders') }}",
                            "{{ table('products') }}"
                        ]
                    }
                ]
            }
        }
        
        result = detector.validate(semantic_data)
        
        # Should NOT have warnings about identical table lists
        identical_warnings = [
            w for w in result.get_warnings() 
            if 'identical table lists' in w.message.lower()
        ]
        
        assert len(identical_warnings) == 0, (
            f"Expected no 'identical table lists' warnings, but got: {identical_warnings}"
        )
    
    def test_semantic_views_with_invalid_json_string(self, detector):
        """
        Test that semantic views with invalid JSON strings don't crash
        and are treated as having no tables.
        """
        semantic_data = {
            'semantic_views': {
                'items': [
                    {
                        'name': 'view_with_invalid_json',
                        'tables': "this is not valid JSON ["
                    },
                    {
                        'name': 'view_with_valid_list',
                        'tables': ["{{ table('some_table') }}"]
                    }
                ]
            }
        }
        
        # Should not crash
        result = detector.validate(semantic_data)
        
        # No errors should be raised, just graceful handling
        assert result is not None
    
    def test_empty_semantic_views_no_warnings(self, detector):
        """Test that empty semantic views data produces no warnings."""
        semantic_data = {
            'semantic_views': {
                'items': []
            }
        }
        
        result = detector.validate(semantic_data)
        
        assert result.warning_count == 0
        assert result.error_count == 0
    
    def test_multiple_views_with_some_duplicates(self, detector):
        """
        Test detection with multiple views where only some have identical tables.
        """
        semantic_data = {
            'semantic_views': {
                'items': [
                    {
                        'name': 'unique_view',
                        'tables': json.dumps(["{{ table('unique_table') }}"])
                    },
                    {
                        'name': 'duplicate_view_1',
                        'tables': json.dumps([
                            "{{ table('shared_a') }}",
                            "{{ table('shared_b') }}"
                        ])
                    },
                    {
                        'name': 'duplicate_view_2',
                        'tables': json.dumps([
                            "{{ table('shared_b') }}",  # Same tables, different order
                            "{{ table('shared_a') }}"
                        ])
                    },
                    {
                        'name': 'another_unique_view',
                        'tables': json.dumps(["{{ table('another_unique') }}"])
                    }
                ]
            }
        }
        
        result = detector.validate(semantic_data)
        
        # Should have exactly 1 warning for the duplicate pair
        identical_warnings = [
            w for w in result.get_warnings() 
            if 'identical table lists' in w.message.lower()
        ]
        
        assert len(identical_warnings) == 1
        assert 'duplicate_view_1' in identical_warnings[0].message
        assert 'duplicate_view_2' in identical_warnings[0].message
        assert 'unique_view' not in identical_warnings[0].message
        assert 'another_unique_view' not in identical_warnings[0].message

