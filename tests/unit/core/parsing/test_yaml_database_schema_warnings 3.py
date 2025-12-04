"""
Test YAML Database/Schema Warning Behavior

Tests that database and schema fields in YAML meta.sst are:
1. Completely ignored during extraction
2. Generate warnings to inform users
3. Manifest is the only source for database/schema
"""

import pytest
from pathlib import Path
from unittest.mock import Mock
from snowflake_semantic_tools.core.parsing.parsers.data_extractors import extract_table_info


class TestYAMLDatabaseSchemaWarnings:
    """Test that YAML database/schema fields generate warnings and are ignored."""
    
    def test_yaml_database_generates_warning(self, caplog):
        """Test that database in meta.sst generates a warning."""
        model = {
            'name': 'test_model',
            'description': 'Test model',
            'meta': {
                'sst': {
                    'database': 'ANALYTICS',  # This should be IGNORED
                    'primary_key': ['id']
                }
            }
        }
        
        file_path = Path('test.yml')
        
        # Extract without manifest
        result = extract_table_info(model, file_path, target_database=None, manifest_parser=None)
        
        # Check that warning was logged
        assert any("database in meta.sst - this is IGNORED" in record.message for record in caplog.records)
        
        # Check that database is empty (YAML was ignored)
        assert result['database'] == ''
    
    def test_yaml_schema_generates_warning(self, caplog):
        """Test that schema in meta.sst generates a warning."""
        model = {
            'name': 'test_model',
            'description': 'Test model',
            'meta': {
                'sst': {
                    'schema': 'MEMBERSHIPS',  # This should be IGNORED
                    'primary_key': ['id']
                }
            }
        }
        
        file_path = Path('test.yml')
        
        # Extract without manifest
        result = extract_table_info(model, file_path, target_database=None, manifest_parser=None)
        
        # Check that warning was logged
        assert any("schema in meta.sst - this is IGNORED" in record.message for record in caplog.records)
        
        # Check that schema is empty (YAML was ignored)
        assert result['schema'] == ''
    
    def test_yaml_database_and_schema_both_warned(self, caplog):
        """Test that both database and schema generate separate warnings."""
        model = {
            'name': 'test_model',
            'description': 'Test model',
            'meta': {
                'sst': {
                    'database': 'ANALYTICS',  # This should be IGNORED
                    'schema': 'MEMBERSHIPS',  # This should be IGNORED
                    'primary_key': ['id']
                }
            }
        }
        
        file_path = Path('test.yml')
        
        # Extract without manifest
        result = extract_table_info(model, file_path, target_database=None, manifest_parser=None)
        
        # Check both warnings
        warnings = [record.message for record in caplog.records if record.levelname == 'WARNING']
        assert any("database in meta.sst - this is IGNORED" in msg for msg in warnings)
        assert any("schema in meta.sst - this is IGNORED" in msg for msg in warnings)
        
        # Both should be empty
        assert result['database'] == ''
        assert result['schema'] == ''
    
    def test_yaml_ignored_even_with_manifest(self, caplog):
        """Test that YAML is ignored even when manifest also has values."""
        model = {
            'name': 'test_model',
            'description': 'Test model',
            'meta': {
                'sst': {
                    'database': 'WRONG_DATABASE',  # Should be IGNORED
                    'schema': 'WRONG_SCHEMA',      # Should be IGNORED
                    'primary_key': ['id']
                }
            }
        }
        
        file_path = Path('test.yml')
        
        # Mock manifest parser
        mock_manifest = Mock()
        mock_manifest.manifest = True
        mock_manifest.get_location.return_value = {
            'database': 'CORRECT_DATABASE',
            'schema': 'CORRECT_SCHEMA'
        }
        
        # Extract with manifest
        result = extract_table_info(model, file_path, target_database=None, manifest_parser=mock_manifest)
        
        # Check warnings were logged
        warnings = [record.message for record in caplog.records if record.levelname == 'WARNING']
        assert any("database in meta.sst - this is IGNORED" in msg for msg in warnings)
        assert any("schema in meta.sst - this is IGNORED" in msg for msg in warnings)
        
        # Should use manifest values, not YAML
        assert result['database'] == 'CORRECT_DATABASE'
        assert result['schema'] == 'CORRECT_SCHEMA'
    
    def test_no_warning_when_yaml_omits_database_schema(self, caplog):
        """Test that no warnings are generated when YAML correctly omits database/schema."""
        model = {
            'name': 'test_model',
            'description': 'Test model',
            'meta': {
                'sst': {
                    # Correctly omits database and schema
                    'primary_key': ['id']
                }
            }
        }
        
        file_path = Path('test.yml')
        
        # Mock manifest parser
        mock_manifest = Mock()
        mock_manifest.manifest = True
        mock_manifest.get_location.return_value = {
            'database': 'ANALYTICS',
            'schema': 'MEMBERSHIPS'
        }
        
        # Extract with manifest
        result = extract_table_info(model, file_path, target_database=None, manifest_parser=mock_manifest)
        
        # Should have NO warnings about database/schema
        warnings = [record.message for record in caplog.records if record.levelname == 'WARNING']
        assert not any("database in meta.sst - this is IGNORED" in msg for msg in warnings)
        assert not any("schema in meta.sst - this is IGNORED" in msg for msg in warnings)
        
        # Should use manifest values
        assert result['database'] == 'ANALYTICS'
        assert result['schema'] == 'MEMBERSHIPS'
    
    def test_target_database_overrides_manifest(self, caplog):
        """Test that target_database (defer mechanism) works correctly."""
        model = {
            'name': 'test_model',
            'description': 'Test model',
            'meta': {
                'sst': {
                    'database': 'WRONG_DATABASE',  # Should be IGNORED
                    'primary_key': ['id']
                }
            }
        }
        
        file_path = Path('test.yml')
        
        # Mock manifest parser
        mock_manifest = Mock()
        mock_manifest.manifest = True
        mock_manifest.get_location.return_value = {
            'database': 'ANALYTICS',
            'schema': 'MEMBERSHIPS'
        }
        
        # Extract with target_database (defer mechanism)
        result = extract_table_info(
            model, 
            file_path, 
            target_database='SCRATCH',  # Override for dev environment
            manifest_parser=mock_manifest
        )
        
        # Warning about YAML database
        assert any("database in meta.sst - this is IGNORED" in record.message for record in caplog.records)
        
        # Database should be from target_database (defer mechanism)
        assert result['database'] == 'SCRATCH'
        # Schema should still be from manifest (no override for schema)
        assert result['schema'] == 'MEMBERSHIPS'


class TestManifestOnlyBehavior:
    """Test that manifest is the ONLY source for database/schema."""
    
    def test_without_manifest_and_without_yaml_empty_strings(self):
        """Test that without manifest or YAML, database/schema are empty."""
        model = {
            'name': 'test_model',
            'description': 'Test model',
            'meta': {
                'sst': {
                    'primary_key': ['id']
                }
            }
        }
        
        file_path = Path('test.yml')
        
        # Extract without manifest or target_database
        result = extract_table_info(model, file_path, target_database=None, manifest_parser=None)
        
        assert result['database'] == ''
        assert result['schema'] == ''
    
    def test_manifest_is_authoritative_source(self):
        """Test that manifest values are used as-is, never modified."""
        model = {
            'name': 'test_model',
            'description': 'Test model',
            'meta': {
                'sst': {
                    'primary_key': ['id']
                }
            }
        }
        
        file_path = Path('test.yml')
        
        # Mock manifest with specific values
        mock_manifest = Mock()
        mock_manifest.manifest = True
        mock_manifest.get_location.return_value = {
            'database': 'MY_CUSTOM_DATABASE',
            'schema': 'my_custom_schema'  # lowercase
        }
        
        result = extract_table_info(model, file_path, target_database=None, manifest_parser=mock_manifest)
        
        # Should use manifest values exactly (uppercased by manifest parser)
        assert result['database'] == 'MY_CUSTOM_DATABASE'
        assert result['schema'] == 'MY_CUSTOM_SCHEMA'
    
    def test_manifest_not_found_for_model_empty_strings(self):
        """Test that if model not in manifest, database/schema are empty."""
        model = {
            'name': 'missing_model',
            'description': 'Model not in manifest',
            'meta': {
                'sst': {
                    'primary_key': ['id']
                }
            }
        }
        
        file_path = Path('test.yml')
        
        # Mock manifest that doesn't have this model
        mock_manifest = Mock()
        mock_manifest.manifest = True
        mock_manifest.get_location.return_value = None  # Not found
        
        result = extract_table_info(model, file_path, target_database=None, manifest_parser=mock_manifest)
        
        # Should be empty when model not in manifest
        assert result['database'] == ''
        assert result['schema'] == ''

