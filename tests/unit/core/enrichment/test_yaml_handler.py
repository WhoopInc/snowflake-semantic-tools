"""Unit tests for yaml_handler module."""

import pytest
import tempfile
import os
from pathlib import Path
from snowflake_semantic_tools.core.enrichment.yaml_handler import YAMLHandler


class TestYAMLHandler:
    """Test YAMLHandler class."""
    
    @pytest.fixture
    def yaml_handler(self):
        """Create YAMLHandler instance."""
        return YAMLHandler()
    
    @pytest.fixture
    def temp_yaml_file(self):
        """Create temporary YAML file."""
        fd, path = tempfile.mkstemp(suffix='.yml')
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)
    
    def test_read_yaml_valid_file(self, yaml_handler, temp_yaml_file):
        """Test reading valid YAML file."""
        yaml_content = """
version: 2
models:
  - name: test_model
    description: Test model
    columns:
      - name: user_id
        description: User identifier
"""
        with open(temp_yaml_file, 'w') as f:
            f.write(yaml_content)
        
        result = yaml_handler.read_yaml(temp_yaml_file)
        
        assert result is not None
        assert result['version'] == 2
        assert len(result['models']) == 1
        assert result['models'][0]['name'] == 'test_model'
        assert len(result['models'][0]['columns']) == 1
    
    def test_read_yaml_nonexistent_file(self, yaml_handler):
        """Test reading nonexistent file returns None."""
        result = yaml_handler.read_yaml('/nonexistent/file.yml')
        assert result is None
    
    def test_read_yaml_invalid_yaml(self, yaml_handler, temp_yaml_file):
        """Test reading invalid YAML returns None."""
        with open(temp_yaml_file, 'w') as f:
            f.write("invalid: yaml: content: [")
        
        result = yaml_handler.read_yaml(temp_yaml_file)
        assert result is None
    
    def test_write_yaml_basic(self, yaml_handler, temp_yaml_file):
        """Test writing basic YAML content."""
        content = {
            'version': 2,
            'models': [
                {
                    'name': 'test_model',
                    'description': 'Test model',
                    'columns': [
                        {'name': 'user_id', 'description': 'User ID'}
                    ]
                }
            ]
        }
        
        success = yaml_handler.write_yaml(content, temp_yaml_file)
        
        assert success is True
        assert os.path.exists(temp_yaml_file)
        
        # Read back and verify
        result = yaml_handler.read_yaml(temp_yaml_file)
        assert result == content
    
    def test_write_yaml_with_sst_structure(self, yaml_handler, temp_yaml_file):
        """Test writing YAML with meta.sst structure (legacy test with old data)."""
        content = {
            'version': 2,
            'models': [
                {
                    'name': 'test_model',
                    'meta': {
                        'sst': {
                            'primary_key': ['user_id'],
                            'cortex_searchable': True
                        }
                    },
                    'columns': [
                        {
                            'name': 'user_id',
                            'meta': {
                                'sst': {
                                    'column_type': 'dimension',
                                    'data_type': 'number',
                                    'sample_values': ['1', '2', '3']
                                }
                            }
                        }
                    ]
                }
            ]
        }
        
        success = yaml_handler.write_yaml(content, temp_yaml_file)
        
        assert success is True
        
        # Read back and verify structure
        result = yaml_handler.read_yaml(temp_yaml_file)
        assert result['models'][0]['meta']['sst']['primary_key'] == ['user_id']
        assert result['models'][0]['columns'][0]['meta']['sst']['column_type'] == 'dimension'
    
    def test_ensure_sst_structure_empty_model(self, yaml_handler):
        """Test ensuring SST structure on empty model."""
        model = {}
        
        result = yaml_handler.ensure_sst_structure(model)
        
        assert 'meta' in result
        assert 'sst' in result['meta']
        assert isinstance(result['meta']['sst'], dict)
    
    def test_ensure_sst_structure_existing_meta(self, yaml_handler):
        """Test ensuring SST structure with existing meta."""
        model = {
            'name': 'test_model',
            'meta': {
                'custom_field': 'value'
            }
        }
        
        result = yaml_handler.ensure_sst_structure(model)
        
        assert 'meta' in result
        assert 'sst' in result['meta']
        assert 'custom_field' in result['meta']
        assert result['meta']['custom_field'] == 'value'
    
    def test_ensure_sst_structure_creates_sst(self, yaml_handler):
        """Test ensure_sst_structure creates sst section."""
        model = {
            'name': 'test_model',
            'meta': {
                'sst': {
                    'primary_key': ['id']
                }
            }
        }
        
        result = yaml_handler.ensure_sst_structure(model)
        
        # Should have sst section
        assert 'sst' in result['meta']
        assert result['meta']['sst']['primary_key'] == ['id']
    
    def test_ensure_column_sst_structure_empty_column(self, yaml_handler):
        """Test ensuring sst structure on empty column."""
        column = {}
        
        result = yaml_handler.ensure_column_sst_structure(column)
        
        assert 'meta' in result
        assert 'sst' in result['meta']
        assert isinstance(result['meta']['sst'], dict)
    
    def test_ensure_column_sst_structure_existing_meta(self, yaml_handler):
        """Test ensuring column sst structure with existing meta."""
        column = {
            'name': 'user_id',
            'meta': {
                'custom_field': 'value'
            }
        }
        
        result = yaml_handler.ensure_column_sst_structure(column)
        
        assert 'meta' in result
        assert 'sst' in result['meta']
        assert 'custom_field' in result['meta']
        assert result['meta']['custom_field'] == 'value'
    
    def test_find_yaml_file_for_model_same_directory(self, yaml_handler):
        """Test finding YAML file in same directory as SQL."""
        sql_path = '/path/to/models/test_model/test_model.sql'
        
        result = yaml_handler.find_yaml_file_for_model(sql_path)
        
        assert result == '/path/to/models/test_model/test_model.yml'
    
    def test_find_yaml_file_for_model_nested_path(self, yaml_handler):
        """Test finding YAML file for nested model."""
        sql_path = '/path/to/models/analytics/users/user_activity.sql'
        
        result = yaml_handler.find_yaml_file_for_model(sql_path)
        
        assert result == '/path/to/models/analytics/users/user_activity.yml'
    
    def test_write_yaml_preserves_order(self, yaml_handler, temp_yaml_file):
        """Test that YAML writing preserves key order."""
        content = {
            'version': 2,
            'models': [
                {
                    'name': 'test_model',
                    'description': 'Test description',
                    'meta': {
                        'sst': {
                            'primary_key': ['id'],
                            'cortex_searchable': True
                        }
                    },
                    'columns': [
                        {
                            'name': 'id',
                            'description': 'ID column',
                            'meta': {
                                'sst': {
                                    'column_type': 'dimension',
                                    'data_type': 'number'
                                }
                            }
                        }
                    ]
                }
            ]
        }
        
        yaml_handler.write_yaml(content, temp_yaml_file)
        
        # Read the file as text to check order
        with open(temp_yaml_file, 'r') as f:
            yaml_text = f.read()
        
        # Check that version comes before models
        assert yaml_text.index('version') < yaml_text.index('models')
        
        # Check that name comes before description
        assert yaml_text.index('name:') < yaml_text.index('description:')
    
    def test_write_yaml_handles_none_values(self, yaml_handler, temp_yaml_file):
        """Test that None values are handled correctly."""
        content = {
            'version': 2,
            'models': [
                {
                    'name': 'test_model',
                    'description': None,
                    'meta': {
                        'sst': {
                            'primary_key': None
                        }
                    }
                }
            ]
        }
        
        success = yaml_handler.write_yaml(content, temp_yaml_file)
        
        assert success is True
        
        # Read back
        result = yaml_handler.read_yaml(temp_yaml_file)
        assert result['models'][0]['description'] is None
    
    def test_write_yaml_handles_empty_lists(self, yaml_handler, temp_yaml_file):
        """Test that empty lists are handled correctly."""
        content = {
            'version': 2,
            'models': [
                {
                    'name': 'test_model',
                    'columns': [],
                    'meta': {
                        'sst': {
                            'sample_values': []
                        }
                    }
                }
            ]
        }
        
        success = yaml_handler.write_yaml(content, temp_yaml_file)
        
        assert success is True
        
        # Read back
        result = yaml_handler.read_yaml(temp_yaml_file)
        assert result['models'][0]['columns'] == []
        assert result['models'][0]['meta']['sst']['sample_values'] == []


class TestSemanticModelsSupport:
    """Test support for semantic_models key (dbt MetricFlow format)."""
    
    @pytest.fixture
    def yaml_handler(self):
        """Create YAMLHandler instance."""
        return YAMLHandler()
    
    def test_get_models_list_with_models_key(self, yaml_handler):
        """Test _get_models_list returns models from 'models' key."""
        yaml_content = {
            'models': [
                {'name': 'model1'},
                {'name': 'model2'}
            ]
        }
        result = yaml_handler._get_models_list(yaml_content)
        assert len(result) == 2
        assert result[0]['name'] == 'model1'
        assert result[1]['name'] == 'model2'
    
    def test_get_models_list_with_semantic_models_key(self, yaml_handler):
        """Test _get_models_list returns models from 'semantic_models' key when 'models' missing."""
        yaml_content = {
            'semantic_models': [
                {'name': 'semantic_model1'},
                {'name': 'semantic_model2'}
            ]
        }
        result = yaml_handler._get_models_list(yaml_content)
        assert len(result) == 2
        assert result[0]['name'] == 'semantic_model1'
        assert result[1]['name'] == 'semantic_model2'
    
    def test_get_models_list_prefers_models_over_semantic_models(self, yaml_handler):
        """Test _get_models_list prefers 'models' when both keys present."""
        yaml_content = {
            'models': [{'name': 'from_models'}],
            'semantic_models': [{'name': 'from_semantic'}]
        }
        result = yaml_handler._get_models_list(yaml_content)
        assert len(result) == 1
        assert result[0]['name'] == 'from_models'
    
    def test_get_models_list_empty_content(self, yaml_handler):
        """Test _get_models_list returns empty list for empty content."""
        result = yaml_handler._get_models_list({})
        assert result == []
    
    def test_get_models_list_invalid_type(self, yaml_handler):
        """Test _get_models_list handles non-list models gracefully."""
        yaml_content = {'models': 'not_a_list'}
        result = yaml_handler._get_models_list(yaml_content)
        assert result == []
    
    def test_has_models_with_models_key(self, yaml_handler):
        """Test _has_models returns True when 'models' key exists."""
        yaml_content = {'models': []}
        assert yaml_handler._has_models(yaml_content) is True
    
    def test_has_models_with_semantic_models_key(self, yaml_handler):
        """Test _has_models returns True when 'semantic_models' key exists."""
        yaml_content = {'semantic_models': []}
        assert yaml_handler._has_models(yaml_content) is True
    
    def test_has_models_with_neither_key(self, yaml_handler):
        """Test _has_models returns False when neither key exists."""
        yaml_content = {'version': 2}
        assert yaml_handler._has_models(yaml_content) is False
    
    def test_get_existing_model_metadata_from_semantic_models(self, yaml_handler):
        """Test get_existing_model_metadata works with semantic_models key."""
        yaml_content = {
            'semantic_models': [
                {'name': 'target_model', 'description': 'Found me!'}
            ]
        }
        result = yaml_handler.get_existing_model_metadata(yaml_content, 'target_model')
        assert result is not None
        assert result['name'] == 'target_model'
        assert result['description'] == 'Found me!'
