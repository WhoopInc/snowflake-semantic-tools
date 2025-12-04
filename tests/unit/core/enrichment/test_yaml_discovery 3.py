"""
Unit tests for YAML file discovery in enrichment.

Tests the find_yaml_file_for_model method which searches for existing
YAML files that match a SQL model file.
"""

import pytest
from pathlib import Path
import tempfile
import shutil

from snowflake_semantic_tools.core.enrichment import YAMLHandler


@pytest.fixture
def temp_model_dir():
    """Create a temporary directory with various YAML/SQL configurations."""
    temp_dir = Path(tempfile.mkdtemp())
    
    yield temp_dir
    
    # Cleanup
    shutil.rmtree(temp_dir)


def test_find_yaml_same_name(temp_model_dir):
    """Test finding YAML file with same name as SQL file."""
    # Create model.sql and model.yml
    sql_file = temp_model_dir / "orders.sql"
    yaml_file = temp_model_dir / "orders.yml"
    
    sql_file.write_text("SELECT * FROM orders")
    yaml_file.write_text("version: 2\nmodels:\n  - name: orders")
    
    handler = YAMLHandler()
    found_yaml = handler.find_yaml_file_for_model(str(sql_file))
    
    assert found_yaml == str(yaml_file)


def test_find_yaml_with_yaml_extension(temp_model_dir):
    """Test finding YAML file with .yaml extension."""
    # Create model.sql and model.yaml
    sql_file = temp_model_dir / "orders.sql"
    yaml_file = temp_model_dir / "orders.yaml"
    
    sql_file.write_text("SELECT * FROM orders")
    yaml_file.write_text("version: 2\nmodels:\n  - name: orders")
    
    handler = YAMLHandler()
    found_yaml = handler.find_yaml_file_for_model(str(sql_file))
    
    assert found_yaml == str(yaml_file)


def test_find_yaml_different_name_same_model(temp_model_dir):
    """Test finding YAML file with different name but matching model name."""
    # Create orders.sql but YAML is named differently (e.g., all_models.yml)
    sql_file = temp_model_dir / "orders.sql"
    yaml_file = temp_model_dir / "all_models.yml"
    
    sql_file.write_text("SELECT * FROM orders")
    yaml_file.write_text("version: 2\nmodels:\n  - name: orders\n  - name: customers")
    
    handler = YAMLHandler()
    found_yaml = handler.find_yaml_file_for_model(str(sql_file))
    
    # Should find all_models.yml because it contains 'orders' model
    assert found_yaml == str(yaml_file)


def test_find_yaml_no_existing_file(temp_model_dir):
    """Test when no YAML file exists - should return expected path."""
    # Create only SQL file
    sql_file = temp_model_dir / "orders.sql"
    sql_file.write_text("SELECT * FROM orders")
    
    handler = YAMLHandler()
    found_yaml = handler.find_yaml_file_for_model(str(sql_file))
    
    # Should return expected path (orders.yml) even though it doesn't exist
    expected_yaml = temp_model_dir / "orders.yml"
    assert found_yaml == str(expected_yaml)


def test_find_yaml_multiple_yaml_files(temp_model_dir):
    """Test with multiple YAML files - should find the one with matching model."""
    # Create orders.sql and multiple YAML files
    sql_file = temp_model_dir / "orders.sql"
    yaml_file1 = temp_model_dir / "customers.yml"
    yaml_file2 = temp_model_dir / "all_models.yml"
    
    sql_file.write_text("SELECT * FROM orders")
    yaml_file1.write_text("version: 2\nmodels:\n  - name: customers")
    yaml_file2.write_text("version: 2\nmodels:\n  - name: orders\n  - name: invoices")
    
    handler = YAMLHandler()
    found_yaml = handler.find_yaml_file_for_model(str(sql_file))
    
    # Should find all_models.yml because it contains 'orders' model
    assert found_yaml == str(yaml_file2)


def test_find_yaml_prefers_same_name(temp_model_dir):
    """Test that same-name YAML is preferred over different-name YAML."""
    # Create orders.sql, orders.yml, and all_models.yml (both contain 'orders')
    sql_file = temp_model_dir / "orders.sql"
    yaml_file_same = temp_model_dir / "orders.yml"
    yaml_file_diff = temp_model_dir / "all_models.yml"
    
    sql_file.write_text("SELECT * FROM orders")
    yaml_file_same.write_text("version: 2\nmodels:\n  - name: orders")
    yaml_file_diff.write_text("version: 2\nmodels:\n  - name: orders\n  - name: customers")
    
    handler = YAMLHandler()
    found_yaml = handler.find_yaml_file_for_model(str(sql_file))
    
    # Should prefer orders.yml over all_models.yml
    assert found_yaml == str(yaml_file_same)


def test_yaml_contains_model_true(temp_model_dir):
    """Test _yaml_contains_model returns True when model exists."""
    yaml_file = temp_model_dir / "models.yml"
    yaml_file.write_text("version: 2\nmodels:\n  - name: orders\n  - name: customers")
    
    handler = YAMLHandler()
    result = handler._yaml_contains_model(yaml_file, "orders")
    
    assert result is True


def test_yaml_contains_model_false(temp_model_dir):
    """Test _yaml_contains_model returns False when model doesn't exist."""
    yaml_file = temp_model_dir / "models.yml"
    yaml_file.write_text("version: 2\nmodels:\n  - name: customers")
    
    handler = YAMLHandler()
    result = handler._yaml_contains_model(yaml_file, "orders")
    
    assert result is False


def test_yaml_contains_model_no_models_key(temp_model_dir):
    """Test _yaml_contains_model returns False when YAML has no 'models' key."""
    yaml_file = temp_model_dir / "sources.yml"
    yaml_file.write_text("version: 2\nsources:\n  - name: raw")
    
    handler = YAMLHandler()
    result = handler._yaml_contains_model(yaml_file, "orders")
    
    assert result is False


def test_yaml_contains_model_invalid_yaml(temp_model_dir):
    """Test _yaml_contains_model returns False for invalid YAML."""
    yaml_file = temp_model_dir / "invalid.yml"
    yaml_file.write_text("invalid: yaml: content: [[[")
    
    handler = YAMLHandler()
    result = handler._yaml_contains_model(yaml_file, "orders")
    
    assert result is False


def test_find_yaml_case_sensitive(temp_model_dir):
    """Test that model name matching is case-sensitive."""
    # Create orders.sql and YAML with 'ORDERS' (uppercase)
    sql_file = temp_model_dir / "orders.sql"
    yaml_file = temp_model_dir / "all_models.yml"
    
    sql_file.write_text("SELECT * FROM orders")
    yaml_file.write_text("version: 2\nmodels:\n  - name: ORDERS")
    
    handler = YAMLHandler()
    found_yaml = handler.find_yaml_file_for_model(str(sql_file))
    
    # Should NOT find all_models.yml because 'ORDERS' != 'orders'
    # Should return expected path instead
    expected_yaml = temp_model_dir / "orders.yml"
    assert found_yaml == str(expected_yaml)
