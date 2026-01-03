"""
Unit tests for enrichment model discovery.

Tests the _discover_models method with different input formats:
- Direct SQL file path
- Direct YAML file path
- Directory path
- Glob patterns
"""

import shutil
import tempfile
from pathlib import Path

import pytest

from snowflake_semantic_tools.services.enrich_metadata import EnrichmentConfig, MetadataEnrichmentService


@pytest.fixture
def temp_models_dir():
    """Create a temporary models directory structure for testing."""
    temp_dir = Path(tempfile.mkdtemp())

    # Create directory structure
    models_dir = temp_dir / "models"
    models_dir.mkdir()

    subdirs = [
        models_dir / "analytics",
        models_dir / "analytics" / "accounting",
        models_dir / "analytics" / "marketing",
        models_dir / "analytics" / "_intermediate",
    ]

    for subdir in subdirs:
        subdir.mkdir(parents=True, exist_ok=True)

    # Create SQL and YAML files
    files = [
        ("models/analytics/accounting/orders.sql", "SELECT * FROM orders"),
        ("models/analytics/accounting/orders.yml", "version: 2\nmodels:\n  - name: orders"),
        ("models/analytics/accounting/invoices.sql", "SELECT * FROM invoices"),
        ("models/analytics/accounting/invoices.yml", "version: 2\nmodels:\n  - name: invoices"),
        ("models/analytics/marketing/campaigns.sql", "SELECT * FROM campaigns"),
        ("models/analytics/marketing/campaigns.yml", "version: 2\nmodels:\n  - name: campaigns"),
        ("models/analytics/_intermediate/staging.sql", "SELECT * FROM staging"),
        ("models/analytics/_intermediate/staging.yml", "version: 2\nmodels:\n  - name: staging"),
    ]

    for file_path, content in files:
        full_path = temp_dir / file_path
        with open(full_path, "w") as f:
            f.write(content)

    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir)


def test_discover_single_sql_file(temp_models_dir):
    """Test discovery with direct SQL file path."""
    sql_file = temp_models_dir / "models/analytics/accounting/orders.sql"

    config = EnrichmentConfig(target_path=str(sql_file), database="ANALYTICS", schema="accounting")

    service = MetadataEnrichmentService(config)
    models = service._discover_models()

    assert len(models) == 1
    assert models[0] == str(sql_file)


def test_discover_single_yaml_file(temp_models_dir):
    """Test discovery with direct YAML file path (finds corresponding SQL)."""
    yaml_file = temp_models_dir / "models/analytics/accounting/orders.yml"
    sql_file = temp_models_dir / "models/analytics/accounting/orders.sql"

    config = EnrichmentConfig(target_path=str(yaml_file), database="ANALYTICS", schema="accounting")

    service = MetadataEnrichmentService(config)
    models = service._discover_models()

    assert len(models) == 1
    assert models[0] == str(sql_file)


def test_discover_yaml_without_sql(temp_models_dir):
    """Test discovery with YAML file that has no corresponding SQL file."""
    # Create a YAML file without SQL
    orphan_yaml = temp_models_dir / "models/analytics/accounting/orphan.yml"
    with open(orphan_yaml, "w") as f:
        f.write("version: 2\nmodels:\n  - name: orphan")

    config = EnrichmentConfig(target_path=str(orphan_yaml), database="ANALYTICS", schema="accounting")

    service = MetadataEnrichmentService(config)
    models = service._discover_models()

    # Should return empty list with a warning
    assert len(models) == 0


def test_discover_directory(temp_models_dir):
    """Test discovery with directory path (finds all SQL files recursively)."""
    accounting_dir = temp_models_dir / "models/analytics/accounting"

    config = EnrichmentConfig(target_path=str(accounting_dir), database="ANALYTICS", schema="accounting")

    service = MetadataEnrichmentService(config)
    models = service._discover_models()

    # Should find orders.sql and invoices.sql
    assert len(models) == 2
    assert any("orders.sql" in m for m in models)
    assert any("invoices.sql" in m for m in models)


def test_discover_directory_with_exclusions(temp_models_dir):
    """Test discovery with directory path and excluded directories."""
    analytics_dir = temp_models_dir / "models/analytics"

    config = EnrichmentConfig(
        target_path=str(analytics_dir), database="ANALYTICS", schema="analytics", excluded_dirs=["_intermediate"]
    )

    service = MetadataEnrichmentService(config)
    models = service._discover_models()

    # Should find 3 files (orders, invoices, campaigns) but NOT staging
    assert len(models) == 3
    assert not any("staging.sql" in m for m in models)
    assert any("orders.sql" in m for m in models)
    assert any("invoices.sql" in m for m in models)
    assert any("campaigns.sql" in m for m in models)


def test_discover_root_models_directory(temp_models_dir):
    """Test discovery from root models/ directory."""
    models_dir = temp_models_dir / "models"

    config = EnrichmentConfig(target_path=str(models_dir), database="ANALYTICS", schema="analytics")

    service = MetadataEnrichmentService(config)
    models = service._discover_models()

    # Should find all 4 SQL files
    assert len(models) == 4


def test_discover_nonexistent_file(temp_models_dir):
    """Test discovery with nonexistent file path."""
    nonexistent = temp_models_dir / "models/nonexistent.sql"

    config = EnrichmentConfig(target_path=str(nonexistent), database="ANALYTICS", schema="analytics")

    service = MetadataEnrichmentService(config)
    models = service._discover_models()

    # Should return empty list
    assert len(models) == 0


def test_discover_models_sorted(temp_models_dir):
    """Test that discovered models are returned in sorted order."""
    models_dir = temp_models_dir / "models/analytics"

    config = EnrichmentConfig(target_path=str(models_dir), database="ANALYTICS", schema="analytics")

    service = MetadataEnrichmentService(config)
    models = service._discover_models()

    # Check that models are sorted
    assert models == sorted(models)


def test_discover_yaml_and_yaml_extensions(temp_models_dir):
    """Test that both .yml and .yaml extensions work."""
    # Create a .yaml file
    yaml_file = temp_models_dir / "models/analytics/accounting/test.yaml"
    sql_file = temp_models_dir / "models/analytics/accounting/test.sql"

    with open(yaml_file, "w") as f:
        f.write("version: 2\nmodels:\n  - name: test")
    with open(sql_file, "w") as f:
        f.write("SELECT * FROM test")

    config = EnrichmentConfig(target_path=str(yaml_file), database="ANALYTICS", schema="accounting")

    service = MetadataEnrichmentService(config)
    models = service._discover_models()

    assert len(models) == 1
    assert models[0] == str(sql_file)
