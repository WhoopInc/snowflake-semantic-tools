#!/usr/bin/env python3
"""
Unit tests for ManifestParser

Tests the dbt manifest.json parser for database/schema resolution.
"""

import json
import pytest
from pathlib import Path
from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser


@pytest.fixture
def sample_manifest():
    """Create a sample manifest structure for testing."""
    return {
        "metadata": {
            "dbt_version": "1.7.0",
            "target_name": "prod",
            "project_name": "analytics_dbt"
        },
        "nodes": {
            "model.analytics_dbt.memberships_members": {
                "resource_type": "model",
                "database": "ANALYTICS",
                "schema": "MEMBERSHIPS",
                "name": "memberships_members",
                "alias": "memberships_members",
                "relation_name": '"ANALYTICS"."MEMBERSHIPS"."MEMBERSHIPS_MEMBERS"',
                "original_file_path": "models/analytics/memberships/memberships_members.sql"
            },
            "model.analytics_dbt.int_memberships_prep": {
                "resource_type": "model",
                "database": "ANALYTICS_INTERMEDIATE",
                "schema": "INT_MEMBERSHIPS",
                "name": "int_memberships_prep",
                "alias": "int_memberships_prep",
                "relation_name": '"ANALYTICS_INTERMEDIATE"."INT_MEMBERSHIPS"."INT_MEMBERSHIPS_PREP"',
                "original_file_path": "models/analytics/memberships/_intermediate/int_memberships_prep.sql"
            },
            "test.analytics_dbt.test_members": {
                "resource_type": "test",
                "database": "ANALYTICS",
                "schema": "MEMBERSHIPS",
                "name": "test_members"
            },
            "seed.analytics_dbt.seed_data": {
                "resource_type": "seed",
                "database": "ANALYTICS",
                "schema": "PUBLIC",
                "name": "seed_data"
            }
        }
    }


@pytest.fixture
def manifest_file(tmp_path, sample_manifest):
    """Create a temporary manifest.json file."""
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(manifest_path, 'w') as f:
        json.dump(sample_manifest, f)
    
    return manifest_path


class TestManifestParser:
    """Test suite for ManifestParser class."""
    
    def test_init_default_path(self):
        """Test initialization with default path."""
        parser = ManifestParser()
        assert parser.manifest is None
        assert parser.model_locations == {}
    
    def test_init_custom_path(self, manifest_file):
        """Test initialization with custom path."""
        parser = ManifestParser(manifest_path=manifest_file)
        assert parser.manifest_path == manifest_file
    
    def test_load_success(self, manifest_file):
        """Test successful manifest loading."""
        parser = ManifestParser(manifest_path=manifest_file)
        result = parser.load()
        
        assert result is True
        assert parser.manifest is not None
        assert len(parser.model_locations) == 2  # Only models, not tests/seeds
    
    def test_load_file_not_found(self, tmp_path):
        """Test loading when manifest file doesn't exist."""
        parser = ManifestParser(manifest_path=tmp_path / "nonexistent.json")
        result = parser.load()
        
        assert result is False
        assert parser.manifest is None
    
    def test_load_invalid_json(self, tmp_path):
        """Test loading with invalid JSON."""
        manifest_path = tmp_path / "manifest.json"
        with open(manifest_path, 'w') as f:
            f.write("{ invalid json }")
        
        parser = ManifestParser(manifest_path=manifest_path)
        result = parser.load()
        
        assert result is False
        assert parser.manifest is None
    
    def test_build_location_cache(self, manifest_file):
        """Test that location cache is built correctly."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        # Should have 2 models (test and seed excluded)
        assert len(parser.model_locations) == 2
        
        # Check memberships_members
        assert 'memberships_members' in parser.model_locations
        location = parser.model_locations['memberships_members']
        assert location['database'] == 'ANALYTICS'
        assert location['schema'] == 'MEMBERSHIPS'
        assert location['alias'] == 'memberships_members'
        
        # Check int_memberships_prep
        assert 'int_memberships_prep' in parser.model_locations
        location = parser.model_locations['int_memberships_prep']
        assert location['database'] == 'ANALYTICS_INTERMEDIATE'
        assert location['schema'] == 'INT_MEMBERSHIPS'
    
    def test_get_location_exists(self, manifest_file):
        """Test getting location for existing model."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        location = parser.get_location('memberships_members')
        
        assert location is not None
        assert location['database'] == 'ANALYTICS'
        assert location['schema'] == 'MEMBERSHIPS'
        assert location['alias'] == 'memberships_members'
        assert 'memberships_members.sql' in location['original_file_path']
    
    def test_get_location_not_exists(self, manifest_file):
        """Test getting location for non-existent model."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        location = parser.get_location('nonexistent_model')
        
        assert location is None
    
    def test_get_location_by_path_stem_match(self, manifest_file):
        """Test getting location by path using stem matching."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        model_path = Path('models/analytics/memberships/memberships_members.sql')
        location = parser.get_location_by_path(model_path)
        
        assert location is not None
        assert location['database'] == 'ANALYTICS'
        assert location['schema'] == 'MEMBERSHIPS'
    
    def test_get_location_by_path_full_match(self, manifest_file):
        """Test getting location by path using full path matching."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        model_path = Path('models/analytics/memberships/_intermediate/int_memberships_prep.sql')
        location = parser.get_location_by_path(model_path)
        
        assert location is not None
        assert location['database'] == 'ANALYTICS_INTERMEDIATE'
        assert location['schema'] == 'INT_MEMBERSHIPS'
    
    def test_get_location_by_path_not_found(self, manifest_file):
        """Test getting location for path not in manifest."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        model_path = Path('models/analytics/nonexistent/model.sql')
        location = parser.get_location_by_path(model_path)
        
        assert location is None
    
    def test_get_all_models_in_directory(self, manifest_file):
        """Test getting all models in a specific directory."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        directory = Path('models/analytics/memberships')
        models = parser.get_all_models_in_directory(directory)
        
        # Should find 2 models (including _intermediate)
        assert len(models) == 2
        
        # Check model names
        model_names = [m['model_name'] for m in models]
        assert 'memberships_members' in model_names
        assert 'int_memberships_prep' in model_names
    
    def test_get_all_models_in_directory_empty(self, manifest_file):
        """Test getting models from directory with no models."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        directory = Path('models/analytics/nonexistent')
        models = parser.get_all_models_in_directory(directory)
        
        assert len(models) == 0
    
    def test_get_target_name(self, manifest_file):
        """Test extracting target name from manifest."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        target = parser.get_target_name()
        
        assert target == 'prod'
    
    def test_is_prod_target_true(self, manifest_file):
        """Test production target detection - true case."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        assert parser.is_prod_target() is True
    
    def test_is_prod_target_false(self, tmp_path, sample_manifest):
        """Test production target detection - false case."""
        # Modify manifest to have dev target
        sample_manifest['metadata']['target_name'] = 'dev'
        
        manifest_path = tmp_path / "manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(sample_manifest, f)
        
        parser = ManifestParser(manifest_path=manifest_path)
        parser.load()
        
        assert parser.is_prod_target() is False
    
    def test_get_dbt_version(self, manifest_file):
        """Test extracting dbt version."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        version = parser.get_dbt_version()
        
        assert version == '1.7.0'
    
    def test_get_project_name(self, manifest_file):
        """Test extracting project name."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        project = parser.get_project_name()
        
        assert project == 'analytics_dbt'
    
    def test_validate_against_yaml_matches(self, manifest_file):
        """Test validation when YAML matches manifest."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        result = parser.validate_against_yaml(
            'memberships_members',
            'ANALYTICS',
            'MEMBERSHIPS'
        )
        
        assert result['matches'] is True
        assert len(result['differences']) == 0
    
    def test_validate_against_yaml_database_mismatch(self, manifest_file):
        """Test validation when database doesn't match."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        result = parser.validate_against_yaml(
            'memberships_members',
            'WRONG_DATABASE',
            'MEMBERSHIPS'
        )
        
        assert result['matches'] is False
        assert len(result['differences']) == 1
        assert 'database' in result['differences'][0]
    
    def test_validate_against_yaml_schema_mismatch(self, manifest_file):
        """Test validation when schema doesn't match."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        result = parser.validate_against_yaml(
            'memberships_members',
            'ANALYTICS',
            'WRONG_SCHEMA'
        )
        
        assert result['matches'] is False
        assert len(result['differences']) == 1
        assert 'schema' in result['differences'][0]
    
    def test_validate_against_yaml_model_not_found(self, manifest_file):
        """Test validation when model not in manifest."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        result = parser.validate_against_yaml(
            'nonexistent_model',
            'ANALYTICS',
            'MEMBERSHIPS'
        )
        
        assert result['matches'] is False
        assert 'not found' in result['differences'][0]
    
    def test_get_summary(self, manifest_file):
        """Test getting manifest summary."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        summary = parser.get_summary()
        
        assert summary['loaded'] is True
        assert summary['target_name'] == 'prod'
        assert summary['dbt_version'] == '1.7.0'
        assert summary['project_name'] == 'analytics_dbt'
        assert summary['total_models'] == 2
        assert 'ANALYTICS' in summary['models_by_database']
        assert summary['models_by_database']['ANALYTICS'] == 1
        assert summary['models_by_database']['ANALYTICS_INTERMEDIATE'] == 1
    
    def test_get_summary_not_loaded(self):
        """Test getting summary when manifest not loaded."""
        parser = ManifestParser()
        summary = parser.get_summary()
        
        assert summary['loaded'] is False
    
    def test_case_insensitive_database_schema(self, manifest_file):
        """Test that database and schema are uppercased."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        location = parser.get_location('memberships_members')
        
        # Should be uppercased
        assert location['database'] == location['database'].upper()
        assert location['schema'] == location['schema'].upper()
    
    def test_intermediate_models_different_database(self, manifest_file):
        """Test that intermediate models resolve to correct database."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        # Regular model
        regular_location = parser.get_location('memberships_members')
        assert regular_location['database'] == 'ANALYTICS'
        assert regular_location['schema'] == 'MEMBERSHIPS'
        
        # Intermediate model (different database)
        int_location = parser.get_location('int_memberships_prep')
        assert int_location['database'] == 'ANALYTICS_INTERMEDIATE'
        assert int_location['schema'] == 'INT_MEMBERSHIPS'


class TestManifestStalenessDetection:
    """Test staleness detection features."""
    
    def test_get_manifest_age_returns_timedelta(self, manifest_file):
        """Test that get_manifest_age returns a timedelta."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        age = parser.get_manifest_age()
        
        assert age is not None
        from datetime import timedelta
        assert isinstance(age, timedelta)
        # Should be very recent (just created)
        assert age.total_seconds() < 60  # Less than 1 minute old
    
    def test_get_manifest_age_without_manifest(self):
        """Test that get_manifest_age returns None when manifest not loaded."""
        parser = ManifestParser()
        
        age = parser.get_manifest_age()
        
        assert age is None
    
    def test_is_manifest_stale_fresh_manifest(self, manifest_file):
        """Test that a freshly created manifest is not stale."""
        parser = ManifestParser(manifest_path=manifest_file)
        parser.load()
        
        is_stale, reason = parser.is_manifest_stale(threshold_hours=24)
        
        assert is_stale is False
        assert reason is None
    
    def test_is_manifest_stale_by_age(self, tmp_path, sample_manifest):
        """Test that an old manifest is detected as stale."""
        import os
        import time
        
        # Create a manifest file
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(sample_manifest))
        
        # Modify file timestamp to make it old (25 hours ago)
        old_time = time.time() - (25 * 3600)  # 25 hours ago
        os.utime(manifest_path, (old_time, old_time))
        
        parser = ManifestParser(manifest_path=manifest_path)
        parser.load()
        
        is_stale, reason = parser.is_manifest_stale(threshold_hours=24)
        
        assert is_stale is True
        assert reason is not None
        assert "day(s) old" in reason or "hour(s) old" in reason
    
    def test_is_manifest_stale_by_file_changes(self, tmp_path, sample_manifest):
        """Test that manifest is stale when .sql files are newer."""
        import os
        import time
        
        # Create a manifest file
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(sample_manifest))
        
        # Make manifest timestamp old
        old_time = time.time() - 10  # 10 seconds ago
        os.utime(manifest_path, (old_time, old_time))
        
        # Create a models directory with a newer .sql file
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        sql_file = models_dir / "test.sql"
        sql_file.write_text("SELECT 1")  # Created just now (newer than manifest)
        
        # Change to temp directory for test
        import os as os_module
        original_cwd = os_module.getcwd()
        try:
            os_module.chdir(tmp_path)
            
            parser = ManifestParser(manifest_path=manifest_path)
            parser.load()
            
            is_stale, reason = parser.is_manifest_stale(threshold_hours=24)
            
            assert is_stale is True
            assert reason is not None
            assert "Model files modified" in reason
        finally:
            os_module.chdir(original_cwd)
    
    def test_is_manifest_stale_without_manifest(self):
        """Test that missing manifest is reported as stale."""
        parser = ManifestParser()
        
        is_stale, reason = parser.is_manifest_stale(threshold_hours=24)
        
        assert is_stale is True
        assert "not found" in reason


class TestManifestParserIntegration:
    """Integration tests with real manifest example."""
    
    def test_load_example_manifest(self):
        """Test loading the example manifest from documentation."""
        # This assumes the example manifest exists
        example_path = Path(__file__).parent.parent.parent / "examples" / "manifest_example.json"
        
        if not example_path.exists():
            pytest.skip("Example manifest not found")
        
        parser = ManifestParser(manifest_path=example_path)
        result = parser.load()
        
        assert result is True
        assert len(parser.model_locations) >= 2
        
        # Check specific models from example
        members_location = parser.get_location('memberships_members')
        if members_location:
            assert members_location['database'] == 'ANALYTICS'
            assert members_location['schema'] == 'MEMBERSHIPS'


