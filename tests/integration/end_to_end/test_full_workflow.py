#!/usr/bin/env python3
"""
End-to-end workflow tests using the sample project.

Tests complete SST workflows including validation, init, and format commands
using the embedded sample dbt+SST project.
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from tests.fixtures.sample_project_factory import SampleProjectFactory
from tests.integration.base import IntegrationTestBase


class TestFullWorkflow(IntegrationTestBase):
    """Test complete SST workflows using the sample project."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click CLI test runner."""
        return CliRunner()

    def test_validate_succeeds_on_valid_project(self, runner: CliRunner, sample_project: Path):
        """Test that validate passes on a properly configured project."""
        from snowflake_semantic_tools.interfaces.cli.commands.validate import validate

        original_cwd = os.getcwd()
        try:
            os.chdir(sample_project)
            result = runner.invoke(validate, ["--no-snowflake-check"])

            # Should complete without critical errors
            # Note: May have warnings but should not have errors that cause exit code != 0
            assert result.exit_code == 0 or "error" not in result.output.lower()
        finally:
            os.chdir(original_cwd)

    def test_validate_with_verbose_output(self, runner: CliRunner, sample_project: Path):
        """Test validate command with verbose flag."""
        from snowflake_semantic_tools.interfaces.cli.commands.validate import validate

        original_cwd = os.getcwd()
        try:
            os.chdir(sample_project)
            result = runner.invoke(validate, ["--verbose", "--no-snowflake-check"])

            # Verbose mode should show more detailed output
            assert "Running with sst=" in result.output
        finally:
            os.chdir(original_cwd)

    def test_validate_catches_invalid_metrics(self, runner: CliRunner, tmp_path: Path):
        """Test that validation detects invalid metrics."""
        from snowflake_semantic_tools.interfaces.cli.commands.validate import validate

        project = SampleProjectFactory.create_with_invalid_metrics(tmp_path)

        original_cwd = os.getcwd()
        try:
            os.chdir(project)
            result = runner.invoke(validate, ["--no-snowflake-check"])

            # Should detect the invalid metric (missing expr field)
            # Either exits with error or output contains error indication
            has_error = result.exit_code != 0 or "error" in result.output.lower()
            assert has_error, f"Expected validation error but got: {result.output}"
        finally:
            os.chdir(original_cwd)

    def test_validate_catches_missing_table_reference(self, runner: CliRunner, tmp_path: Path):
        """Test that validation detects references to non-existent tables."""
        from snowflake_semantic_tools.interfaces.cli.commands.validate import validate

        project = SampleProjectFactory.create_with_missing_table_reference(tmp_path)

        original_cwd = os.getcwd()
        try:
            os.chdir(project)
            result = runner.invoke(validate, ["--no-snowflake-check"])

            # Should detect the invalid table reference
            has_error = result.exit_code != 0 or "nonexistent" in result.output.lower()
            assert has_error, f"Expected validation error for missing table but got: {result.output}"
        finally:
            os.chdir(original_cwd)

    def test_init_wizard_on_fresh_dbt_project(self, runner: CliRunner, tmp_path: Path):
        """Test init wizard creates correct structure on uninitialized project."""
        from snowflake_semantic_tools.interfaces.cli.commands.init import init

        project = SampleProjectFactory.create_without_sst(tmp_path)

        # Add profiles.yml for init to work
        profiles_content = """sample_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: test.us-east-1
      user: test_user
      authenticator: externalbrowser
      role: TEST_ROLE
      warehouse: TEST_WH
      database: SAMPLE_DATABASE
      schema: DEV
"""
        (project / "profiles.yml").write_text(profiles_content)

        original_cwd = os.getcwd()
        try:
            os.chdir(project)
            with patch("snowflake_semantic_tools.services.init_wizard.InitWizard._test_snowflake_connection"):
                result = runner.invoke(init, ["--skip-prompts"])

            assert result.exit_code == 0, f"Init failed with: {result.output}"
            assert (project / "sst_config.yml").exists() or (project / "sst_config.yaml").exists()
            assert (project / "snowflake_semantic_models").is_dir()
        finally:
            os.chdir(original_cwd)

    def test_init_check_only_shows_status(self, runner: CliRunner, sample_project: Path):
        """Test init --check-only shows status without modifying project."""
        from snowflake_semantic_tools.interfaces.cli.commands.init import init

        # Add profiles.yml
        profiles_content = """sample_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: test.us-east-1
      user: test_user
      authenticator: externalbrowser
      role: TEST_ROLE
      warehouse: TEST_WH
      database: SAMPLE_DATABASE
      schema: DEV
"""
        (sample_project / "profiles.yml").write_text(profiles_content)

        original_cwd = os.getcwd()
        try:
            os.chdir(sample_project)
            result = runner.invoke(init, ["--check-only"])

            assert result.exit_code == 0
            assert "SST Setup Status" in result.output or "sst_config" in result.output
        finally:
            os.chdir(original_cwd)

    def test_format_maintains_valid_yaml(self, runner: CliRunner, sample_project: Path):
        """Test that format command preserves valid YAML structure."""
        from snowflake_semantic_tools.interfaces.cli.commands.format import format_cmd
        from snowflake_semantic_tools.interfaces.cli.commands.validate import validate

        original_cwd = os.getcwd()
        try:
            os.chdir(sample_project)

            # Format the models directory
            result = runner.invoke(format_cmd, ["models/"])
            assert result.exit_code == 0, f"Format failed: {result.output}"

            # Validate can still pass after formatting
            result = runner.invoke(validate, ["--no-snowflake-check"])
            # Either passes or has only warnings (no critical errors)
            assert result.exit_code == 0 or "error" not in result.output.lower()
        finally:
            os.chdir(original_cwd)

    def test_format_semantic_models(self, runner: CliRunner, sample_project: Path):
        """Test formatting semantic model YAML files."""
        from snowflake_semantic_tools.interfaces.cli.commands.format import format_cmd

        original_cwd = os.getcwd()
        try:
            os.chdir(sample_project)
            result = runner.invoke(format_cmd, ["snowflake_semantic_models/"])

            assert result.exit_code == 0, f"Format failed: {result.output}"
        finally:
            os.chdir(original_cwd)


class TestSampleProjectFactory:
    """Tests for the SampleProjectFactory class itself."""

    def test_create_returns_valid_project(self, tmp_path: Path):
        """Test that create() returns a complete project structure."""
        project = SampleProjectFactory.create(tmp_path)

        assert project.exists()
        assert (project / "dbt_project.yml").exists()
        assert (project / "sst_config.yaml").exists()
        assert (project / "target" / "manifest.json").exists()
        assert (project / "snowflake_semantic_models").is_dir()
        assert (project / "models" / "marts").is_dir()
        assert (project / "models" / "staging").is_dir()

    def test_create_without_sst_removes_config(self, tmp_path: Path):
        """Test that create_without_sst removes SST configuration."""
        project = SampleProjectFactory.create_without_sst(tmp_path)

        assert project.exists()
        assert (project / "dbt_project.yml").exists()
        assert not (project / "sst_config.yaml").exists()
        assert not (project / "snowflake_semantic_models").exists()

    def test_create_without_manifest_removes_manifest(self, tmp_path: Path):
        """Test that create_without_manifest removes manifest.json."""
        project = SampleProjectFactory.create_without_manifest(tmp_path)

        assert project.exists()
        assert (project / "dbt_project.yml").exists()
        assert not (project / "target" / "manifest.json").exists()

    def test_create_with_invalid_metrics_adds_invalid_file(self, tmp_path: Path):
        """Test that create_with_invalid_metrics adds an invalid metrics file."""
        project = SampleProjectFactory.create_with_invalid_metrics(tmp_path)

        invalid_metrics = project / "snowflake_semantic_models" / "metrics" / "invalid.yml"
        assert invalid_metrics.exists()
        content = invalid_metrics.read_text()
        assert "broken_metric" in content

    def test_create_unenriched_strips_metadata(self, tmp_path: Path):
        """Test that create_unenriched removes SST metadata from models."""
        project = SampleProjectFactory.create_unenriched(tmp_path)

        # Check that customers.yml exists but has no config.meta.sst
        customers_yml = project / "models" / "marts" / "customers.yml"
        assert customers_yml.exists()

        import yaml

        with open(customers_yml) as f:
            data = yaml.safe_load(f)

        # Models should have no config section
        for model in data.get("models", []):
            assert "config" not in model, "Model should not have config after stripping"
            for col in model.get("columns", []):
                assert "config" not in col, "Column should not have config after stripping"

    def test_create_with_profiles_adds_profiles_yml(self, tmp_path: Path):
        """Test that create_with_profiles adds a profiles.yml file."""
        project = SampleProjectFactory.create_with_profiles(tmp_path)

        assert (project / "profiles.yml").exists()
        content = (project / "profiles.yml").read_text()
        assert "sample_project" in content
        assert "snowflake" in content

    def test_multiple_projects_are_isolated(self, tmp_path: Path):
        """Test that creating multiple projects gives independent copies."""
        project1 = SampleProjectFactory.create(tmp_path, "project1")
        project2 = SampleProjectFactory.create(tmp_path, "project2")

        assert project1 != project2
        assert project1.exists()
        assert project2.exists()

        # Modifying one should not affect the other
        (project1 / "test_file.txt").write_text("test")
        assert not (project2 / "test_file.txt").exists()
