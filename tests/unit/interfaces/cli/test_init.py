"""
Unit tests for init CLI command.
"""

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from snowflake_semantic_tools.interfaces.cli.commands.init import init


@pytest.fixture
def runner():
    """Create a Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def temp_dbt_project(tmp_path):
    """Create a temporary dbt project structure."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    dbt_project = {
        "name": "test_project",
        "profile": "test_profile",
        "model-paths": ["models"],
    }
    dbt_project_file = project_dir / "dbt_project.yml"
    with open(dbt_project_file, "w") as f:
        yaml.dump(dbt_project, f)
    return project_dir


@pytest.fixture
def temp_profiles_yml(tmp_path):
    """Create a temporary profiles.yml in a separate directory."""
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    profiles = {
        "test_profile": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "snowflake",
                    "account": "test.us-east-1",
                    "user": "test_user",
                    "authenticator": "externalbrowser",
                    "role": "TEST_ROLE",
                    "warehouse": "TEST_WH",
                    "database": "TEST_DB",
                    "schema": "DEV",
                },
            },
        }
    }
    profiles_file = profiles_dir / "profiles.yml"
    with open(profiles_file, "w") as f:
        yaml.dump(profiles, f)
    return profiles_file


class TestInitCommand:
    """Tests for init CLI command."""

    def test_init_help(self, runner):
        """Test init --help works."""
        result = runner.invoke(init, ["--help"])

        assert result.exit_code == 0
        assert "Initialize SST" in result.output
        assert "--skip-prompts" in result.output
        assert "--check-only" in result.output

    def test_init_no_dbt_project_exits(self, runner, tmp_path):
        """Test that init exits with error when no dbt project."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(init, ["--skip-prompts"])

        assert result.exit_code == 1
        assert "No dbt project detected" in result.output

    def test_init_skip_prompts(self, runner, temp_dbt_project, temp_profiles_yml):
        """Test init --skip-prompts uses defaults."""
        import os
        import shutil

        shutil.copy(temp_profiles_yml, temp_dbt_project / "profiles.yml")

        # Change to the project directory
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dbt_project)
            # Mock connection test to avoid actual Snowflake connection
            with patch("snowflake_semantic_tools.services.init_wizard.InitWizard._test_snowflake_connection"):
                result = runner.invoke(init, ["--skip-prompts"])

            assert result.exit_code == 0
            assert "Detected dbt project" in result.output
            assert (temp_dbt_project / "sst_config.yml").exists()
        finally:
            os.chdir(original_cwd)

    def test_init_check_only(self, runner, temp_dbt_project, temp_profiles_yml):
        """Test init --check-only shows status without creating files."""
        import os
        import shutil

        shutil.copy(temp_profiles_yml, temp_dbt_project / "profiles.yml")

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dbt_project)
            result = runner.invoke(init, ["--check-only"])

            assert result.exit_code == 0
            assert "SST Setup Status" in result.output
            assert "dbt project" in result.output or "test_project" in result.output
            # Should not create sst_config.yml
            assert not (temp_dbt_project / "sst_config.yml").exists()
        finally:
            os.chdir(original_cwd)

    def test_init_check_only_no_config(self, runner, temp_dbt_project, temp_profiles_yml):
        """Test check-only shows missing config."""
        import os
        import shutil

        shutil.copy(temp_profiles_yml, temp_dbt_project / "profiles.yml")

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dbt_project)
            result = runner.invoke(init, ["--check-only"])

            assert result.exit_code == 0
            assert "No sst_config" in result.output or "sst init" in result.output
        finally:
            os.chdir(original_cwd)

    def test_init_check_only_with_config(self, runner, temp_dbt_project, temp_profiles_yml):
        """Test check-only shows existing config."""
        import os
        import shutil

        shutil.copy(temp_profiles_yml, temp_dbt_project / "profiles.yml")

        # Create sst_config.yml
        sst_config = {"project": {"semantic_models_dir": "test", "dbt_models_dir": "models"}}
        with open(temp_dbt_project / "sst_config.yml", "w") as f:
            yaml.dump(sst_config, f)

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dbt_project)
            result = runner.invoke(init, ["--check-only"])

            assert result.exit_code == 0
            assert "sst_config" in result.output
        finally:
            os.chdir(original_cwd)

    def test_init_creates_semantic_models_dir(self, runner, temp_dbt_project, temp_profiles_yml):
        """Test that init creates semantic models directory."""
        import os
        import shutil

        shutil.copy(temp_profiles_yml, temp_dbt_project / "profiles.yml")

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dbt_project)
            with patch("snowflake_semantic_tools.services.init_wizard.InitWizard._test_snowflake_connection"):
                result = runner.invoke(init, ["--skip-prompts"])

            assert result.exit_code == 0
            semantic_dir = temp_dbt_project / "snowflake_semantic_models"
            assert semantic_dir.exists()
            assert (semantic_dir / "metrics").exists()
            assert (semantic_dir / "relationships").exists()
        finally:
            os.chdir(original_cwd)

    def test_init_creates_example_files(self, runner, temp_dbt_project, temp_profiles_yml):
        """Test that init creates example files."""
        import os
        import shutil

        shutil.copy(temp_profiles_yml, temp_dbt_project / "profiles.yml")

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dbt_project)
            with patch("snowflake_semantic_tools.services.init_wizard.InitWizard._test_snowflake_connection"):
                result = runner.invoke(init, ["--skip-prompts"])

            assert result.exit_code == 0
            semantic_dir = temp_dbt_project / "snowflake_semantic_models"
            assert (semantic_dir / "metrics" / "_examples.yml").exists()
            assert (semantic_dir / "README.md").exists()
        finally:
            os.chdir(original_cwd)


class TestInitCommandEdgeCases:
    """Edge case tests for init command."""

    def test_init_with_existing_sst_config(self, runner, temp_dbt_project, temp_profiles_yml):
        """Test init with existing sst_config.yml."""
        import os
        import shutil

        shutil.copy(temp_profiles_yml, temp_dbt_project / "profiles.yml")

        # Create existing config
        with open(temp_dbt_project / "sst_config.yml", "w") as f:
            f.write("project:\n  semantic_models_dir: existing")

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dbt_project)
            with patch("snowflake_semantic_tools.services.init_wizard.InitWizard._test_snowflake_connection"):
                # In skip-prompts mode, it should overwrite
                result = runner.invoke(init, ["--skip-prompts"])

            assert result.exit_code == 0
        finally:
            os.chdir(original_cwd)

    def test_init_with_semantic_dir_exists(self, runner, temp_dbt_project, temp_profiles_yml):
        """Test init when semantic models directory already exists."""
        import os
        import shutil

        shutil.copy(temp_profiles_yml, temp_dbt_project / "profiles.yml")

        # Create existing directory
        (temp_dbt_project / "snowflake_semantic_models" / "metrics").mkdir(parents=True)

        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dbt_project)
            with patch("snowflake_semantic_tools.services.init_wizard.InitWizard._test_snowflake_connection"):
                result = runner.invoke(init, ["--skip-prompts"])

            # Should still succeed
            assert result.exit_code == 0
        finally:
            os.chdir(original_cwd)
