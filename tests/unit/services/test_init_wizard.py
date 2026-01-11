"""
Unit tests for InitWizard service.
"""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml

from snowflake_semantic_tools.services.init_wizard import DbtProjectInfo, InitWizard, ProfileInfo, WizardConfig

# Note: WizardConfig no longer has dbt_models_dir field - dbt model paths
# are now auto-detected from dbt_project.yml

# =============================================================================
# Fixtures
# =============================================================================


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
                "prod": {
                    "type": "snowflake",
                    "account": "test.us-east-1",
                    "user": "test_user",
                    "authenticator": "externalbrowser",
                    "role": "TEST_ROLE",
                    "warehouse": "TEST_WH",
                    "database": "TEST_DB",
                    "schema": "PROD",
                },
            },
        }
    }
    profiles_file = profiles_dir / "profiles.yml"
    with open(profiles_file, "w") as f:
        yaml.dump(profiles, f)
    return profiles_file


@pytest.fixture
def wizard(temp_dbt_project):
    """Create an InitWizard instance for testing."""
    return InitWizard(project_dir=temp_dbt_project, skip_prompts=True)


# =============================================================================
# Detection Tests
# =============================================================================


class TestDetectDbtProject:
    """Tests for dbt project detection."""

    def test_detect_dbt_project_found(self, temp_dbt_project):
        """Test detection when dbt_project.yml exists."""
        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        result = wizard.detect_dbt_project()

        assert result is not None
        assert result.name == "test_project"
        assert result.profile_name == "test_profile"
        assert result.model_paths == ["models"]

    def test_detect_dbt_project_not_found(self, tmp_path):
        """Test detection when dbt_project.yml doesn't exist."""
        wizard = InitWizard(project_dir=tmp_path, skip_prompts=True)
        result = wizard.detect_dbt_project()

        assert result is None

    def test_detect_dbt_project_invalid_yaml(self, tmp_path):
        """Test detection with invalid YAML."""
        dbt_project_file = tmp_path / "dbt_project.yml"
        dbt_project_file.write_text("invalid: yaml: content:")

        wizard = InitWizard(project_dir=tmp_path, skip_prompts=True)
        result = wizard.detect_dbt_project()

        assert result is None

    def test_detect_dbt_project_uses_name_as_profile_fallback(self, tmp_path):
        """Test that project name is used as profile fallback."""
        dbt_project = {"name": "my_project"}  # No profile specified
        dbt_project_file = tmp_path / "dbt_project.yml"
        with open(dbt_project_file, "w") as f:
            yaml.dump(dbt_project, f)

        wizard = InitWizard(project_dir=tmp_path, skip_prompts=True)
        result = wizard.detect_dbt_project()

        assert result is not None
        assert result.profile_name == "my_project"


class TestDetectProfile:
    """Tests for profile detection."""

    def test_detect_profile_found(self, temp_dbt_project, temp_profiles_yml):
        """Test profile detection when profile exists."""
        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)

        # Copy profiles.yml to project directory
        import shutil

        shutil.copy(temp_profiles_yml, temp_dbt_project / "profiles.yml")

        result = wizard.detect_profile("test_profile")

        assert result is not None
        assert result.name == "test_profile"
        assert "dev" in result.targets
        assert "prod" in result.targets
        assert result.default_target == "dev"

    def test_detect_profile_not_found(self, temp_dbt_project):
        """Test profile detection when profile doesn't exist."""
        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        result = wizard.detect_profile("nonexistent_profile")

        assert result is None

    def test_detect_profile_wrong_name(self, temp_dbt_project, temp_profiles_yml):
        """Test profile detection with wrong profile name."""
        import shutil

        shutil.copy(temp_profiles_yml, temp_dbt_project / "profiles.yml")

        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        result = wizard.detect_profile("wrong_profile")

        assert result is None


# =============================================================================
# Config Creation Tests
# =============================================================================


class TestCreateSstConfig:
    """Tests for sst_config.yml creation."""

    def test_create_sst_config_new(self, temp_dbt_project):
        """Test creating new sst_config.yml."""
        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        config = WizardConfig(
            semantic_models_dir="snowflake_semantic_models",
        )

        wizard._create_sst_config(config)

        config_path = temp_dbt_project / "sst_config.yml"
        assert config_path.exists()

        with open(config_path) as f:
            content = f.read()
        assert "snowflake_semantic_models" in content
        # dbt_models_dir should NOT be in the config anymore
        assert "dbt_models_dir" not in content or "# Note:" in content

    def test_create_sst_config_overwrite(self, temp_dbt_project):
        """Test overwriting existing sst_config.yml."""
        # Create existing config
        existing_config = temp_dbt_project / "sst_config.yml"
        existing_config.write_text("old: config")

        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        config = WizardConfig(
            semantic_models_dir="semantic_models",
        )

        wizard._create_sst_config(config)

        with open(existing_config) as f:
            content = f.read()
        assert "semantic_models" in content
        assert "old" not in content


class TestDetectExistingConfig:
    """Tests for existing config detection."""

    def test_detect_existing_config_yaml(self, temp_dbt_project):
        """Test detection of sst_config.yaml."""
        config_file = temp_dbt_project / "sst_config.yaml"
        config_file.write_text("project:\n  semantic_models_dir: test")

        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        result = wizard.detect_existing_config()

        assert result is not None
        assert result.name == "sst_config.yaml"

    def test_detect_existing_config_yml(self, temp_dbt_project):
        """Test detection of sst_config.yml."""
        config_file = temp_dbt_project / "sst_config.yml"
        config_file.write_text("project:\n  semantic_models_dir: test")

        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        result = wizard.detect_existing_config()

        assert result is not None
        assert result.name == "sst_config.yml"

    def test_detect_existing_config_not_found(self, temp_dbt_project):
        """Test when no config exists."""
        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        result = wizard.detect_existing_config()

        assert result is None


# =============================================================================
# Directory/File Creation Tests
# =============================================================================


class TestCreateDirectories:
    """Tests for directory structure creation."""

    def test_create_directories_full_structure(self, temp_dbt_project):
        """Test creation of all subdirectories."""
        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        config = WizardConfig(semantic_models_dir="snowflake_semantic_models")

        wizard._create_directories(config)

        base_dir = temp_dbt_project / "snowflake_semantic_models"
        assert base_dir.exists()
        assert (base_dir / "metrics").exists()
        assert (base_dir / "relationships").exists()
        assert (base_dir / "filters").exists()
        assert (base_dir / "verified_queries").exists()
        assert (base_dir / "custom_instructions").exists()

    def test_create_directories_already_exists(self, temp_dbt_project):
        """Test that existing directories don't cause errors."""
        # Create directory first
        existing_dir = temp_dbt_project / "snowflake_semantic_models" / "metrics"
        existing_dir.mkdir(parents=True)

        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        config = WizardConfig(semantic_models_dir="snowflake_semantic_models")

        # Should not raise
        wizard._create_directories(config)

        assert existing_dir.exists()


class TestCreateExampleFiles:
    """Tests for example file creation."""

    def test_create_example_files(self, temp_dbt_project):
        """Test creation of example files."""
        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        config = WizardConfig(
            semantic_models_dir="snowflake_semantic_models",
            create_examples=True,
        )

        wizard._create_directories(config)
        wizard._create_example_files(config)

        base_dir = temp_dbt_project / "snowflake_semantic_models"

        # Check example files exist
        assert (base_dir / "metrics" / "_examples.yml").exists()
        assert (base_dir / "relationships" / "_examples.yml").exists()
        assert (base_dir / "filters" / "_examples.yml").exists()
        assert (base_dir / "verified_queries" / "_examples.yml").exists()
        assert (base_dir / "custom_instructions" / "_examples.yml").exists()
        assert (base_dir / "semantic_views.yml").exists()
        assert (base_dir / "README.md").exists()

    def test_example_files_are_commented(self, temp_dbt_project):
        """Test that example content is commented out."""
        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        config = WizardConfig(
            semantic_models_dir="snowflake_semantic_models",
            create_examples=True,
        )

        wizard._create_directories(config)
        wizard._create_example_files(config)

        metrics_file = temp_dbt_project / "snowflake_semantic_models" / "metrics" / "_examples.yml"
        content = metrics_file.read_text()

        # All snowflake_metrics: lines should be commented
        lines = content.split("\n")
        for line in lines:
            if "snowflake_metrics:" in line:
                assert line.strip().startswith("#"), f"Line should be commented: {line}"

    def test_create_readme(self, temp_dbt_project):
        """Test README.md creation."""
        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)
        config = WizardConfig(
            semantic_models_dir="snowflake_semantic_models",
            create_examples=True,
        )

        wizard._create_directories(config)
        wizard._create_example_files(config)

        readme = temp_dbt_project / "snowflake_semantic_models" / "README.md"
        assert readme.exists()

        content = readme.read_text()
        assert "Semantic Models" in content
        assert "metrics" in content.lower()


# =============================================================================
# Validation Tests
# =============================================================================


class TestValidation:
    """Tests for input validation."""

    def test_validate_snowflake_account_format_valid(self, temp_dbt_project):
        """Test valid Snowflake account formats."""
        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)

        assert wizard._validate_account("abc123.us-east-1") is None
        assert wizard._validate_account("myaccount.snowflakecomputing.com") is None
        assert wizard._validate_account("org-account") is None

    def test_validate_snowflake_account_format_invalid(self, temp_dbt_project):
        """Test invalid Snowflake account formats."""
        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)

        assert wizard._validate_account("") is not None
        assert wizard._validate_account("nodotsordashes") is not None

    def test_validate_required_fields(self, temp_dbt_project):
        """Test that empty required fields are rejected."""
        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)

        assert wizard._validate_account("") is not None
        assert "required" in wizard._validate_account("").lower()


# =============================================================================
# Profile Creation Tests
# =============================================================================


class TestProfileCreation:
    """Tests for profile creation."""

    @patch("questionary.text")
    @patch("questionary.select")
    def test_create_profile_sso_auth(self, mock_select, mock_text, temp_dbt_project, tmp_path):
        """Test creating profile with SSO authentication."""
        # Mock user inputs
        mock_text.return_value.ask.side_effect = [
            "myaccount.us-east-1",  # account
            "user@example.com",  # user
            "TEST_ROLE",  # role
            "TEST_WH",  # warehouse
            "TEST_DB",  # database
            "DEV",  # schema
        ]
        mock_select.return_value.ask.return_value = "sso"

        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=False)

        # Override home to tmp_path for test
        with patch.object(Path, "home", return_value=tmp_path):
            result = wizard._create_profile_interactive("test_profile")

        assert result is True

        profiles_path = tmp_path / ".dbt" / "profiles.yml"
        assert profiles_path.exists()

        with open(profiles_path) as f:
            profiles = yaml.safe_load(f)

        assert "test_profile" in profiles
        assert profiles["test_profile"]["outputs"]["dev"]["authenticator"] == "externalbrowser"

    @patch("questionary.text")
    @patch("questionary.select")
    def test_create_profile_password_auth(self, mock_select, mock_text, temp_dbt_project, tmp_path):
        """Test creating profile with password authentication uses env_var."""
        mock_text.return_value.ask.side_effect = [
            "myaccount.us-east-1",
            "user@example.com",
            "TEST_ROLE",
            "TEST_WH",
            "TEST_DB",
            "DEV",
        ]
        mock_select.return_value.ask.return_value = "password"

        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=False)

        with patch.object(Path, "home", return_value=tmp_path):
            result = wizard._create_profile_interactive("test_profile")

        assert result is True

        profiles_path = tmp_path / ".dbt" / "profiles.yml"
        with open(profiles_path) as f:
            profiles = yaml.safe_load(f)

        password_value = profiles["test_profile"]["outputs"]["dev"]["password"]
        assert "env_var" in password_value
        assert "SNOWFLAKE_PASSWORD" in password_value

    @patch("questionary.text")
    @patch("questionary.select")
    def test_create_profile_keypair_auth(self, mock_select, mock_text, temp_dbt_project, tmp_path):
        """Test creating profile with key pair authentication."""
        mock_text.return_value.ask.side_effect = [
            "myaccount.us-east-1",
            "user@example.com",
            "~/.ssh/snowflake.p8",  # private key path
            "TEST_ROLE",
            "TEST_WH",
            "TEST_DB",
            "DEV",
        ]
        mock_select.return_value.ask.return_value = "keypair"

        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=False)

        with patch.object(Path, "home", return_value=tmp_path):
            result = wizard._create_profile_interactive("test_profile")

        assert result is True

        profiles_path = tmp_path / ".dbt" / "profiles.yml"
        with open(profiles_path) as f:
            profiles = yaml.safe_load(f)

        output = profiles["test_profile"]["outputs"]["dev"]
        assert "private_key_path" in output
        assert "env_var" in output["private_key_passphrase"]

    def test_create_profile_appends_existing(self, temp_dbt_project, tmp_path):
        """Test that new profile is appended to existing profiles.yml."""
        # Create existing profiles.yml
        dbt_dir = tmp_path / ".dbt"
        dbt_dir.mkdir()
        existing_profiles = {
            "existing_profile": {
                "target": "dev",
                "outputs": {"dev": {"type": "snowflake", "account": "old"}},
            }
        }
        with open(dbt_dir / "profiles.yml", "w") as f:
            yaml.dump(existing_profiles, f)

        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=False)

        with (
            patch("questionary.text") as mock_text,
            patch("questionary.select") as mock_select,
            patch.object(Path, "home", return_value=tmp_path),
        ):
            mock_text.return_value.ask.side_effect = [
                "new.us-east-1",
                "new_user",
                "ROLE",
                "WH",
                "DB",
                "SCHEMA",
            ]
            mock_select.return_value.ask.return_value = "sso"

            wizard._create_profile_interactive("new_profile")

        with open(dbt_dir / "profiles.yml") as f:
            profiles = yaml.safe_load(f)

        # Both profiles should exist
        assert "existing_profile" in profiles
        assert "new_profile" in profiles


# =============================================================================
# Integration Tests
# =============================================================================


class TestWizardRun:
    """Integration tests for the full wizard run."""

    def test_run_skip_prompts_mode(self, temp_dbt_project, temp_profiles_yml):
        """Test running wizard in skip-prompts mode."""
        import shutil

        shutil.copy(temp_profiles_yml, temp_dbt_project / "profiles.yml")

        wizard = InitWizard(project_dir=temp_dbt_project, skip_prompts=True)

        # Mock the connection test
        with patch.object(wizard, "_test_snowflake_connection"):
            result = wizard.run()

        assert result is True

        # Verify files were created
        assert (temp_dbt_project / "sst_config.yml").exists()
        assert (temp_dbt_project / "snowflake_semantic_models").exists()

    def test_run_no_dbt_project_fails(self, tmp_path):
        """Test that wizard fails when no dbt project exists."""
        wizard = InitWizard(project_dir=tmp_path, skip_prompts=True)
        result = wizard.run()

        assert result is False
