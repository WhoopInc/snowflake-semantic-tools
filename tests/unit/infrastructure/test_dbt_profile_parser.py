"""
Unit tests for DbtProfileParser.

Tests the parsing of dbt profiles.yml for Snowflake authentication.
"""

import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from snowflake_semantic_tools.infrastructure.dbt.exceptions import (
    DbtProfileNotFoundError,
    DbtProfileParseError,
    DbtProjectNotFoundError,
)
from snowflake_semantic_tools.infrastructure.dbt.profile_parser import DbtProfileConfig, DbtProfileParser

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def fixtures_path():
    """Return path to test fixtures."""
    return Path(__file__).parent.parent.parent / "fixtures" / "dbt_profiles"


@pytest.fixture
def temp_project_dir(fixtures_path):
    """Create a temporary project directory with dbt_project.yml and profiles.yml."""
    temp_dir = tempfile.mkdtemp()

    # Copy dbt_project.yml
    shutil.copy(
        fixtures_path / "valid_dbt_project.yml",
        Path(temp_dir) / "dbt_project.yml",
    )

    # Copy profiles.yml
    shutil.copy(
        fixtures_path / "valid_password_profile.yml",
        Path(temp_dir) / "profiles.yml",
    )

    yield Path(temp_dir)

    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def temp_project_with_profile(fixtures_path, request):
    """Create a temporary project directory with a specific profile file."""
    profile_file = request.param
    temp_dir = tempfile.mkdtemp()

    # Copy dbt_project.yml
    shutil.copy(
        fixtures_path / "valid_dbt_project.yml",
        Path(temp_dir) / "dbt_project.yml",
    )

    # Copy specified profiles.yml
    shutil.copy(
        fixtures_path / profile_file,
        Path(temp_dir) / "profiles.yml",
    )

    yield Path(temp_dir)

    # Cleanup
    shutil.rmtree(temp_dir)


# =============================================================================
# DbtProfileParser Tests
# =============================================================================


class TestDbtProfileParser:
    """Tests for DbtProfileParser."""

    def test_get_profile_name_success(self, temp_project_dir):
        """Test extracting profile name from dbt_project.yml."""
        parser = DbtProfileParser(project_dir=temp_project_dir)
        profile_name = parser.get_profile_name()
        assert profile_name == "test_project"

    def test_get_profile_name_missing_project(self, tmp_path):
        """Test error when dbt_project.yml doesn't exist."""
        parser = DbtProfileParser(project_dir=tmp_path)
        with pytest.raises(DbtProjectNotFoundError):
            parser.get_profile_name()

    def test_find_profiles_in_project_dir(self, temp_project_dir):
        """Test finding profiles.yml in project directory."""
        parser = DbtProfileParser(project_dir=temp_project_dir)
        profiles_path = parser.find_profiles_yml()
        assert profiles_path is not None
        assert profiles_path.exists()
        assert profiles_path == temp_project_dir / "profiles.yml"

    def test_find_profiles_not_found(self, tmp_path, fixtures_path):
        """Test when profiles.yml doesn't exist anywhere."""
        # Create a temp project dir without profiles.yml
        shutil.copy(
            fixtures_path / "valid_dbt_project.yml",
            tmp_path / "dbt_project.yml",
        )

        # Create parser with mocked standard locations that don't exist
        parser = DbtProfileParser(project_dir=tmp_path)

        # Override the PROFILES_LOCATIONS to empty list so we don't find real ~/.dbt/profiles.yml
        parser.PROFILES_LOCATIONS = []

        profiles_path = parser.find_profiles_yml()
        assert profiles_path is None


class TestDbtProfileParserParseProfile:
    """Tests for parsing profiles."""

    @pytest.mark.parametrize(
        "temp_project_with_profile",
        ["valid_password_profile.yml"],
        indirect=True,
    )
    def test_parse_profile_password_auth(self, temp_project_with_profile):
        """Test parsing a profile with password authentication."""
        parser = DbtProfileParser(project_dir=temp_project_with_profile)
        config = parser.parse_profile(target="dev")

        assert config.account == "test_account"
        assert config.user == "test_user"
        assert config.password == "test_password"
        assert config.role == "TEST_ROLE"
        assert config.warehouse == "TEST_WH"
        assert config.database == "TEST_DB"
        assert config.schema == "TEST_SCHEMA"
        assert config.profile_name == "test_project"
        assert config.target_name == "dev"

    @pytest.mark.parametrize(
        "temp_project_with_profile",
        ["valid_password_profile.yml"],
        indirect=True,
    )
    def test_parse_profile_uses_default_target(self, temp_project_with_profile):
        """Test that default target is used when not specified."""
        parser = DbtProfileParser(project_dir=temp_project_with_profile)
        config = parser.parse_profile()  # No target specified

        assert config.target_name == "dev"  # Default from profile

    @pytest.mark.parametrize(
        "temp_project_with_profile",
        ["valid_password_profile.yml"],
        indirect=True,
    )
    def test_parse_profile_specific_target(self, temp_project_with_profile):
        """Test parsing a specific target."""
        parser = DbtProfileParser(project_dir=temp_project_with_profile)
        config = parser.parse_profile(target="prod")

        assert config.database == "PROD_DB"
        assert config.schema == "PROD_SCHEMA"
        assert config.target_name == "prod"

    @pytest.mark.parametrize(
        "temp_project_with_profile",
        ["valid_keypair_profile.yml"],
        indirect=True,
    )
    def test_parse_profile_keypair_auth(self, temp_project_with_profile):
        """Test parsing a profile with key pair authentication."""
        parser = DbtProfileParser(project_dir=temp_project_with_profile)
        config = parser.parse_profile(target="dev")

        assert config.private_key_path == "~/.ssh/snowflake_key.p8"
        assert config.private_key_passphrase == "test_passphrase"
        assert config.password is None

    @pytest.mark.parametrize(
        "temp_project_with_profile",
        ["valid_sso_profile.yml"],
        indirect=True,
    )
    def test_parse_profile_sso_auth(self, temp_project_with_profile):
        """Test parsing a profile with SSO authentication."""
        parser = DbtProfileParser(project_dir=temp_project_with_profile)
        config = parser.parse_profile(target="dev")

        assert config.authenticator == "externalbrowser"
        assert config.password is None
        assert config.private_key_path is None

    @pytest.mark.parametrize(
        "temp_project_with_profile",
        ["invalid_missing_account.yml"],
        indirect=True,
    )
    def test_parse_profile_missing_required_field(self, temp_project_with_profile):
        """Test error when required field is missing."""
        parser = DbtProfileParser(project_dir=temp_project_with_profile)
        with pytest.raises(DbtProfileParseError) as exc_info:
            parser.parse_profile(target="dev")
        assert "account" in str(exc_info.value).lower()

    @pytest.mark.parametrize(
        "temp_project_with_profile",
        ["invalid_wrong_type.yml"],
        indirect=True,
    )
    def test_parse_profile_wrong_db_type(self, temp_project_with_profile):
        """Test error when profile is not Snowflake type."""
        parser = DbtProfileParser(project_dir=temp_project_with_profile)
        with pytest.raises(DbtProfileParseError) as exc_info:
            parser.parse_profile(target="dev")
        assert "snowflake" in str(exc_info.value).lower()


class TestDbtProfileParserEnvVarResolution:
    """Tests for environment variable resolution."""

    def test_resolve_env_var_simple(self):
        """Test resolving simple env_var() syntax."""
        parser = DbtProfileParser()

        with patch.dict(os.environ, {"MY_VAR": "resolved_value"}):
            result = parser.resolve_env_var("{{ env_var('MY_VAR') }}")
            assert result == "resolved_value"

    def test_resolve_env_var_with_default(self):
        """Test resolving env_var() with default value when var not set."""
        parser = DbtProfileParser()

        result = parser.resolve_env_var("{{ env_var('NONEXISTENT_VAR', 'default_value') }}")
        assert result == "default_value"

    def test_resolve_env_var_no_default_not_set(self):
        """Test resolving env_var() without default when var not set."""
        parser = DbtProfileParser()

        result = parser.resolve_env_var("{{ env_var('NONEXISTENT_VAR') }}")
        assert result == ""

    def test_resolve_env_var_double_quotes(self):
        """Test resolving env_var() with double quotes."""
        parser = DbtProfileParser()

        with patch.dict(os.environ, {"MY_VAR": "value"}):
            result = parser.resolve_env_var('{{ env_var("MY_VAR") }}')
            assert result == "value"

    def test_resolve_env_var_with_spaces(self):
        """Test resolving env_var() with various spacing."""
        parser = DbtProfileParser()

        with patch.dict(os.environ, {"MY_VAR": "value"}):
            # Extra spaces
            result = parser.resolve_env_var("{{  env_var( 'MY_VAR' )  }}")
            assert result == "value"

    def test_resolve_env_var_in_string(self):
        """Test resolving env_var() embedded in a string."""
        parser = DbtProfileParser()

        with patch.dict(os.environ, {"ACCOUNT": "my_account"}):
            result = parser.resolve_env_var("prefix_{{ env_var('ACCOUNT') }}_suffix")
            assert result == "prefix_my_account_suffix"

    def test_non_string_value_passthrough(self):
        """Test that non-string values are passed through unchanged."""
        parser = DbtProfileParser()

        assert parser.resolve_env_var(123) == 123
        assert parser.resolve_env_var(True) is True
        assert parser.resolve_env_var(None) is None

    @pytest.mark.parametrize(
        "temp_project_with_profile",
        ["with_env_vars_profile.yml"],
        indirect=True,
    )
    def test_parse_profile_with_env_vars(self, temp_project_with_profile):
        """Test parsing a profile that uses env_var() syntax."""
        parser = DbtProfileParser(project_dir=temp_project_with_profile)

        with patch.dict(
            os.environ,
            {
                "TEST_SF_ACCOUNT": "env_account",
                "TEST_SF_USER": "env_user",
                "TEST_SF_PASSWORD": "env_password",
            },
        ):
            config = parser.parse_profile(target="dev")

            assert config.account == "env_account"
            assert config.user == "env_user"
            assert config.password == "env_password"
            # Role uses default value since env var not set
            assert config.role == "DEFAULT_ROLE"


class TestDbtProfileConfig:
    """Tests for DbtProfileConfig."""

    def test_get_auth_method_password(self):
        """Test auth method detection for password."""
        config = DbtProfileConfig(
            account="test",
            user="test",
            password="test_pass",
        )
        assert config.get_auth_method() == "password"

    def test_get_auth_method_keypair(self):
        """Test auth method detection for key pair."""
        config = DbtProfileConfig(
            account="test",
            user="test",
            private_key_path="/path/to/key",
        )
        assert config.get_auth_method() == "key_pair"

    def test_get_auth_method_sso(self):
        """Test auth method detection for SSO."""
        config = DbtProfileConfig(
            account="test",
            user="test",
            authenticator="externalbrowser",
        )
        assert config.get_auth_method() == "sso_browser"

    def test_get_auth_method_oauth(self):
        """Test auth method detection for OAuth."""
        config = DbtProfileConfig(
            account="test",
            user="test",
            authenticator="oauth",
        )
        assert config.get_auth_method() == "oauth"

    def test_get_auth_method_default(self):
        """Test default auth method when nothing specified."""
        config = DbtProfileConfig(
            account="test",
            user="test",
        )
        # Defaults to SSO browser
        assert config.get_auth_method() == "sso_browser"
