"""
Unit tests for snowflake_session CLI helper.

Tests the centralized session management:
- Session creation and cleanup
- Exception handling
- Config loading from dbt profile
"""

from unittest.mock import MagicMock, patch

import click
import pytest

from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig


class TestSnowflakeSession:
    """Test centralized session helper."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock SnowflakeConfig."""
        config = MagicMock(spec=SnowflakeConfig)
        config.account = "test_account"
        config.user = "test_user"
        config.profile_name = "test_profile"
        config.target_name = "dev"
        return config

    @pytest.fixture
    def mock_client(self):
        """Create a mock SnowflakeClient."""
        client = MagicMock()
        return client

    def test_session_creates_and_closes_client(self, mock_config, mock_client):
        """Session should properly manage client lifecycle."""
        with patch(
            "snowflake_semantic_tools.interfaces.cli.utils.build_snowflake_config",
            return_value=mock_config,
        ):
            with patch(
                "snowflake_semantic_tools.interfaces.cli.utils.SnowflakeClient",
                return_value=mock_client,
            ):
                from snowflake_semantic_tools.interfaces.cli.utils import snowflake_session

                with snowflake_session(target="dev") as client:
                    assert client is mock_client

                # close() should be called on exit
                mock_client.close.assert_called_once()

    def test_session_closes_on_exception(self, mock_config, mock_client):
        """Connection should be closed even when exception raised."""
        with patch(
            "snowflake_semantic_tools.interfaces.cli.utils.build_snowflake_config",
            return_value=mock_config,
        ):
            with patch(
                "snowflake_semantic_tools.interfaces.cli.utils.SnowflakeClient",
                return_value=mock_client,
            ):
                from snowflake_semantic_tools.interfaces.cli.utils import snowflake_session

                with pytest.raises(ValueError, match="Test error"):
                    with snowflake_session(target="dev") as client:
                        raise ValueError("Test error")

                # close() should still be called
                mock_client.close.assert_called_once()

    def test_session_uses_provided_config(self, mock_config, mock_client):
        """Session should use provided config instead of loading from profile."""
        with patch(
            "snowflake_semantic_tools.interfaces.cli.utils.SnowflakeClient",
            return_value=mock_client,
        ) as mock_client_class:
            from snowflake_semantic_tools.interfaces.cli.utils import snowflake_session

            with snowflake_session(config=mock_config) as client:
                pass

            # Should create client with provided config
            mock_client_class.assert_called_once_with(mock_config)

    def test_session_loads_config_from_profile(self, mock_config, mock_client):
        """Session should auto-load config from dbt profile when not provided."""
        with patch(
            "snowflake_semantic_tools.interfaces.cli.utils.build_snowflake_config",
            return_value=mock_config,
        ) as mock_build:
            with patch(
                "snowflake_semantic_tools.interfaces.cli.utils.SnowflakeClient",
                return_value=mock_client,
            ):
                from snowflake_semantic_tools.interfaces.cli.utils import snowflake_session

                with snowflake_session(target="prod", database_override="MYDB") as client:
                    pass

                # Should call build_snowflake_config with correct args
                mock_build.assert_called_once()
                call_kwargs = mock_build.call_args[1]
                assert call_kwargs["target"] == "prod"
                assert call_kwargs["database"] == "MYDB"

    def test_session_raises_click_exception_on_config_failure(self):
        """Session should raise ClickException when config loading fails."""
        with patch(
            "snowflake_semantic_tools.interfaces.cli.utils.build_snowflake_config",
            side_effect=click.ClickException("Profile not found"),
        ):
            from snowflake_semantic_tools.interfaces.cli.utils import snowflake_session

            with pytest.raises(click.ClickException, match="Profile not found"):
                with snowflake_session(target="nonexistent") as client:
                    pass


class TestSnowflakeClientContextManager:
    """Test SnowflakeClient context manager support."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock SnowflakeConfig."""
        config = MagicMock(spec=SnowflakeConfig)
        config.account = "test_account"
        config.user = "test_user"
        config.connection_params = {"account": "test_account", "user": "test_user"}
        return config

    def test_client_context_manager_closes_connection(self, mock_config):
        """SnowflakeClient context manager should close connection on exit."""
        with patch("snowflake.connector.connect"):
            from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeClient

            with SnowflakeClient(mock_config) as client:
                # Mock the connection manager's close
                client.connection_manager.close = MagicMock()

            # close() should be called
            client.connection_manager.close.assert_called_once()

    def test_client_context_manager_closes_on_exception(self, mock_config):
        """SnowflakeClient context manager should close even on exception."""
        with patch("snowflake.connector.connect"):
            from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeClient

            with pytest.raises(RuntimeError, match="Test error"):
                with SnowflakeClient(mock_config) as client:
                    client.connection_manager.close = MagicMock()
                    close_mock = client.connection_manager.close
                    raise RuntimeError("Test error")

            # close() should still be called
            close_mock.assert_called_once()
