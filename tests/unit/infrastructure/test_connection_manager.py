"""
Unit tests for ConnectionManager connection pooling.

Tests the connection reuse implementation:
- Connection persistence across calls
- Health checks before reuse
- Auto-reconnect on stale connections
- Explicit cleanup
"""

from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig
from snowflake_semantic_tools.infrastructure.snowflake.connection_manager import ConnectionManager


@pytest.fixture
def mock_config():
    """Create a mock SnowflakeConfig."""
    config = MagicMock(spec=SnowflakeConfig)
    config.account = "test_account"
    config.user = "test_user"
    config.role = "test_role"
    config.authenticator = None
    config.password = "test_password"
    config.private_key_path = None
    config.connection_params = {
        "account": "test_account",
        "user": "test_user",
        "password": "test_password",
    }
    return config


@pytest.fixture
def mock_connection():
    """Create a mock Snowflake connection."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    conn.cursor.return_value = cursor
    return conn


class TestConnectionReuse:
    """Test connection persistence and reuse."""

    def test_connection_reused_across_calls(self, mock_config, mock_connection):
        """Same connection should be returned for multiple get_connection() calls."""
        with patch("snowflake.connector.connect", return_value=mock_connection) as mock_connect:
            manager = ConnectionManager(mock_config)

            # First call - creates connection
            with manager.get_connection() as conn1:
                pass

            # Second call - should reuse
            with manager.get_connection() as conn2:
                pass

            # Third call - should still reuse
            with manager.get_connection() as conn3:
                pass

            # Connection should only be created once
            assert mock_connect.call_count == 1
            assert conn1 is conn2 is conn3

    def test_stale_connection_triggers_reconnect(self, mock_config, mock_connection):
        """Dead connection should be detected and replaced."""
        with patch("snowflake.connector.connect", return_value=mock_connection) as mock_connect:
            manager = ConnectionManager(mock_config)

            # First call - creates connection
            with manager.get_connection() as conn1:
                pass

            assert mock_connect.call_count == 1

            # Simulate connection becoming stale (health check fails)
            mock_connection.cursor.return_value.execute.side_effect = Exception("Connection lost")

            # Create a fresh connection for the reconnect
            fresh_conn = MagicMock()
            fresh_cursor = MagicMock()
            fresh_cursor.fetchone.return_value = (1,)
            fresh_conn.cursor.return_value = fresh_cursor
            mock_connect.return_value = fresh_conn

            # Next call should reconnect
            with manager.get_connection() as conn2:
                pass

            # Should have connected twice (initial + reconnect)
            assert mock_connect.call_count == 2
            assert conn2 is fresh_conn

    def test_health_check_uses_lightweight_query(self, mock_config, mock_connection):
        """Health check should use SELECT 1, not expensive queries."""
        with patch("snowflake.connector.connect", return_value=mock_connection):
            manager = ConnectionManager(mock_config)

            # Force connection creation
            with manager.get_connection():
                pass

            # Second call triggers health check
            with manager.get_connection():
                pass

            # Check health check query was called
            cursor = mock_connection.cursor.return_value
            cursor.execute.assert_called_with("SELECT 1")


class TestConnectionHealthCheck:
    """Test connection health monitoring."""

    def test_healthy_connection_returns_true(self, mock_config, mock_connection):
        """Active connection should pass health check."""
        with patch("snowflake.connector.connect", return_value=mock_connection):
            manager = ConnectionManager(mock_config)
            manager._connection = mock_connection

            assert manager._is_connection_alive() is True

    def test_none_connection_returns_false(self, mock_config):
        """None connection should fail health check."""
        manager = ConnectionManager(mock_config)
        manager._connection = None

        assert manager._is_connection_alive() is False

    def test_error_during_health_check_returns_false(self, mock_config, mock_connection):
        """Query error should be treated as unhealthy."""
        mock_connection.cursor.return_value.execute.side_effect = Exception("Query failed")

        manager = ConnectionManager(mock_config)
        manager._connection = mock_connection

        assert manager._is_connection_alive() is False


class TestExplicitCleanup:
    """Test explicit connection cleanup."""

    def test_close_closes_connection(self, mock_config, mock_connection):
        """close() should properly close underlying connection."""
        with patch("snowflake.connector.connect", return_value=mock_connection):
            manager = ConnectionManager(mock_config)

            # Create connection
            with manager.get_connection():
                pass

            # Close should call connection.close()
            manager.close()

            mock_connection.close.assert_called_once()
            assert manager._connection is None

    def test_close_idempotent(self, mock_config, mock_connection):
        """Multiple close() calls should be safe."""
        with patch("snowflake.connector.connect", return_value=mock_connection):
            manager = ConnectionManager(mock_config)

            # Create connection
            with manager.get_connection():
                pass

            # Multiple closes should not raise
            manager.close()
            manager.close()
            manager.close()

            # Connection.close() should only be called once (first time)
            assert mock_connection.close.call_count == 1

    def test_close_handles_close_error(self, mock_config, mock_connection):
        """close() should handle errors during cleanup gracefully."""
        mock_connection.close.side_effect = Exception("Close failed")

        with patch("snowflake.connector.connect", return_value=mock_connection):
            manager = ConnectionManager(mock_config)

            # Create connection
            with manager.get_connection():
                pass

            # Should not raise even if close fails
            manager.close()
            assert manager._connection is None


class TestErrorHandling:
    """Test error handling in connection operations."""

    def test_error_marks_connection_stale(self, mock_config, mock_connection):
        """Errors during operations should mark connection as stale."""
        with patch("snowflake.connector.connect", return_value=mock_connection):
            manager = ConnectionManager(mock_config)

            # Create connection
            with manager.get_connection():
                pass

            assert manager._connection is not None

            # Simulate error during operation
            try:
                with manager.get_connection():
                    raise Exception("Operation failed")
            except Exception:
                pass

            # Connection should be marked as stale
            assert manager._connection is None

    def test_permission_error_logged_with_context(self, mock_config):
        """Permission errors should include helpful context."""
        with patch("snowflake.connector.connect") as mock_connect:
            # Make connect itself raise the error
            mock_connect.side_effect = Exception("not authorized to access database")

            manager = ConnectionManager(mock_config)

            with pytest.raises(Exception, match="not authorized"):
                with manager.get_connection():
                    pass
