#!/usr/bin/env python3
"""
Snowflake Connection Manager

Handles Snowflake database connections with persistent connection reuse.
Implements connection pooling with health checks and auto-reconnect for
optimal performance and reduced authentication overhead.

Key features:
- Persistent connection reuse within a session
- Lightweight health checks before reuse
- Automatic reconnection on stale connections
- Explicit cleanup via close() method
"""

import os
from contextlib import contextmanager
from typing import Any, Dict, Optional

import snowflake.connector

from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("snowflake.connection_manager")


class ConnectionManager:
    """
    Manages Snowflake database connections with persistent connection reuse.

    Instead of creating a new connection for every operation (which triggers
    SSO authentication each time), this manager maintains a persistent connection
    that is reused across multiple operations within a session.

    The connection is:
    - Created lazily on first use
    - Validated with a health check before reuse
    - Automatically reconnected if stale or disconnected
    - Explicitly closed via close() method

    Usage:
        manager = ConnectionManager(config)
        with manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
        # Connection stays open for next operation

        # When done with all operations:
        manager.close()
    """

    def __init__(self, config: SnowflakeConfig):
        """
        Initialize the connection manager.

        Args:
            config: SnowflakeConfig instance with connection parameters
        """
        self.config = config
        self._connection: Optional[Any] = None  # Persistent connection
        self._first_connection_logged = False  # Track if we've logged initial connection

    @property
    def connection_params(self) -> Dict[str, Any]:
        """Get current connection parameters from config."""
        # Use the connection_params property from SnowflakeConfig
        # This already has all auth configured - don't re-read from environment!
        params = self.config.connection_params.copy()

        # Handle private key authentication file path
        if self.config.private_key_path:
            params["private_key_file"] = self.config.private_key_path
            # Private key password should already be in config, but check env as fallback
            if not params.get("private_key_file_pwd"):
                key_password = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSWORD")
                if key_password:
                    params["private_key_file_pwd"] = key_password
            logger.debug("Using RSA key authentication")
        elif self.config.password:
            logger.debug("Using password authentication")
        elif self.config.authenticator:
            logger.debug(f"Using {self.config.authenticator} authentication")

        return params

    def _is_connection_alive(self) -> bool:
        """
        Check if the persistent connection is still valid.

        Uses a lightweight SELECT 1 query to verify the connection
        is responsive without causing significant overhead.

        Returns:
            True if connection is alive and responsive, False otherwise
        """
        if self._connection is None:
            return False
        try:
            cursor = self._connection.cursor()
            try:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            finally:
                cursor.close()
            return True
        except Exception:
            logger.debug("Connection health check failed, will reconnect")
            return False

    def _connect(self) -> None:
        """
        Create a new connection to Snowflake.

        Closes any existing connection before creating a new one.
        Only logs the first connection to avoid spam in logs.
        """
        # Close existing connection if any
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass  # Ignore errors when closing stale connection
            self._connection = None

        # Log first connection (subsequent reconnects are silent unless debug)
        if not self._first_connection_logged:
            logger.info(f"Connecting to Snowflake as {self.config.user}@{self.config.account}")
            logger.debug(f"Connection params: account={self.config.account}, authenticator={self.config.authenticator}")
            self._first_connection_logged = True
        else:
            logger.debug("Reconnecting to Snowflake...")

        self._connection = snowflake.connector.connect(**self.connection_params)

    @contextmanager
    def get_connection(self):
        """
        Get or reuse the persistent Snowflake connection.

        This context manager:
        1. Checks if existing connection is alive
        2. Creates new connection if needed
        3. Yields the connection for use
        4. Does NOT close the connection on exit (for reuse)
        5. Marks connection as stale on errors

        Yields:
            Active Snowflake connection

        Raises:
            Exception: If connection fails (with helpful error context)
        """
        try:
            # Check if we need to connect or reconnect
            if not self._is_connection_alive():
                self._connect()

            yield self._connection

        except Exception as e:
            # Mark connection as stale so next call will reconnect
            self._connection = None

            error_msg = str(e).lower()

            # Provide helpful context for common errors
            if "not authorized" in error_msg or "does not exist" in error_msg:
                logger.error(f"Failed to connect to Snowflake: {e}")
                logger.error(
                    f"Connection details: user={self.config.user}, account={self.config.account}, role={self.config.role}"
                )

                # Check if it's a permission issue
                if "not authorized" in error_msg:
                    logger.error("")
                    logger.error("This appears to be a PERMISSION issue.")
                    logger.error(f"Your role '{self.config.role}' may not have access to the requested resource.")
                    logger.error("")
                    logger.error("Try updating 'role:' in your dbt profiles.yml to a role with broader permissions:")
                    logger.error("  role: ACCOUNTADMIN")
                    logger.error("")
            else:
                logger.error(f"Failed to connect to Snowflake: {e}")
                logger.debug(f"Connection params: user={self.config.user}, account={self.config.account}")

            raise

    def close(self) -> None:
        """
        Explicitly close the persistent connection.

        Should be called when done with all Snowflake operations to
        release the connection. Safe to call multiple times.
        """
        if self._connection is not None:
            try:
                self._connection.close()
                logger.debug("Snowflake connection closed")
            except Exception:
                pass  # Ignore errors during cleanup
            self._connection = None

    def test_connection(self) -> bool:
        """Test Snowflake connection and return success status."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT CURRENT_VERSION()")
                result = cursor.fetchone()
                logger.info(f"Snowflake connection successful - Version: {result[0]}")
                return True
        except Exception as e:
            logger.error(f"Snowflake connection test failed: {e}")
            return False

    def get_database_info(self) -> Dict[str, Any]:
        """Get information about the current database connection."""
        try:
            with self.get_connection() as conn:
                from snowflake.connector import DictCursor

                cursor = conn.cursor(DictCursor)

                # Get basic connection info
                cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
                result = cursor.fetchone()

                return {
                    "database": result["CURRENT_DATABASE()"],
                    "schema": result["CURRENT_SCHEMA()"],
                    "warehouse": result["CURRENT_WAREHOUSE()"],
                    "role": result["CURRENT_ROLE()"],
                    "account": self.config.account,
                    "user": self.config.user,
                }

        except Exception as e:
            logger.error(f"Failed to get database info: {e}")
            return {}
