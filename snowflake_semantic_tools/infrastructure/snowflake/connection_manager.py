#!/usr/bin/env python3
"""
Snowflake Connection Manager

Handles Snowflake database connections, connection parameters, and connection testing.
Provides connection context managers and validates connectivity.
"""

import os
from contextlib import contextmanager
from typing import Any, Dict, Optional

import snowflake.connector

from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("snowflake.connection_manager")


class ConnectionManager:
    """Manages Snowflake database connections and connection parameters."""

    def __init__(self, config: SnowflakeConfig):
        """
        Initialize the connection manager.

        Args:
            config: SnowflakeConfig instance
        """
        self.config = config
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

    @contextmanager
    def get_connection(self):
        """Context manager for Snowflake database connections."""
        conn = None
        try:
            # Only log first connection to avoid spam (can be 100s of connections per command)
            if not self._first_connection_logged:
                logger.info(f"Connecting to Snowflake as {self.config.user}@{self.config.account}")
                logger.debug(
                    f"Connection params: account={self.config.account}, authenticator={self.config.authenticator}"
                )
                self._first_connection_logged = True

            conn = snowflake.connector.connect(**self.connection_params)

            yield conn
        except Exception as e:
            error_msg = str(e).lower()

            # Provide helpful context for common errors
            if "not authorized" in error_msg or "does not exist" in error_msg:
                logger.error(f"Failed to connect to Snowflake: {e}")
                logger.error(
                    f"Connection details: user={self.config.user}, account={self.config.account}, role={self.config.role}"
                )

                # Check if it's a permission issue
                if "not authorized" in error_msg:
                    logger.error(f"")
                    logger.error(f"This appears to be a PERMISSION issue.")
                    logger.error(f"Your role '{self.config.role}' may not have access to the requested resource.")
                    logger.error(f"")
                    logger.error(f"Try setting SNOWFLAKE_ROLE to a role with broader permissions:")
                    logger.error(f"  export SNOWFLAKE_ROLE=ACCOUNTADMIN")
                    logger.error(f"")
            else:
                logger.error(f"Failed to connect to Snowflake: {e}")
                logger.debug(f"Connection params: user={self.config.user}, account={self.config.account}")

            raise
        finally:
            if conn:
                conn.close()
                # No longer log every connection close - too much noise

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
