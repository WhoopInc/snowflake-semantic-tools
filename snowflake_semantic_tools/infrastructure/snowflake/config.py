"""
Snowflake Configuration

Configuration models for Snowflake connections with multi-authentication support.

Provides a centralized configuration model that supports various authentication
methods required in different environments:
- Development: Password or SSO authentication
- Production: RSA key pair authentication
- CI/CD: Environment variable configuration
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class SnowflakeConfig:
    """
    Comprehensive configuration for Snowflake connections.

    Encapsulates all connection parameters and authentication methods,
    providing a unified interface for Snowflake connectivity across
    different environments and authentication scenarios.

    Authentication Priority:
    1. Password (if provided)
    2. RSA private key (if path provided)
    3. SSO/External browser (if authenticator specified)

    The configuration automatically uppercases database and schema names
    to handle Snowflake's case-sensitivity requirements correctly.
    """

    account: str
    user: str
    role: str
    warehouse: str
    database: str
    schema: str

    # Authentication options
    password: Optional[str] = None
    private_key_path: Optional[str] = None
    authenticator: Optional[str] = None

    # Connection options
    timeout: int = 30
    max_retries: int = 3

    @property
    def connection_params(self) -> dict:
        """Get connection parameters for snowflake-connector."""
        params = {
            "account": self.account,
            "user": self.user,
            "role": self.role,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }

        # Support insecure mode for environments with certificate issues
        if os.getenv("SNOWFLAKE_INSECURE_MODE", "").lower() in ("true", "1", "yes"):
            params["insecure_mode"] = True

        # Add authentication
        if self.password:
            params["password"] = self.password
        elif self.private_key_path:
            params["private_key_path"] = self.private_key_path
        elif self.authenticator:
            params["authenticator"] = self.authenticator

        return params

    @property
    def fully_qualified_schema(self) -> str:
        """Get fully qualified schema name."""
        return f"{self.database}.{self.schema}"

    @staticmethod
    def detect_auth_method(verbose: bool = False) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Detect authentication method from environment variables.

        Checks in order of preference:
        1. Password authentication
        2. RSA private key authentication
        3. Browser SSO (externalbrowser)

        Args:
            verbose: If True, print which method is being used

        Returns:
            Tuple of (password, private_key_path, authenticator)
        """
        import os

        password = os.getenv("SNOWFLAKE_PASSWORD")
        private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR")

        # Smart fallback: if no password or key, use browser SSO
        if not password and not private_key_path and not authenticator:
            authenticator = "externalbrowser"
            if verbose:
                # Use click for consistent formatting (no color - just info)
                import click

                click.echo("Using SSO authentication (browser will open)")
        elif password:
            if verbose:
                import click

                click.echo("Using password authentication")
        elif private_key_path:
            if verbose:
                import click

                click.echo("Using private key authentication")
        elif authenticator:
            if verbose:
                import click

                click.echo(f"Using {authenticator} authentication")

        return (password, private_key_path, authenticator)

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        """
        Create configuration from environment variables.

        Required environment variables:
        - SNOWFLAKE_ACCOUNT
        - SNOWFLAKE_USER or SNOWFLAKE_USERNAME (dbt compatibility)
        - SNOWFLAKE_ROLE
        - SNOWFLAKE_WAREHOUSE
        - SNOWFLAKE_DATABASE
        - SNOWFLAKE_SCHEMA

        Optional:
        - SNOWFLAKE_PASSWORD
        - SNOWFLAKE_PRIVATE_KEY_PATH
        - SNOWFLAKE_AUTHENTICATOR

        Returns:
            SnowflakeConfig instance populated from environment

        Raises:
            ValueError: If required environment variables are missing
        """
        import os

        # Support both SNOWFLAKE_USER and SNOWFLAKE_USERNAME (dbt uses USERNAME)
        # Prefer SNOWFLAKE_USER for backward compatibility
        user = os.getenv("SNOWFLAKE_USER") or os.getenv("SNOWFLAKE_USERNAME")

        # Check required environment variables
        required_vars = {
            "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
            "SNOWFLAKE_USER": user,
            "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE"),
            "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
            "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
        }

        missing = [k for k, v in required_vars.items() if not v]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        # Detect authentication method
        password, private_key_path, authenticator = cls.detect_auth_method()

        return cls(
            account=required_vars["SNOWFLAKE_ACCOUNT"],
            user=required_vars["SNOWFLAKE_USER"],
            role=required_vars["SNOWFLAKE_ROLE"],
            warehouse=required_vars["SNOWFLAKE_WAREHOUSE"],
            database=required_vars["SNOWFLAKE_DATABASE"],
            schema=required_vars["SNOWFLAKE_SCHEMA"],
            password=password,
            private_key_path=private_key_path,
            authenticator=authenticator,
        )
