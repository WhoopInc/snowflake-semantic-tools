"""
Snowflake Configuration

Configuration models for Snowflake connections using dbt profiles.yml.

Provides a centralized configuration model that supports various authentication
methods defined in dbt's profiles.yml:
- Password authentication
- RSA key pair authentication
- SSO/External browser authentication
- OAuth authentication

All authentication is managed through ~/.dbt/profiles.yml, aligning with
dbt conventions and providing a single source of truth.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from snowflake_semantic_tools.infrastructure.dbt.exceptions import DbtProfileParseError
from snowflake_semantic_tools.infrastructure.dbt.profile_parser import DbtProfileParser


@dataclass
class SnowflakeConfig:
    """
    Comprehensive configuration for Snowflake connections.

    Encapsulates all connection parameters and authentication methods,
    providing a unified interface for Snowflake connectivity.

    Configuration is loaded from dbt's profiles.yml file using the
    `from_dbt_profile()` class method.

    Authentication Priority (from profiles.yml):
    1. Password (if provided)
    2. RSA private key (if path provided)
    3. SSO/External browser (default if nothing specified)
    """

    # Required fields
    account: str
    user: str

    # Connection context (can be overridden by CLI flags)
    database: str
    schema: str

    # Optional connection fields
    role: Optional[str] = None
    warehouse: Optional[str] = None

    # Authentication options
    password: Optional[str] = None
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    authenticator: Optional[str] = None
    token: Optional[str] = None  # For OAuth

    # Connection options
    timeout: int = 30
    max_retries: int = 3

    # Metadata from profile parsing
    profile_name: Optional[str] = None
    target_name: Optional[str] = None

    @property
    def connection_params(self) -> dict:
        """Get connection parameters for snowflake-connector."""
        params = {
            "account": self.account,
            "user": self.user,
            "database": self.database,
            "schema": self.schema,
        }

        # Add optional connection context
        if self.role:
            params["role"] = self.role
        if self.warehouse:
            params["warehouse"] = self.warehouse

        # Support insecure mode for environments with certificate issues
        if os.getenv("SNOWFLAKE_INSECURE_MODE", "").lower() in ("true", "1", "yes"):
            params["insecure_mode"] = True

        # Add authentication (priority order)
        if self.password:
            params["password"] = self.password
        elif self.private_key_path:
            params["private_key_path"] = self.private_key_path
            if self.private_key_passphrase:
                params["private_key_file_pwd"] = self.private_key_passphrase
        elif self.token:
            params["token"] = self.token
            params["authenticator"] = "oauth"
        elif self.authenticator:
            params["authenticator"] = self.authenticator
        else:
            # Default to browser SSO if no auth method specified
            params["authenticator"] = "externalbrowser"

        return params

    @property
    def fully_qualified_schema(self) -> str:
        """Get fully qualified schema name."""
        return f"{self.database}.{self.schema}"

    def get_auth_method(self) -> str:
        """Return a string describing the authentication method."""
        if self.password:
            return "password"
        elif self.private_key_path:
            return "key_pair"
        elif self.token:
            return "oauth"
        elif self.authenticator == "externalbrowser":
            return "sso_browser"
        elif self.authenticator:
            return self.authenticator
        else:
            return "sso_browser"

    @classmethod
    def from_dbt_profile(
        cls,
        target: Optional[str] = None,
        project_dir: Optional[Path] = None,
        database_override: Optional[str] = None,
        schema_override: Optional[str] = None,
        verbose: bool = False,
    ) -> "SnowflakeConfig":
        """
        Create configuration from dbt profiles.yml.

        This is the primary (and only) authentication method for SST.
        Reads connection parameters from the dbt profile specified in
        dbt_project.yml.

        Args:
            target: Target name (e.g., 'dev', 'prod'). If None, uses default.
            project_dir: Path to dbt project directory. Defaults to current dir.
            database_override: Override database from profile (for CLI --database flag)
            schema_override: Override schema from profile (for CLI --schema flag)
            verbose: If True, print which profile/target is being used

        Returns:
            SnowflakeConfig instance populated from dbt profile

        Raises:
            DbtProfileNotFoundError: If profiles.yml doesn't exist
            DbtProjectNotFoundError: If dbt_project.yml doesn't exist
            DbtProfileParseError: If profile is invalid or missing required fields

        Example:
            # Use default target
            config = SnowflakeConfig.from_dbt_profile()

            # Use specific target
            config = SnowflakeConfig.from_dbt_profile(target="prod")

            # Override database/schema for specific operation
            config = SnowflakeConfig.from_dbt_profile(
                database_override="ANALYTICS",
                schema_override="PRODUCTION"
            )
        """
        # Parse the profile
        parser = DbtProfileParser(project_dir=project_dir)
        profile_config = parser.parse_profile(target=target)

        # Determine database and schema (CLI overrides take precedence)
        database = database_override or profile_config.database
        schema = schema_override or profile_config.schema

        # Validate we have database and schema
        if not database:
            raise DbtProfileParseError(
                f"No database specified in profile '{profile_config.profile_name}' "
                f"target '{profile_config.target_name}'.\n\n"
                "Either add 'database:' to your profile or use --database flag.",
                parser.find_profiles_yml(),
            )

        if not schema:
            raise DbtProfileParseError(
                f"No schema specified in profile '{profile_config.profile_name}' "
                f"target '{profile_config.target_name}'.\n\n"
                "Either add 'schema:' to your profile or use --schema flag.",
                parser.find_profiles_yml(),
            )

        if verbose:
            import click

            auth_method = profile_config.get_auth_method()
            click.echo(
                f"Using dbt profile: {profile_config.profile_name} "
                f"(target: {profile_config.target_name}, auth: {auth_method})"
            )

        return cls(
            account=profile_config.account,
            user=profile_config.user,
            database=database.upper(),  # Snowflake convention
            schema=schema.upper(),  # Snowflake convention
            role=profile_config.role,
            warehouse=profile_config.warehouse,
            password=profile_config.password,
            private_key_path=profile_config.private_key_path,
            private_key_passphrase=profile_config.private_key_passphrase,
            authenticator=profile_config.authenticator,
            token=profile_config.token,
            profile_name=profile_config.profile_name,
            target_name=profile_config.target_name,
        )
