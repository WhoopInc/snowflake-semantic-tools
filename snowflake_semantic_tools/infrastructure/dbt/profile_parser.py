"""
dbt Profile Parser

Parses dbt profiles.yml for Snowflake connection credentials.

This module reads dbt's standard configuration files to extract Snowflake
connection parameters, providing a single source of truth for authentication
that aligns with dbt conventions.

Supported authentication methods:
- Password authentication
- Key pair authentication (RSA)
- SSO/External browser authentication
- OAuth authentication

Environment variable interpolation is supported via dbt's {{ env_var() }} syntax.
"""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from snowflake_semantic_tools.infrastructure.dbt.exceptions import (
    DbtProfileNotFoundError,
    DbtProfileParseError,
    DbtProjectNotFoundError,
)
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class DbtProfileConfig:
    """
    Parsed dbt profile configuration for Snowflake.

    Contains all connection parameters extracted from profiles.yml,
    with environment variables already resolved.
    """

    # Required fields
    account: str
    user: str

    # Optional connection fields
    role: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None

    # Authentication fields (one of these should be set)
    password: Optional[str] = None
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    authenticator: Optional[str] = None  # externalbrowser, oauth, snowflake_jwt
    token: Optional[str] = None  # for oauth

    # Metadata
    profile_name: Optional[str] = None
    target_name: Optional[str] = None

    def get_auth_method(self) -> str:
        """Return a string describing the authentication method."""
        if self.password:
            return "password"
        elif self.private_key_path:
            return "key_pair"
        elif self.authenticator == "externalbrowser":
            return "sso_browser"
        elif self.authenticator == "oauth":
            return "oauth"
        elif self.authenticator == "snowflake_jwt":
            return "jwt"
        else:
            # Default to externalbrowser if nothing specified
            return "sso_browser"


# =============================================================================
# Profile Parser
# =============================================================================


class DbtProfileParser:
    """
    Parse dbt profiles.yml for Snowflake connection credentials.

    This parser reads dbt's standard configuration files to extract
    Snowflake connection parameters. It supports:

    - Reading profile name from dbt_project.yml
    - Locating profiles.yml in standard locations
    - Resolving {{ env_var() }} syntax
    - Extracting target-specific configurations

    Usage:
        parser = DbtProfileParser()
        config = parser.parse_profile(target="prod")
        print(f"Connecting to {config.account} as {config.user}")
    """

    # Standard locations for profiles.yml
    PROFILES_LOCATIONS = [
        Path.home() / ".dbt" / "profiles.yml",
    ]

    def __init__(self, project_dir: Optional[Path] = None):
        """
        Initialize the profile parser.

        Args:
            project_dir: Path to dbt project directory. Defaults to current directory.
        """
        self.project_dir = Path(project_dir) if project_dir else Path.cwd()
        self._profiles_cache: Optional[Dict] = None
        self._project_cache: Optional[Dict] = None

    def get_profile_name(self) -> str:
        """
        Extract the profile name from dbt_project.yml.

        Returns:
            Profile name string

        Raises:
            DbtProjectNotFoundError: If dbt_project.yml doesn't exist
            DbtProfileParseError: If profile name is not specified
        """
        project_file = self.project_dir / "dbt_project.yml"

        if not project_file.exists():
            raise DbtProjectNotFoundError(self.project_dir)

        if self._project_cache is None:
            try:
                with open(project_file, "r", encoding="utf-8") as f:
                    self._project_cache = yaml.safe_load(f) or {}
            except yaml.YAMLError as e:
                raise DbtProfileParseError(f"Invalid YAML in dbt_project.yml: {e}", project_file)

        profile_name = self._project_cache.get("profile")

        if not profile_name:
            raise DbtProfileParseError(
                "No 'profile:' field found in dbt_project.yml.\n\n"
                "Add a profile reference:\n"
                "  profile: 'your_profile_name'\n",
                project_file,
            )

        logger.debug(f"Found profile name in dbt_project.yml: {profile_name}")
        return profile_name

    def find_profiles_yml(self) -> Optional[Path]:
        """
        Locate profiles.yml in standard locations.

        Search order:
        1. Project directory (./profiles.yml)
        2. User's dbt directory (~/.dbt/profiles.yml)

        Returns:
            Path to profiles.yml or None if not found
        """
        search_paths = []

        # Check project directory first
        project_profiles = self.project_dir / "profiles.yml"
        search_paths.append(project_profiles)
        if project_profiles.exists():
            logger.debug(f"Found profiles.yml in project directory: {project_profiles}")
            return project_profiles

        # Check standard locations
        for path in self.PROFILES_LOCATIONS:
            search_paths.append(path)
            if path.exists():
                logger.debug(f"Found profiles.yml: {path}")
                return path

        logger.debug(f"No profiles.yml found. Searched: {[str(p) for p in search_paths]}")
        return None

    def resolve_env_var(self, value: Any) -> Any:
        """
        Resolve {{ env_var('NAME', 'default') }} syntax in a value.

        Supports dbt's Jinja-style environment variable interpolation:
        - {{ env_var('VAR_NAME') }} - Required env var
        - {{ env_var('VAR_NAME', 'default') }} - With default value

        Args:
            value: Value to process (string or other type)

        Returns:
            Resolved value with environment variables substituted
        """
        if not isinstance(value, str):
            return value

        # Pattern matches: {{ env_var('NAME') }} or {{ env_var('NAME', 'default') }}
        # Supports both single and double quotes
        pattern = r"\{\{\s*env_var\s*\(\s*['\"]([^'\"]+)['\"]\s*(?:,\s*['\"]([^'\"]*)['\"])?\s*\)\s*\}\}"

        def replacer(match):
            var_name = match.group(1)
            default = match.group(2)

            env_value = os.getenv(var_name)

            if env_value is not None:
                return env_value
            elif default is not None:
                return default
            else:
                # Environment variable not set and no default provided
                logger.warning(f"Environment variable '{var_name}' not set and no default provided")
                return ""

        resolved = re.sub(pattern, replacer, value)

        # Log if we resolved anything (but don't log the actual value for security)
        if resolved != value:
            logger.debug(f"Resolved env_var in profile value")

        return resolved

    def _resolve_dict_env_vars(self, d: Dict) -> Dict:
        """Recursively resolve env vars in a dictionary."""
        result = {}
        for key, value in d.items():
            if isinstance(value, dict):
                result[key] = self._resolve_dict_env_vars(value)
            elif isinstance(value, list):
                result[key] = [self.resolve_env_var(v) if isinstance(v, str) else v for v in value]
            else:
                result[key] = self.resolve_env_var(value)
        return result

    def _load_profiles(self) -> Dict:
        """Load and cache profiles.yml content."""
        if self._profiles_cache is not None:
            return self._profiles_cache

        profiles_path = self.find_profiles_yml()

        if profiles_path is None:
            # Build list of searched paths for error message
            searched = [self.project_dir / "profiles.yml"] + self.PROFILES_LOCATIONS
            raise DbtProfileNotFoundError([str(p) for p in searched])

        try:
            with open(profiles_path, "r", encoding="utf-8") as f:
                self._profiles_cache = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise DbtProfileParseError(f"Invalid YAML: {e}", profiles_path)

        logger.debug(f"Loaded profiles from: {profiles_path}")
        return self._profiles_cache

    def parse_profile(self, target: Optional[str] = None) -> DbtProfileConfig:
        """
        Parse profile for specified target (or default).

        Args:
            target: Target name (e.g., 'dev', 'prod'). If None, uses default target.

        Returns:
            DbtProfileConfig with resolved connection parameters

        Raises:
            DbtProfileNotFoundError: If profiles.yml doesn't exist
            DbtProfileParseError: If profile or target is invalid
        """
        profile_name = self.get_profile_name()
        profiles = self._load_profiles()

        # Get the profile
        if profile_name not in profiles:
            available = [k for k in profiles.keys() if not k.startswith("config")]
            raise DbtProfileParseError(
                f"Profile '{profile_name}' not found in profiles.yml.\n\n"
                f"Available profiles: {', '.join(available) if available else '(none)'}\n\n"
                f"Make sure the 'profile:' in dbt_project.yml matches a profile in profiles.yml.",
                self.find_profiles_yml(),
            )

        profile = profiles[profile_name]

        # Determine target
        if target is None:
            target = profile.get("target")
            if not target:
                raise DbtProfileParseError(
                    f"No default target specified in profile '{profile_name}'.\n\n"
                    "Add a 'target:' field to your profile or use --target flag.",
                    self.find_profiles_yml(),
                )

        logger.debug(f"Using profile '{profile_name}' with target '{target}'")

        # Get outputs
        outputs = profile.get("outputs", {})
        if target not in outputs:
            available = list(outputs.keys())
            raise DbtProfileParseError(
                f"Target '{target}' not found in profile '{profile_name}'.\n\n"
                f"Available targets: {', '.join(available) if available else '(none)'}",
                self.find_profiles_yml(),
            )

        target_config = outputs[target]

        # Verify it's a Snowflake profile
        db_type = target_config.get("type", "").lower()
        if db_type != "snowflake":
            raise DbtProfileParseError(
                f"Profile '{profile_name}' target '{target}' is type '{db_type}', not 'snowflake'.\n\n"
                "SST only supports Snowflake connections.",
                self.find_profiles_yml(),
            )

        # Resolve environment variables in the config
        resolved_config = self._resolve_dict_env_vars(target_config)

        # Extract and validate required fields
        account = resolved_config.get("account")
        user = resolved_config.get("user")

        if not account:
            raise DbtProfileParseError(
                f"Missing required field 'account' in profile '{profile_name}' target '{target}'.",
                self.find_profiles_yml(),
            )

        if not user:
            raise DbtProfileParseError(
                f"Missing required field 'user' in profile '{profile_name}' target '{target}'.",
                self.find_profiles_yml(),
            )

        # Build config object
        config = DbtProfileConfig(
            account=account,
            user=user,
            role=resolved_config.get("role"),
            warehouse=resolved_config.get("warehouse"),
            database=resolved_config.get("database"),
            schema=resolved_config.get("schema"),
            password=resolved_config.get("password"),
            private_key_path=resolved_config.get("private_key_path"),
            private_key_passphrase=resolved_config.get("private_key_passphrase"),
            authenticator=resolved_config.get("authenticator"),
            token=resolved_config.get("token"),
            profile_name=profile_name,
            target_name=target,
        )

        logger.info(f"Parsed profile '{profile_name}' target '{target}' (auth: {config.get_auth_method()})")

        return config

    def get_searched_paths(self) -> list:
        """Return list of paths that would be searched for profiles.yml."""
        return [str(self.project_dir / "profiles.yml")] + [str(p) for p in self.PROFILES_LOCATIONS]
