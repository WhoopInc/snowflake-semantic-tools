"""
dbt Infrastructure Module

Provides abstraction layer for dbt CLI commands (Core and Cloud CLI)
and profile parsing for Snowflake authentication.

Handles:
- Command execution and error handling
- dbt Core vs Cloud CLI detection
- Profile parsing from profiles.yml
- Environment variable resolution in profiles

This allows SST to work seamlessly with both dbt Core and dbt Cloud CLI.
"""

from snowflake_semantic_tools.infrastructure.dbt.client import DbtClient, DbtResult, DbtType
from snowflake_semantic_tools.infrastructure.dbt.exceptions import (
    DbtCompileError,
    DbtError,
    DbtNotFoundError,
    DbtProfileError,
    DbtProfileNotFoundError,
    DbtProfileParseError,
    DbtProjectNotFoundError,
)
from snowflake_semantic_tools.infrastructure.dbt.profile_parser import DbtProfileConfig, DbtProfileParser

__all__ = [
    # Client
    "DbtClient",
    "DbtResult",
    "DbtType",
    # Profile parsing
    "DbtProfileParser",
    "DbtProfileConfig",
    # Exceptions
    "DbtError",
    "DbtNotFoundError",
    "DbtCompileError",
    "DbtProfileError",
    "DbtProfileNotFoundError",
    "DbtProfileParseError",
    "DbtProjectNotFoundError",
]
