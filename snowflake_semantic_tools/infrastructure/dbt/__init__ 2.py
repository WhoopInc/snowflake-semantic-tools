"""
dbt Infrastructure Module

Provides abstraction layer for dbt CLI commands (Core and Cloud CLI).

Handles subprocess communication with dbt, including:
- Command execution and error handling
- dbt Core vs Cloud CLI detection
- Consistent result formatting
- Environment variable management

This allows SST to work seamlessly with both dbt Core and dbt Cloud CLI.
"""

from snowflake_semantic_tools.infrastructure.dbt.client import DbtClient, DbtResult
from snowflake_semantic_tools.infrastructure.dbt.exceptions import DbtCompileError, DbtError, DbtNotFoundError

__all__ = [
    "DbtClient",
    "DbtResult",
    "DbtError",
    "DbtNotFoundError",
    "DbtCompileError",
]
