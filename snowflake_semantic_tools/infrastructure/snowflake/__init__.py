"""
Snowflake Infrastructure

Comprehensive Snowflake database operations and connectivity layer.

## Core Components

- **SnowflakeConfig**: Configuration model for connection parameters
- **SnowflakeClient**: Main client for all database operations
- **ConnectionManager**: Connection pooling and lifecycle management

## Specialized Managers

- **SchemaManager**: Database and schema creation/validation
- **TableManager**: Table operations and metadata queries
- **DataLoader**: Bulk data loading and semantic model extraction
- **MetadataManager**: Schema inspection, sample values, and enrichment queries
- **CortexSearchManager**: Cortex Search Service configuration

## Authentication Support

- Password-based authentication
- RSA key pair authentication
- SSO/External browser authentication
- Environment variable configuration

The infrastructure layer handles all Snowflake-specific concerns including
connection management, query execution, error handling, and data type conversions.
"""

from snowflake_semantic_tools.infrastructure.snowflake.client import SnowflakeClient
from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig
from snowflake_semantic_tools.infrastructure.snowflake.connection_manager import ConnectionManager
from snowflake_semantic_tools.infrastructure.snowflake.metadata_manager import MetadataManager

__all__ = ["SnowflakeClient", "SnowflakeConfig", "ConnectionManager", "MetadataManager"]
