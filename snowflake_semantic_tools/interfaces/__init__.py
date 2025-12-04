"""
Interfaces Layer

User-facing entry points for interacting with Snowflake Semantic Tools.

## Architecture

The interfaces layer provides multiple ways to access the semantic tools functionality:

### Command-Line Interface (CLI)
Primary interface for operations teams and CI/CD pipelines:
- `sst extract` - Parse and load semantic metadata to Snowflake
- `sst validate` - Validate semantic models against dbt definitions
- `sst generate` - Create semantic views and YAML models

### Python API
Programmatic interface for integration with other tools:
- SemanticMetadataCollectionBuilder - Build semantic models programmatically
- Direct service access for custom workflows

Both interfaces provide consistent access to the underlying services while
tailoring the user experience to different use cases and environments.
"""

from snowflake_semantic_tools.interfaces import cli

__all__ = ["cli"]
