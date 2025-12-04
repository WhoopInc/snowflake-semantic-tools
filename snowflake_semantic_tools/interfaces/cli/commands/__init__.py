"""
CLI Commands

Individual command implementations for semantic model operations.

## Core Commands

### Enrich (`sst enrich`)
Automatically enriches dbt YAML metadata with semantic information from Snowflake.
Populates column types, data types, sample values, and detects primary keys.

### Format (`sst format`)
Standardizes YAML file structure and formatting for consistency.
Ensures proper field ordering, indentation, and blank line spacing.

### Extract (`sst extract`)
Parses dbt and semantic model YAML files and loads metadata to Snowflake.
The foundation command that populates all semantic metadata tables.

### Validate (`sst validate`)
Checks semantic models for errors and best practice violations.
Essential for CI/CD pipelines to catch issues before deployment.

### Generate (`sst generate`)
Creates deployable artifacts from metadata:
- Snowflake SEMANTIC VIEW objects for BI tools
- YAML models with sample data for AI/LLM tools

Each command is designed to work both interactively and in automated
pipelines, with comprehensive error handling and progress reporting.
"""

from snowflake_semantic_tools.interfaces.cli.commands import deploy, enrich, extract, format, generate, validate

__all__ = ["enrich", "format", "extract", "validate", "generate", "deploy"]
