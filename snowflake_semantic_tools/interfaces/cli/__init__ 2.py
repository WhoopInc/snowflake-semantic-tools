"""
CLI Interface

Command-line interface optimized for operations and CI/CD workflows.

## Features

- **Auto-detection**: Automatically detects repository type and environment
- **Environment Configuration**: Supports .env files and environment variables
- **Progress Feedback**: Clear status updates during long-running operations
- **Error Handling**: Detailed error messages with resolution guidance
- **CI/CD Ready**: Designed for automated pipelines with non-interactive modes

## Available Commands

All commands are accessed through the `sst` entry point:
- `sst validate` - Validate semantic model definitions
- `sst extract` - Extract and load semantic metadata
- `sst generate` - Generate views and/or YAML models

Each command supports `--help` for detailed usage information.
"""

from snowflake_semantic_tools.interfaces.cli.main import cli

__all__ = ["cli"]
