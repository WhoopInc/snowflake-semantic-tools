"""
Parsing Module

Comprehensive YAML parsing system for transforming dbt models and semantic definitions
into Snowflake semantic model metadata.

## Architecture Overview

The parsing module implements a multi-pass parsing strategy:

1. **File Detection**: Automatically identifies file types (dbt models vs semantic models)
2. **Catalog Building**: First pass to build reference catalogs from all files
3. **Template Resolution**: Resolves {{ table() }}, {{ column() }}, {{ metric() }} references
4. **Validation**: Detects hardcoded values and validates references
5. **Data Extraction**: Transforms parsed content into database-ready format

## Components

- **Parser**: Main orchestrator that coordinates the parsing pipeline
- **FileTypeDetector**: Identifies YAML file types based on content patterns
- **Template Engine**: Resolves Jinja-like template references with circular dependency detection
- **Parsers**: Specialized parsers for different file types (dbt, metrics, relationships, etc.)

## Template Resolution Order

Templates are resolved in a specific order to handle dependencies:
1. Tables first (no dependencies)
2. Columns second (depend on tables)
3. Metrics third (may reference other metrics recursively)
4. Custom instructions last (standalone)

This ensures that nested references are properly expanded before validation.
"""

# Import order matters to avoid circular dependencies
from snowflake_semantic_tools.core.parsing.file_detector import FileTypeDetector

# Parser imports other modules, so import it last
from snowflake_semantic_tools.core.parsing.parser import Parser
from snowflake_semantic_tools.core.parsing.template_engine import HardcodedValueDetector, TemplateResolver

__all__ = ["Parser", "FileTypeDetector", "TemplateResolver", "HardcodedValueDetector"]
