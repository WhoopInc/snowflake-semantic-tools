"""
Specialized Parsers for Semantic Model Components

Collection of parsers that extract and transform specific YAML structures:

- **dbt_parser**: Extracts physical table metadata from dbt models
- **manifest_parser**: Parses dbt manifest.json for database/schema resolution
- **semantic_parser**: Processes semantic model definitions (metrics, relationships, etc.)
- **table_summarizer**: Generates AI-optimized summaries for Cortex Search
- **data_extractors**: Common extraction utilities for column/table metadata
- **error_handler**: Centralized YAML parsing error management
"""

from snowflake_semantic_tools.core.parsing.parsers.data_extractors import (
    apply_uppercase_formatting,
    extract_column_info,
    extract_table_info,
    get_column_type,
)
from snowflake_semantic_tools.core.parsing.parsers.dbt_parser import get_empty_result, parse_dbt_yaml_file
from snowflake_semantic_tools.core.parsing.parsers.error_handler import ErrorTracker, format_yaml_error
from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser
from snowflake_semantic_tools.core.parsing.parsers.semantic_parser import (
    parse_semantic_model_file,
    parse_semantic_views,
    parse_snowflake_custom_instructions,
    parse_snowflake_filters,
    parse_snowflake_metrics,
    parse_snowflake_relationships,
    parse_snowflake_verified_queries,
)
from snowflake_semantic_tools.core.parsing.parsers.table_summarizer import generate_table_summaries

__all__ = [
    # dbt parser
    "parse_dbt_yaml_file",
    "get_empty_result",
    # Manifest parser
    "ManifestParser",
    # Semantic parser
    "parse_semantic_model_file",
    "parse_snowflake_metrics",
    "parse_snowflake_relationships",
    "parse_snowflake_filters",
    "parse_snowflake_custom_instructions",
    "parse_snowflake_verified_queries",
    "parse_semantic_views",
    # Error handling
    "ErrorTracker",
    "format_yaml_error",
    # Table summarizer
    "generate_table_summaries",
    # Data extractors
    "apply_uppercase_formatting",
    "extract_column_info",
    "extract_table_info",
    "get_column_type",
]
