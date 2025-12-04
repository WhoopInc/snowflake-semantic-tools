"""
Core Enrichment Module

Provides core functionality for enriching dbt YAML metadata with semantic information.

Components:
- MetadataEnricher: Core orchestrator for metadata enrichment
- YAMLHandler: YAML file operations with preservation rules
- PrimaryKeyValidator: Primary key candidate validation
- CortexSynonymGenerator: LLM-based synonym generation
- Type mappings: Snowflake to SST data type conversions
"""

from snowflake_semantic_tools.core.enrichment.cortex_synonym_generator import CortexSynonymGenerator
from snowflake_semantic_tools.core.enrichment.metadata_enricher import MetadataEnricher
from snowflake_semantic_tools.core.enrichment.primary_key_validator import PrimaryKeyValidator
from snowflake_semantic_tools.core.enrichment.type_mappings import determine_column_type, map_snowflake_to_sst_datatype
from snowflake_semantic_tools.core.enrichment.yaml_handler import YAMLHandler

__all__ = [
    "MetadataEnricher",
    "YAMLHandler",
    "PrimaryKeyValidator",
    "CortexSynonymGenerator",
    "map_snowflake_to_sst_datatype",
    "determine_column_type",
]
