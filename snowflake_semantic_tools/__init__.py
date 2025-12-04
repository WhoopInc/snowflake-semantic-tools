"""
Snowflake Semantic Tools

A comprehensive suite of tools for building SQL Semantic Views from Snowflake data.
Designed for integration with Snowflake Cortex Analyst and BI tools.

**Usage Patterns:**
- CLI: Use snowflake-semantic-tools command (sst)
- Core modules: parsing, validation, generation, enrichment
"""

# Import version from dedicated file (avoids circular imports)
from snowflake_semantic_tools._version import __version__

# Expose Snowflake configuration
from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeConfig

# Import enrichment API
from snowflake_semantic_tools.interfaces.api.metadata_enrichment import MetadataEnricher

# Expose all services for programmatic access (Python API parity with CLI)
from snowflake_semantic_tools.services import (
    DeployService,
    MetadataEnrichmentService,
    SemanticMetadataCollectionValidationService,
    SemanticMetadataExtractionService,
    SemanticViewGenerationService,
    YAMLFormattingService,
)
from snowflake_semantic_tools.services.deploy import DeployConfig

# Expose configuration classes
from snowflake_semantic_tools.services.enrich_metadata import EnrichmentConfig
from snowflake_semantic_tools.services.extract_semantic_metadata import ExtractConfig
from snowflake_semantic_tools.services.format_yaml import FormattingConfig
from snowflake_semantic_tools.services.generate_semantic_views import GenerateConfig
from snowflake_semantic_tools.services.validate_semantic_models import ValidateConfig

__author__ = "WHOOP Inc."

# Main package exports - Python API
__all__ = [
    # API
    "MetadataEnricher",
    # Services
    "MetadataEnrichmentService",
    "SemanticMetadataCollectionValidationService",
    "SemanticMetadataExtractionService",
    "SemanticViewGenerationService",
    "YAMLFormattingService",
    "DeployService",
    # Configuration classes
    "EnrichmentConfig",
    "ValidateConfig",
    "ExtractConfig",
    "GenerateConfig",
    "FormattingConfig",
    "DeployConfig",
    "SnowflakeConfig",
]
