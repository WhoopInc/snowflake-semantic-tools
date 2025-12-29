"""
Snowflake Semantic Tools

A comprehensive suite of tools for building SQL Semantic Views from Snowflake data.
Designed for integration with Snowflake Cortex Analyst and BI tools.

**Usage Patterns:**
- CLI: Use snowflake-semantic-tools command (sst)
- Core modules: parsing, validation, generation, enrichment

Note: This module uses lazy imports (Issue #10) to keep CLI startup fast.
Heavy dependencies (pandas, snowflake-connector) are only loaded when needed.
"""

# Only import version at module load - it's fast and commonly needed
from snowflake_semantic_tools._version import __version__

__author__ = "WHOOP Inc."

# Main package exports - Python API
__all__ = [
    # Version
    "__version__",
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


# Issue #10: Lazy imports for fast CLI startup
# Heavy modules are only imported when their attributes are accessed
def __getattr__(name):
    """Lazy import of heavy modules to keep package import fast."""

    # Snowflake configuration
    if name == "SnowflakeConfig":
        from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeConfig

        return SnowflakeConfig

    # Enrichment API
    if name == "MetadataEnricher":
        from snowflake_semantic_tools.interfaces.api.metadata_enrichment import MetadataEnricher

        return MetadataEnricher

    # Services
    if name == "DeployService":
        from snowflake_semantic_tools.services import DeployService

        return DeployService
    if name == "MetadataEnrichmentService":
        from snowflake_semantic_tools.services import MetadataEnrichmentService

        return MetadataEnrichmentService
    if name == "SemanticMetadataCollectionValidationService":
        from snowflake_semantic_tools.services import SemanticMetadataCollectionValidationService

        return SemanticMetadataCollectionValidationService
    if name == "SemanticMetadataExtractionService":
        from snowflake_semantic_tools.services import SemanticMetadataExtractionService

        return SemanticMetadataExtractionService
    if name == "SemanticViewGenerationService":
        from snowflake_semantic_tools.services import SemanticViewGenerationService

        return SemanticViewGenerationService
    if name == "YAMLFormattingService":
        from snowflake_semantic_tools.services import YAMLFormattingService

        return YAMLFormattingService

    # Configuration classes
    if name == "DeployConfig":
        from snowflake_semantic_tools.services.deploy import DeployConfig

        return DeployConfig
    if name == "EnrichmentConfig":
        from snowflake_semantic_tools.services.enrich_metadata import EnrichmentConfig

        return EnrichmentConfig
    if name == "ExtractConfig":
        from snowflake_semantic_tools.services.extract_semantic_metadata import ExtractConfig

        return ExtractConfig
    if name == "FormattingConfig":
        from snowflake_semantic_tools.services.format_yaml import FormattingConfig

        return FormattingConfig
    if name == "GenerateConfig":
        from snowflake_semantic_tools.services.generate_semantic_views import GenerateConfig

        return GenerateConfig
    if name == "ValidateConfig":
        from snowflake_semantic_tools.services.validate_semantic_models import ValidateConfig

        return ValidateConfig

    raise AttributeError(f"module 'snowflake_semantic_tools' has no attribute '{name}'")
