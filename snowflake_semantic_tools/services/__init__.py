"""
Services Layer

Business logic orchestrators that coordinate core and infrastructure components.

## Architecture

Services implement complex workflows by coordinating multiple components:
- Core modules provide domain logic (parsing, validation, generation)
- Infrastructure provides external integrations (Git, Snowflake)
- Services orchestrate these components to deliver complete features

## Available Services

### SemanticMetadataExtractionService
Orchestrates the complete extraction pipeline:
1. Clones/updates Git repositories
2. Parses dbt and semantic model files
3. Validates and resolves templates
4. Loads metadata to Snowflake
5. Creates Cortex Search Services

### SemanticMetadataCollectionValidationService
Coordinates multi-layer validation:
1. Checks template resolution
2. Validates dbt model requirements
3. Verifies table/column references
4. Detects duplicates and circular dependencies

### SemanticViewGenerationService
Manages semantic view creation:
1. Queries metadata tables
2. Generates CREATE SEMANTIC VIEW SQL
3. Executes in target database

### SemanticViewGenerationService
Generates SQL semantic views for Snowflake with automatic view discovery.

### MetadataEnrichmentService
Enriches dbt YAML metadata with semantic information:
1. Discovers dbt models to process
2. Queries Snowflake for table schemas and sample values
3. Detects primary keys intelligently
4. Updates YAML files with complete metadata
5. Handles connection retries and error recovery

### YAMLFormattingService
Standardizes YAML file structure and formatting:
1. Enforces consistent field ordering
2. Removes excessive blank lines
3. Ensures proper indentation
4. Formats multi-line descriptions
5. Supports dry-run and check-only modes

Each service provides comprehensive error handling, logging, and progress
reporting while abstracting complex multi-step workflows.
"""

from snowflake_semantic_tools.services.deploy import DeployConfig, DeployResult, DeployService
from snowflake_semantic_tools.services.enrich_metadata import (
    EnrichmentConfig,
    EnrichmentResult,
    MetadataEnrichmentService,
)
from snowflake_semantic_tools.services.extract_semantic_metadata import SemanticMetadataExtractionService
from snowflake_semantic_tools.services.format_yaml import FormattingConfig, YAMLFormattingService
from snowflake_semantic_tools.services.generate_semantic_views import (
    SemanticViewGenerationService,
    UnifiedGenerationConfig,
    UnifiedGenerationResult,
)
from snowflake_semantic_tools.services.validate_semantic_models import SemanticMetadataCollectionValidationService

__all__ = [
    "SemanticMetadataExtractionService",
    "SemanticMetadataCollectionValidationService",
    "SemanticViewGenerationService",
    "UnifiedGenerationConfig",
    "UnifiedGenerationResult",
    "MetadataEnrichmentService",
    "EnrichmentConfig",
    "EnrichmentResult",
    "YAMLFormattingService",
    "FormattingConfig",
    "DeployService",
    "DeployConfig",
    "DeployResult",
]
