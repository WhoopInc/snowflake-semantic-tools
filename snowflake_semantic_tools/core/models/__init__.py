"""
Domain Models

Core data models for semantic model processing and validation.

This module provides the foundational data structures for:

1. **Semantic Models**: Business-friendly metadata layer
   - Metrics: Aggregated KPIs and business measures
   - Relationships: Table join definitions
   - Filters: Reusable WHERE conditions
   - Custom Instructions: AI guidance for Cortex Analyst
   - Verified Queries: Validated natural language to SQL examples
   - Semantic Views: Domain-specific curated views

2. **dbt Models**: Physical database layer foundation
   - DbtModel: Complete model with database location and columns
   - DbtColumn: Column metadata including types and descriptions

3. **Validation**: Comprehensive issue tracking
   - ValidationResult: Container for all validation findings
   - ValidationError/Warning/Info/Success: Issue severity levels

4. **Schemas**: Database table definitions
   - TableSchema: Structure for semantic metadata tables
   - Column: Individual column specifications
   - SemanticTableSchemas: All table schemas for Snowflake
"""

from snowflake_semantic_tools.core.models.dbt_model import DbtColumn, DbtModel
from snowflake_semantic_tools.core.models.schemas import Column, SemanticTableSchemas, TableSchema
from snowflake_semantic_tools.core.models.semantic_model import (
    CustomInstruction,
    Filter,
    Metric,
    Relationship,
    SemanticMetadataCollection,
    SemanticView,
    VerifiedQuery,
)
from snowflake_semantic_tools.core.models.validation import (
    ValidationError,
    ValidationResult,
    ValidationSuccess,
    ValidationWarning,
)

__all__ = [
    # Semantic Metadata Collection
    "SemanticMetadataCollection",
    "Metric",
    "Relationship",
    "Filter",
    "CustomInstruction",
    "VerifiedQuery",
    "SemanticView",
    # dbt Models
    "DbtModel",
    "DbtColumn",
    # Validation
    "ValidationResult",
    "ValidationError",
    "ValidationWarning",
    "ValidationSuccess",
    # Schemas
    "TableSchema",
    "Column",
    "SemanticTableSchemas",
]
