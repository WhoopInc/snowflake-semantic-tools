#!/usr/bin/env python3
"""
Snowflake Semantic Metadata Table Schemas

Defines the physical Snowflake table structures that store semantic metadata
extracted from dbt model YAML files and semantic model definitions.

## Data Flow

This module defines the target schemas for the SST metadata extraction pipeline:

1. **Source**: dbt models define semantic metadata in YAML files
   - Tables, columns, dimensions, facts, time dimensions
   - Located in: `models/` directory (dbt models)
   - Located in: `snowflake_semantic_models/` directory (metrics, relationships, filters, etc.)

2. **Extraction**: `sst extract` command reads and parses YAML files
   - Parses dbt model YAML and semantic model YAML
   - Validates metadata structure
   - Transforms into structured records

3. **Storage**: Metadata is loaded into Snowflake tables matching these schemas
   - Tables prefixed with `sm_` (semantic model)
   - Stored in configured database/schema (e.g., ANALYTICS.SEMANTIC_METADATA)
   - Provides persistent storage for semantic layer definitions

4. **Consumption**: Downstream tools query these tables
   - `sst generate` reads metadata to create semantic views
   - Snowflake Cortex Analyst uses metadata for AI-powered queries
   - External tools can query metadata for analysis

## Schema Organization

Each schema in this module represents a specific metadata table:
- `sm_tables` - Logical tables (business entities)
- `sm_dimensions` - Categorical attributes for grouping/filtering
- `sm_time_dimensions` - Temporal columns for time-series analysis
- `sm_facts` - Numeric measures at row level
- `sm_metrics` - Aggregated KPIs and business calculations
- `sm_relationships` - How tables join together
- `sm_filters` - Reusable WHERE clause conditions
- `sm_verified_queries` - Pre-validated example queries
- `sm_custom_instructions` - AI guidance for query generation
- `sm_semantic_views` - Curated subsets for specific domains

## Usage

These schemas are used by:
- **Extract pipeline**: Creates/validates Snowflake tables during `sst extract`
- **Generation pipeline**: Reads from these tables during `sst generate`
- **Validation**: Ensures metadata conforms to expected structure
- **Documentation**: Serves as reference for metadata structure
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Literal, Union


class ColumnType(Enum):
    """Supported Snowflake column types."""

    VARCHAR = "VARCHAR"
    BOOLEAN = "BOOLEAN"
    ARRAY = "ARRAY"


@dataclass
class Column:
    """Represents a database column definition."""

    name: str
    type: ColumnType
    nullable: bool = True
    description: str = ""


@dataclass
class TableSchema:
    """Represents a complete table schema definition."""

    name: str
    columns: List[Column]
    description: str = ""


class SemanticTableSchemas:
    """Container for all semantic model table schemas."""

    @staticmethod
    def get_table_schema() -> TableSchema:
        """Schema for logical tables that map to physical database tables/views."""
        return TableSchema(
            name="sm_tables",
            description="Logical tables representing business entities (customers, orders, etc.) that map to physical Snowflake tables or views",
            columns=[
                Column("table_name", ColumnType.VARCHAR, description="Logical table name used in the semantic model"),
                Column("database", ColumnType.VARCHAR, description="Physical database containing the base table"),
                Column("schema", ColumnType.VARCHAR, description="Physical schema containing the base table"),
                Column(
                    "description",
                    ColumnType.VARCHAR,
                    description="Business-friendly description of what this table contains",
                ),
                Column(
                    "primary_key",
                    ColumnType.ARRAY,
                    description="List of columns that uniquely identify each row (required for relationships)",
                ),
                Column(
                    "unique_keys",
                    ColumnType.ARRAY,
                    description="List of columns that form a UNIQUE constraint (required for ASOF relationships)",
                ),
                Column(
                    "synonyms",
                    ColumnType.ARRAY,
                    description="Alternative business terms users might use to refer to this table",
                ),
                Column(
                    "cortex_searchable",
                    ColumnType.BOOLEAN,
                    description="Whether to include this table in Cortex Analyst queries",
                ),
            ],
        )

    @staticmethod
    def get_dimension_schema() -> TableSchema:
        """Schema for dimensions - categorical data that provides context to facts."""
        return TableSchema(
            name="sm_dimensions",
            description="Dimensions are categorical attributes (product names, customer types, locations) used to filter, group, and label facts in analyses",
            columns=[
                Column("table_name", ColumnType.VARCHAR, description="Logical table this dimension belongs to"),
                Column("name", ColumnType.VARCHAR, description="Business-friendly name for this dimension"),
                Column(
                    "expr",
                    ColumnType.VARCHAR,
                    description="SQL expression defining this dimension (can reference physical columns or other logical columns)",
                ),
                Column("data_type", ColumnType.VARCHAR, description="Snowflake data type (TEXT, NUMBER, etc.)"),
                Column(
                    "description",
                    ColumnType.VARCHAR,
                    description="Business context explaining what this dimension represents",
                ),
                Column(
                    "synonyms",
                    ColumnType.ARRAY,
                    description="Alternative terms users might use to refer to this dimension",
                ),
                Column("sample_values", ColumnType.ARRAY, description="Common values users are likely to filter by"),
                Column(
                    "is_enum",
                    ColumnType.BOOLEAN,
                    description="If true, sample_values contains ALL possible values (for validation)",
                ),
            ],
        )

    @staticmethod
    def get_time_dimension_schema() -> TableSchema:
        """Schema for time dimensions - temporal data for trend analysis."""
        return TableSchema(
            name="sm_time_dimensions",
            description="Time dimensions provide temporal context for analyzing facts across different periods (dates, months, years) and enable trend analysis",
            columns=[
                Column("table_name", ColumnType.VARCHAR, description="Logical table this time dimension belongs to"),
                Column("name", ColumnType.VARCHAR, description="Business-friendly name for this time dimension"),
                Column(
                    "description",
                    ColumnType.VARCHAR,
                    description="Business context including timezone information if applicable",
                ),
                Column(
                    "expr",
                    ColumnType.VARCHAR,
                    description="SQL expression defining this time dimension (e.g., DATE columns, DATEDIFF calculations)",
                ),
                Column("data_type", ColumnType.VARCHAR, description="Temporal data type (DATE, TIMESTAMP, etc.)"),
                Column(
                    "synonyms",
                    ColumnType.ARRAY,
                    description="Alternative terms users might use (e.g., 'order date' vs 'purchase date')",
                ),
                Column("sample_values", ColumnType.ARRAY, description="Example date/time values for reference"),
            ],
        )

    @staticmethod
    def get_facts_schema() -> TableSchema:
        """Schema for facts - unaggregated numeric values at the row level."""
        return TableSchema(
            name="sm_facts",
            description="Facts are measurable, quantitative data representing business processes (sales amounts, quantities, costs) at the row level, before aggregation",
            columns=[
                Column("table_name", ColumnType.VARCHAR, description="Logical table this fact belongs to"),
                Column("name", ColumnType.VARCHAR, description="Business-friendly name for this fact"),
                Column(
                    "description", ColumnType.VARCHAR, description="Business context explaining what this fact measures"
                ),
                Column(
                    "expr",
                    ColumnType.VARCHAR,
                    description="SQL expression defining this fact (can reference physical or logical columns within same table)",
                ),
                Column("data_type", ColumnType.VARCHAR, description="Numeric data type (NUMBER, DECIMAL, etc.)"),
                Column(
                    "synonyms",
                    ColumnType.ARRAY,
                    description="Alternative terms users might use (e.g., 'revenue' vs 'sales amount')",
                ),
                Column("sample_values", ColumnType.ARRAY, description="Example numeric values for reference"),
            ],
        )

    @staticmethod
    def get_filter_schema() -> TableSchema:
        """Schema for filters - predefined conditions to limit query results."""
        return TableSchema(
            name="sm_filters",
            description="Filters are predefined WHERE clause conditions that limit query results to specific data subsets (e.g., 'last 30 days', 'North America only')",
            columns=[
                Column("name", ColumnType.VARCHAR, description="Business-friendly name for this filter"),
                Column("table_name", ColumnType.VARCHAR, description="Logical table this filter applies to"),
                Column(
                    "description",
                    ColumnType.VARCHAR,
                    description="Business context explaining when/why to use this filter",
                ),
                Column("expr", ColumnType.VARCHAR, description="SQL boolean expression defining the filter condition"),
                Column(
                    "synonyms", ColumnType.ARRAY, description="Alternative terms users might use to apply this filter"
                ),
            ],
        )

    @staticmethod
    def get_metric_schema() -> TableSchema:
        """Schema for metrics - aggregated KPIs and business performance measures."""
        return TableSchema(
            name="sm_metrics",
            description="Metrics are aggregated business performance indicators (KPIs) calculated using aggregate functions (SUM, AVG, COUNT) across multiple rows",
            columns=[
                Column("name", ColumnType.VARCHAR, description="Business-friendly name for this metric"),
                Column("table_name", ColumnType.VARCHAR, description="Primary logical table this metric is based on"),
                Column(
                    "description",
                    ColumnType.VARCHAR,
                    description="Business context explaining what this metric measures and how it's used",
                ),
                Column(
                    "expr",
                    ColumnType.VARCHAR,
                    description="SQL expression with aggregate functions (can reference columns from multiple tables via relationships)",
                ),
                Column(
                    "synonyms",
                    ColumnType.ARRAY,
                    description="Alternative terms users might use (e.g., 'total revenue' vs 'gross sales')",
                ),
                Column("sample_values", ColumnType.ARRAY, description="Example calculated values for reference"),
            ],
        )

    @staticmethod
    def get_relationship_schema() -> TableSchema:
        """Schema for relationships - defines how logical tables connect via joins."""
        return TableSchema(
            name="sm_relationships",
            description="Relationships define how logical tables join together, enabling cross-table analysis (e.g., joining orders to customers)",
            columns=[
                Column("relationship_name", ColumnType.VARCHAR, description="Unique identifier for this relationship"),
                Column("left_table_name", ColumnType.VARCHAR, description="Logical table on the left side of the join"),
                Column(
                    "right_table_name", ColumnType.VARCHAR, description="Logical table on the right side of the join"
                ),
            ],
        )

    @staticmethod
    def get_relationship_column_schema() -> TableSchema:
        """Schema for relationship column mappings - specifies join conditions."""
        return TableSchema(
            name="sm_relationship_columns",
            description="Defines join conditions for each relationship (supports equality, ASOF, and range joins)",
            columns=[
                Column(
                    "relationship_name",
                    ColumnType.VARCHAR,
                    description="References the parent relationship from sm_relationships",
                ),
                Column(
                    "join_condition",
                    ColumnType.VARCHAR,
                    description='Full join condition expression with templates (e.g., \'{{ column("orders", "customer_id") }} = {{ column("customers", "id") }}\')',
                ),
                Column(
                    "condition_type",
                    ColumnType.VARCHAR,
                    description="Auto-detected type: 'equality', 'asof', or 'range' based on operator",
                ),
                Column(
                    "left_expression",
                    ColumnType.VARCHAR,
                    description="Left side of the join condition (extracted from templates)",
                ),
                Column(
                    "right_expression",
                    ColumnType.VARCHAR,
                    description="Right side of the join condition (extracted from templates)",
                ),
                Column("operator", ColumnType.VARCHAR, description="Join operator: '=', '>=', '<=', 'BETWEEN', etc."),
            ],
        )

    @staticmethod
    def get_verified_query_schema() -> TableSchema:
        """Schema for verified queries - pre-validated examples for Cortex Analyst."""
        return TableSchema(
            name="sm_verified_queries",
            description="Verified queries are pre-validated natural language questions with correct SQL answers, used to train and guide Cortex Analyst",
            columns=[
                Column("name", ColumnType.VARCHAR, description="Unique identifier for this verified query"),
                Column("question", ColumnType.VARCHAR, description="Natural language question that users might ask"),
                Column("tables", ColumnType.ARRAY, description="List of logical tables referenced in the SQL answer"),
                Column("verified_at", ColumnType.VARCHAR, description="Timestamp when this query was validated"),
                Column("verified_by", ColumnType.VARCHAR, description="Person or process that validated this query"),
                Column(
                    "use_as_onboarding_question",
                    ColumnType.VARCHAR,
                    description="Whether to show this as an example question to new users",
                ),
                Column(
                    "sql",
                    ColumnType.VARCHAR,
                    description="The correct SQL query that answers the natural language question",
                ),
            ],
        )

    @staticmethod
    def get_custom_instructions_schema() -> TableSchema:
        """Schema for custom instructions - guidance for Cortex Analyst's behavior."""
        return TableSchema(
            name="sm_custom_instructions",
            description="Custom instructions provide domain-specific guidance to Cortex Analyst for question categorization and SQL generation",
            columns=[
                Column(
                    "name",
                    ColumnType.VARCHAR,
                    description="Unique identifier for this instruction set (referenced by semantic views)",
                ),
                Column(
                    "instruction",
                    ColumnType.VARCHAR,
                    description="Combined text of question categorization and SQL generation instructions",
                ),
            ],
        )

    @staticmethod
    def get_table_summary_schema() -> TableSchema:
        """Schema for table summaries - AI-optimized descriptions for Cortex Search."""
        return TableSchema(
            name="sm_table_summaries",
            description="AI-generated summaries of logical tables used by Cortex Search Service for improved natural language understanding",
            columns=[
                Column("TABLE_NAME", ColumnType.VARCHAR, description="Logical table name being summarized"),
                Column(
                    "DATABASE_NAME",
                    ColumnType.VARCHAR,
                    description="Database name where the table resides (for FQN construction)",
                ),
                Column(
                    "SCHEMA_NAME",
                    ColumnType.VARCHAR,
                    description="Schema name where the table resides (for FQN construction)",
                ),
                Column(
                    "TABLE_SUMMARY",
                    ColumnType.VARCHAR,
                    description="Comprehensive AI-friendly summary including table purpose, key columns, and common use cases",
                ),
                Column(
                    "CORTEX_SEARCHABLE",
                    ColumnType.BOOLEAN,
                    description="Whether this table is included in Cortex Analyst (inherited from sm_tables)",
                ),
            ],
        )

    @staticmethod
    def get_semantic_views_schema() -> TableSchema:
        """Schema for semantic views - curated data models for specific business domains."""
        return TableSchema(
            name="sm_semantic_views",
            description="Semantic views are curated subsets of the semantic model, grouping related tables for specific business use cases (e.g., 'sales_analytics', 'customer_360')",
            columns=[
                Column(
                    "name",
                    ColumnType.VARCHAR,
                    description="Unique name for this semantic view (becomes the Snowflake SEMANTIC VIEW object name)",
                ),
                Column(
                    "description", ColumnType.VARCHAR, description="Business purpose and scope of this semantic view"
                ),
                Column(
                    "tables",
                    ColumnType.ARRAY,
                    description="List of logical table names to include in this view's scope",
                ),
                Column(
                    "custom_instructions",
                    ColumnType.VARCHAR,
                    description="Resolved custom instructions text providing domain-specific guidance for this view",
                ),
            ],
        )

    @classmethod
    def get_all_schemas(cls) -> Dict[str, TableSchema]:
        """Get all table schemas as a dictionary."""
        return {
            "sm_tables": cls.get_table_schema(),
            "sm_dimensions": cls.get_dimension_schema(),
            "sm_time_dimensions": cls.get_time_dimension_schema(),
            "sm_facts": cls.get_facts_schema(),
            "sm_filters": cls.get_filter_schema(),
            "sm_metrics": cls.get_metric_schema(),
            "sm_relationships": cls.get_relationship_schema(),
            "sm_relationship_columns": cls.get_relationship_column_schema(),
            "sm_verified_queries": cls.get_verified_query_schema(),
            "sm_custom_instructions": cls.get_custom_instructions_schema(),
            "sm_table_summaries": cls.get_table_summary_schema(),
            "sm_semantic_views": cls.get_semantic_views_schema(),
        }
