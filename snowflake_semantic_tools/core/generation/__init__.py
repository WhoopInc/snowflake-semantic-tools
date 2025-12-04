"""
Generation Module

Transforms semantic metadata into SQL Semantic Views for Snowflake.

## Snowflake Semantic Views (SQL)

Native Snowflake objects that define metrics, dimensions, and relationships
for BI tools and Cortex Analyst. Generated as CREATE SEMANTIC VIEW statements
executed directly in Snowflake.

## Components

- **SemanticViewBuilder**: Generates SQL for Snowflake SEMANTIC VIEW objects

The builder queries metadata tables populated by the extract command and
generates production-ready SQL semantic views.

## References

- Snowflake Semantic Views: https://docs.snowflake.com/en/sql-reference/sql/create-semantic-view
- Cortex Analyst: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst
"""

from snowflake_semantic_tools.core.generation.semantic_view_builder import SemanticViewBuilder

__all__ = [
    "SemanticViewBuilder",
]
