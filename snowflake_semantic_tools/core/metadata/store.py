"""
MetadataStore ABC

Defines the interface for reading compiled semantic metadata.
Both InMemoryStore (from manifest) and SnowflakeStore (from SM_* tables)
implement this interface so the SemanticViewBuilder is agnostic to
the data source.

Return value conventions:
- Per-table methods (get_dimensions, get_facts, get_time_dimensions)
  EXCLUDE TABLE_NAME from results — the caller already knows the table.
- Cross-table methods (get_metrics, get_filters, get_verified_queries,
  get_relationships) RETAIN their identifier fields (TABLE_NAME, TABLES,
  RELATIONSHIP_NAME, LEFT_TABLE_NAME, RIGHT_TABLE_NAME) because the
  builder needs them for filtering and routing.
- get_relationship_columns EXCLUDES RELATIONSHIP_NAME — caller knows it.
- All keys are UPPERCASE to match Snowflake convention, EXCEPT
  get_custom_instructions which returns question_categorization and
  sql_generation in lowercase (matching the builder's expected contract).
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class MetadataStore(ABC):
    """Interface for reading compiled semantic metadata."""

    @abstractmethod
    def get_semantic_views(self) -> List[Dict[str, Any]]:
        """Return all semantic view definitions (NAME, TABLES, DESCRIPTION, CUSTOM_INSTRUCTIONS)."""
        ...

    @abstractmethod
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Return table metadata excluding TABLE_NAME key (DATABASE, SCHEMA, PRIMARY_KEY, etc.).

        If the table is not found, implementations MUST return a fallback dict
        with at least TABLE_NAME and DESCRIPTION keys rather than raising.
        """
        ...

    @abstractmethod
    def get_dimensions(self, table_name: str) -> List[Dict[str, Any]]:
        """Return dimension columns excluding TABLE_NAME (NAME, EXPR, DESCRIPTION, SYNONYMS, etc.)."""
        ...

    @abstractmethod
    def get_time_dimensions(self, table_name: str) -> List[Dict[str, Any]]:
        """Return time dimension columns excluding TABLE_NAME."""
        ...

    @abstractmethod
    def get_facts(self, table_name: str) -> List[Dict[str, Any]]:
        """Return fact columns excluding TABLE_NAME (NAME, EXPR, DESCRIPTION, VISIBILITY, etc.)."""
        ...

    @abstractmethod
    def get_metrics(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Return metrics whose referenced tables are all within table_names. Retains TABLE_NAME field."""
        ...

    @abstractmethod
    def get_relationships(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Return relationships where both sides are in table_names. Retains RELATIONSHIP_NAME, LEFT/RIGHT_TABLE_NAME."""
        ...

    @abstractmethod
    def get_relationship_columns(self, relationship_name: str) -> List[Dict[str, Any]]:
        """Return join conditions excluding RELATIONSHIP_NAME (JOIN_CONDITION, CONDITION_TYPE, etc.)."""
        ...

    @abstractmethod
    def get_verified_queries(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Return verified queries whose tables are all within table_names. Retains TABLES field."""
        ...

    @abstractmethod
    def get_custom_instructions(self, names: List[str]) -> List[Dict[str, Any]]:
        """Return custom instructions matching the given names (NAME, QUESTION_CATEGORIZATION, SQL_GENERATION)."""
        ...

    @abstractmethod
    def get_filters(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Return filters for the given tables. Retains TABLE_NAME field."""
        ...
