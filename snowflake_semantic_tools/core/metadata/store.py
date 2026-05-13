"""
MetadataStore ABC

Defines the interface for reading compiled semantic metadata.
Both InMemoryStore (from manifest) and SnowflakeStore (from SM_* tables)
implement this interface so the SemanticViewBuilder is agnostic to
the data source.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class MetadataStore(ABC):
    """Interface for reading compiled semantic metadata."""

    @abstractmethod
    def get_semantic_views(self) -> List[Dict[str, Any]]:
        """Return all semantic view definitions."""
        ...

    @abstractmethod
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Return table metadata for a single table (from SM_TABLES)."""
        ...

    @abstractmethod
    def get_dimensions(self, table_name: str) -> List[Dict[str, Any]]:
        """Return dimension columns for a table."""
        ...

    @abstractmethod
    def get_time_dimensions(self, table_name: str) -> List[Dict[str, Any]]:
        """Return time dimension columns for a table."""
        ...

    @abstractmethod
    def get_facts(self, table_name: str) -> List[Dict[str, Any]]:
        """Return fact columns for a table."""
        ...

    @abstractmethod
    def get_metrics(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Return metrics whose referenced tables are all within table_names."""
        ...

    @abstractmethod
    def get_relationships(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Return relationships where both sides are in table_names."""
        ...

    @abstractmethod
    def get_relationship_columns(self, relationship_name: str) -> List[Dict[str, Any]]:
        """Return join conditions for a relationship."""
        ...

    @abstractmethod
    def get_verified_queries(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Return verified queries whose tables are all within table_names."""
        ...

    @abstractmethod
    def get_custom_instructions(self, names: List[str]) -> List[Dict[str, Any]]:
        """Return custom instructions matching the given names."""
        ...

    @abstractmethod
    def get_filters(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Return filters for the given tables."""
        ...
