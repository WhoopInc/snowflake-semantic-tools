"""
dbt Model Definitions

Data classes representing dbt models and their metadata.

These classes capture the structure and configuration of dbt models,
which serve as the foundation for semantic model generation. They map
the physical database layer that semantic models build upon.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DbtColumn:
    """
    Represents a column in a dbt model with its metadata.

    Captures column-level information from dbt YAML files, including
    documentation, data types, and quality checks. This metadata helps
    validate semantic model references and provides context for
    dimension/fact definitions.
    """

    name: str
    data_type: Optional[str] = None
    description: Optional[str] = None
    tests: List[str] = field(default_factory=list)
    constraints: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {"name": self.name}

        if self.data_type:
            result["data_type"] = self.data_type
        if self.description:
            result["description"] = self.description
        if self.tests:
            result["tests"] = self.tests
        if self.constraints:
            result["constraints"] = self.constraints
        if self.meta:
            result["meta"] = self.meta

        return result


@dataclass
class DbtModel:
    """
    Represents a complete dbt model with all its metadata.

    Serves as the physical foundation that semantic models reference.
    Each dbt model maps to a table or view in Snowflake and can become
    a logical table in the semantic layer. Contains all information needed
    to validate semantic model references and generate accurate metadata.
    """

    name: str
    database: str
    schema: str
    description: Optional[str] = None
    columns: List[DbtColumn] = field(default_factory=list)
    materialized: str = "table"
    tags: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    @property
    def fully_qualified_name(self) -> str:
        """Get the fully qualified table name."""
        return f"{self.database}.{self.schema}.{self.name}".upper()

    @property
    def table_name(self) -> str:
        """Get just the table name in uppercase."""
        return self.name.upper()

    def get_column(self, column_name: str) -> Optional[DbtColumn]:
        """
        Get a column by name.

        Args:
            column_name: Name of the column to find

        Returns:
            DbtColumn if found, None otherwise
        """
        for column in self.columns:
            if column.name.lower() == column_name.lower():
                return column
        return None

    def has_column(self, column_name: str) -> bool:
        """
        Check if a column exists in this model.

        Args:
            column_name: Name of the column to check

        Returns:
            True if column exists, False otherwise
        """
        return self.get_column(column_name) is not None

    def has_sst_metadata(self) -> bool:
        """
        Check if this model has SST (Snowflake Semantic Tools) metadata configured.

        Models with 'sst' metadata in their meta field are candidates for inclusion
        in the semantic model. This metadata defines metrics, dimensions, and other
        semantic layer properties.

        Returns:
            True if model has 'sst' key in meta field
        """
        if not self.meta:
            return False
        return bool(self.meta.get("sst"))

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {
            "name": self.name,
            "database": self.database,
            "schema": self.schema,
            "materialized": self.materialized,
        }

        if self.description:
            result["description"] = self.description
        if self.columns:
            result["columns"] = [c.to_dict() for c in self.columns]
        if self.tags:
            result["tags"] = self.tags
        if self.meta:
            result["meta"] = self.meta

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DbtModel":
        """
        Create a DbtModel from a dictionary.

        Args:
            data: Dictionary containing model data

        Returns:
            DbtModel instance
        """
        columns = []
        if "columns" in data:
            for col_data in data["columns"]:
                if isinstance(col_data, dict):
                    columns.append(
                        DbtColumn(
                            name=col_data.get("name", ""),
                            data_type=col_data.get("data_type"),
                            description=col_data.get("description"),
                            tests=col_data.get("tests", []),
                            constraints=col_data.get("constraints", []),
                            meta=col_data.get("meta", {}),
                        )
                    )

        return cls(
            name=data.get("name", ""),
            database=data.get("database", ""),
            schema=data.get("schema", ""),
            description=data.get("description"),
            columns=columns,
            materialized=data.get("materialized", "table"),
            tags=data.get("tags", []),
            meta=data.get("meta", {}),
        )
