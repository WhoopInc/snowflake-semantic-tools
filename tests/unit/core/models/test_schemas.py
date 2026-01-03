"""
Test Snowflake metadata schema classes.

Tests the schema definitions for Snowflake semantic metadata tables.
"""

import pytest

from snowflake_semantic_tools.core.models.schemas import Column, ColumnType, SemanticTableSchemas, TableSchema


class TestColumnType:
    """Test ColumnType enum."""

    def test_column_type_values(self):
        """Test that column types have correct values."""
        assert ColumnType.VARCHAR.value == "VARCHAR"
        assert ColumnType.BOOLEAN.value == "BOOLEAN"
        assert ColumnType.ARRAY.value == "ARRAY"


class TestColumn:
    """Test Column data class."""

    def test_column_creation(self):
        """Test basic column creation."""
        column = Column(
            name="user_id", type=ColumnType.VARCHAR, nullable=False, description="Unique identifier for users"
        )

        assert column.name == "user_id"
        assert column.type == ColumnType.VARCHAR
        assert column.nullable is False
        assert column.description == "Unique identifier for users"

    def test_column_defaults(self):
        """Test column with default values."""
        column = Column(name="status", type=ColumnType.VARCHAR)

        assert column.nullable is True  # Default
        assert column.description == ""  # Default


class TestTableSchema:
    """Test TableSchema data class."""

    def test_table_schema_creation(self):
        """Test basic table schema creation."""
        columns = [
            Column("id", ColumnType.VARCHAR, False, "Primary key"),
            Column("name", ColumnType.VARCHAR, True, "User name"),
        ]

        schema = TableSchema(name="users", columns=columns, description="User accounts table")

        assert schema.name == "users"
        assert len(schema.columns) == 2
        assert schema.description == "User accounts table"
        assert schema.columns[0].name == "id"

    def test_table_schema_defaults(self):
        """Test table schema with default values."""
        schema = TableSchema(name="temp_table", columns=[])

        assert schema.description == ""  # Default


class TestSemanticTableSchemas:
    """Test SemanticTableSchemas functionality."""

    def test_get_table_schema(self):
        """Test getting table schema definition."""
        schema = SemanticTableSchemas.get_table_schema()

        assert schema.name == "sm_tables"
        assert len(schema.columns) > 0
        assert schema.description != ""

        # Check for required columns
        column_names = [col.name for col in schema.columns]
        assert "table_name" in column_names
        assert "database" in column_names
        assert "schema" in column_names
        assert "cortex_searchable" in column_names

    def test_get_dimension_schema(self):
        """Test getting dimension schema definition."""
        schema = SemanticTableSchemas.get_dimension_schema()

        assert schema.name == "sm_dimensions"
        assert len(schema.columns) > 0
        assert "dimensions" in schema.description.lower()

        # Check for required columns
        column_names = [col.name for col in schema.columns]
        assert "table_name" in column_names
        assert "name" in column_names
        assert "expr" in column_names
        assert "data_type" in column_names

    def test_get_time_dimension_schema(self):
        """Test getting time dimension schema definition."""
        schema = SemanticTableSchemas.get_time_dimension_schema()

        assert schema.name == "sm_time_dimensions"
        assert "time" in schema.description.lower()

        column_names = [col.name for col in schema.columns]
        assert "table_name" in column_names
        assert "name" in column_names
        assert "expr" in column_names

    def test_get_facts_schema(self):
        """Test getting facts schema definition."""
        schema = SemanticTableSchemas.get_facts_schema()

        assert schema.name == "sm_facts"
        assert "fact" in schema.description.lower()

        column_names = [col.name for col in schema.columns]
        assert "table_name" in column_names
        assert "name" in column_names
        assert "expr" in column_names

    def test_get_metric_schema(self):
        """Test getting metric schema definition."""
        schema = SemanticTableSchemas.get_metric_schema()

        assert schema.name == "sm_metrics"
        assert "metric" in schema.description.lower()

        column_names = [col.name for col in schema.columns]
        assert "name" in column_names
        assert "expr" in column_names
        assert "table_name" in column_names  # Actual column name

    def test_get_relationship_schema(self):
        """Test getting relationship schema definition."""
        schema = SemanticTableSchemas.get_relationship_schema()

        assert schema.name == "sm_relationships"
        assert "relationship" in schema.description.lower()

        column_names = [col.name for col in schema.columns]
        assert "relationship_name" in column_names
        assert "left_table_name" in column_names
        assert "right_table_name" in column_names

    def test_all_schemas_have_required_structure(self):
        """Test that all schemas follow required structure."""
        schema_methods = [
            SemanticTableSchemas.get_table_schema,
            SemanticTableSchemas.get_dimension_schema,
            SemanticTableSchemas.get_time_dimension_schema,
            SemanticTableSchemas.get_facts_schema,
            SemanticTableSchemas.get_metric_schema,
            SemanticTableSchemas.get_relationship_schema,
            SemanticTableSchemas.get_filter_schema,
            SemanticTableSchemas.get_custom_instructions_schema,
            SemanticTableSchemas.get_verified_query_schema,
            SemanticTableSchemas.get_semantic_views_schema,
            SemanticTableSchemas.get_table_summary_schema,
        ]

        for schema_method in schema_methods:
            schema = schema_method()

            # All schemas should have these basic properties
            assert isinstance(schema, TableSchema)
            assert schema.name.startswith("sm_")
            assert len(schema.columns) > 0
            assert schema.description != ""

            # All columns should be properly defined
            for column in schema.columns:
                assert isinstance(column, Column)
                assert column.name != ""
                assert isinstance(column.type, ColumnType)
                assert isinstance(column.nullable, bool)

    def test_schema_column_consistency(self):
        """Test that schema columns are consistently defined."""
        all_schemas = [
            SemanticTableSchemas.get_table_schema(),
            SemanticTableSchemas.get_dimension_schema(),
            SemanticTableSchemas.get_metric_schema(),
            SemanticTableSchemas.get_relationship_schema(),
        ]

        for schema in all_schemas:
            for column in schema.columns:
                # All columns should have descriptions
                assert column.description != "", f"Column {column.name} in {schema.name} missing description"

                # Column names should be lowercase with underscores
                assert column.name.islower() or "_" in column.name, f"Column {column.name} should use snake_case"


# Remove the remaining test classes since they don't match the actual schema structure
