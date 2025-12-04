"""
Test dbt model classes.

Tests the dbt model data structures (DbtColumn, DbtModel).
"""

import pytest
from snowflake_semantic_tools.core.models.dbt_model import DbtColumn, DbtModel


class TestDbtColumn:
    """Test DbtColumn data class."""
    
    def test_dbt_column_creation(self):
        """Test basic dbt column creation."""
        column = DbtColumn(
            name="user_id",
            description="Unique identifier for users",
            data_type="VARCHAR"
        )
        
        assert column.name == "user_id"
        assert column.description == "Unique identifier for users"
        assert column.data_type == "VARCHAR"
    
    def test_dbt_column_with_tests(self):
        """Test dbt column with tests."""
        column = DbtColumn(
            name="email",
            description="User email address",
            data_type="VARCHAR",
            tests=["unique", "not_null"]
        )
        
        assert "unique" in column.tests
        assert "not_null" in column.tests
        assert len(column.tests) == 2
    
    def test_dbt_column_with_meta(self):
        """Test dbt column with metadata."""
        column = DbtColumn(
            name="amount",
            description="Order amount",
            data_type="DECIMAL",
            meta={
                "sst": {
                    "column_type": "fact",
                    "synonyms": ["total", "price", "cost"]
                }
            }
        )
        
        assert column.meta["sst"]["column_type"] == "fact"
        assert "total" in column.meta["sst"]["synonyms"]
    
    def test_dbt_column_defaults(self):
        """Test dbt column with default values."""
        column = DbtColumn(
            name="status",
            data_type="VARCHAR"
        )
        
        assert column.description is None
        assert column.tests == []
        assert column.meta == {}
    
    def test_dbt_column_string_representation(self):
        """Test string representation of dbt column."""
        column = DbtColumn(
            name="id",
            description="Primary key",
            data_type="BIGINT"
        )
        
        str_repr = str(column)
        assert "id" in str_repr
        assert "BIGINT" in str_repr


class TestDbtModel:
    """Test DbtModel data class."""
    
    def test_dbt_model_creation(self):
        """Test basic dbt model creation."""
        columns = [
            DbtColumn(name="id", data_type="BIGINT"),
            DbtColumn(name="name", data_type="VARCHAR"),
            DbtColumn(name="created_at", data_type="TIMESTAMP")
        ]
        
        model = DbtModel(
            name="users",
            database="analytics",
            schema="public",
            description="User accounts table",
            columns=columns
        )
        
        assert model.name == "users"
        assert model.description == "User accounts table"
        assert len(model.columns) == 3
        assert model.database == "analytics"
        assert model.schema == "public"
    
    def test_dbt_model_with_meta(self):
        """Test dbt model with metadata."""
        model = DbtModel(
            name="orders",
            database="analytics",
            schema="public",
            columns=[],
            meta={
                "sst": {
                    "cortex_searchable": True,
                    "primary_key": "id",
                    "synonyms": ["purchases", "transactions"]
                }
            }
        )
        
        assert model.meta["sst"]["cortex_searchable"] is True
        assert model.meta["sst"]["primary_key"] == "id"
        assert "purchases" in model.meta["sst"]["synonyms"]
    
    def test_dbt_model_get_column_by_name(self):
        """Test getting column by name."""
        id_column = DbtColumn(name="id", data_type="BIGINT")
        name_column = DbtColumn(name="name", data_type="VARCHAR")
        
        model = DbtModel(
            name="users",
            database="analytics",
            schema="public",
            columns=[id_column, name_column]
        )
        
        found_column = model.get_column("name")  # Actual method name
        assert found_column is not None
        assert found_column.name == "name"
        assert found_column.data_type == "VARCHAR"
        
        not_found = model.get_column("nonexistent")
        assert not_found is None
    
    def test_dbt_model_get_columns(self):
        """Test getting all columns."""
        columns = [
            DbtColumn(name="id", data_type="BIGINT"),
            DbtColumn(name="email", data_type="VARCHAR"),
            DbtColumn(name="status", data_type="VARCHAR")
        ]
        
        model = DbtModel(name="users", database="analytics", schema="public", columns=columns)
        
        # Test that all columns are accessible
        assert len(model.columns) == 3
        assert model.get_column("id") is not None
        assert model.get_column("email") is not None
        assert model.get_column("status") is not None
        
        # Test column names through the columns property
        column_names = [col.name for col in model.columns]
        assert "id" in column_names
        assert "email" in column_names
        assert "status" in column_names
    
    def test_dbt_model_has_sst_metadata(self):
        """Test checking for SST metadata."""
        model_with_sst = DbtModel(
            name="orders",
            database="analytics",
            schema="public",
            columns=[],
            meta={"sst": {"cortex_searchable": True}}
        )
        
        model_without_sst = DbtModel(
            name="temp_table",
            database="analytics",
            schema="public",
            columns=[],
            meta={}
        )
        
        assert model_with_sst.has_sst_metadata() is True
        assert model_without_sst.has_sst_metadata() is False
    
    def test_dbt_model_sst_metadata_access(self):
        """Test accessing SST metadata."""
        included_model = DbtModel(
            name="users",
            database="analytics",
            schema="public",
            columns=[],
            meta={"sst": {"cortex_searchable": True}}
        )
        
        excluded_model = DbtModel(
            name="temp_users",
            database="analytics", 
            schema="public",
            columns=[],
            meta={"sst": {"cortex_searchable": False}}
        )
        
        no_meta_model = DbtModel(
            name="other_table",
            database="analytics",
            schema="public",
            columns=[]
        )
        
        # Test through meta property access
        assert included_model.meta.get("sst", {}).get("cortex_searchable") is True
        assert excluded_model.meta.get("sst", {}).get("cortex_searchable") is False
        assert no_meta_model.meta.get("sst", {}).get("cortex_searchable") is None
    
    def test_dbt_model_primary_key_metadata(self):
        """Test accessing primary key from metadata."""
        single_pk_model = DbtModel(
            name="users",
            database="analytics",
            schema="public",
            columns=[],
            meta={"sst": {"primary_key": "id"}}
        )
        
        composite_pk_model = DbtModel(
            name="user_events",
            database="analytics",
            schema="public",
            columns=[],
            meta={"sst": {"primary_key": ["user_id", "event_date"]}}
        )
        
        no_pk_model = DbtModel(
            name="temp_table",
            database="analytics",
            schema="public",
            columns=[]
        )
        
        # Access through meta property
        assert single_pk_model.meta.get("sst", {}).get("primary_key") == "id"
        assert composite_pk_model.meta.get("sst", {}).get("primary_key") == ["user_id", "event_date"]
        assert no_pk_model.meta.get("sst", {}).get("primary_key") is None
    
    def test_dbt_model_synonyms_metadata(self):
        """Test accessing synonyms from metadata."""
        model = DbtModel(
            name="orders",
            database="analytics",
            schema="public",
            columns=[],
            meta={
                "sst": {
                    "synonyms": ["purchases", "transactions", "sales"]
                }
            }
        )
        
        # Access through meta property
        synonyms = model.meta.get("sst", {}).get("synonyms", [])
        assert "purchases" in synonyms
        assert "transactions" in synonyms
        assert len(synonyms) == 3
        
        # Test model without synonyms
        no_synonyms_model = DbtModel(
            name="users",
            database="analytics",
            schema="public",
            columns=[]
        )
        synonyms = no_synonyms_model.meta.get("sst", {}).get("synonyms", [])
        assert synonyms == []
    
    def test_dbt_model_fully_qualified_name(self):
        """Test getting fully qualified table name."""
        model = DbtModel(
            name="dim_users",
            database="analytics",
            schema="public",
            columns=[]
        )
        
        # Use the actual property name
        full_name = model.fully_qualified_name
        assert full_name == "ANALYTICS.PUBLIC.DIM_USERS"  # Should be uppercase
        
        # Test table_name property
        table_name = model.table_name
        assert table_name == "DIM_USERS"  # Should be uppercase
    
    def test_dbt_model_basic_properties(self):
        """Test basic dbt model properties."""
        model = DbtModel(
            name="users",
            database="analytics",
            schema="public",
            description="User accounts",
            columns=[DbtColumn(name="id", data_type="BIGINT")]
        )
        
        # Test basic properties
        assert model.name == "users"
        assert model.database == "analytics"
        assert model.schema == "public"
        assert model.description == "User accounts"
        assert len(model.columns) == 1
    
    def test_dbt_model_column_access(self):
        """Test column access using actual API."""
        columns = [
            DbtColumn(name="id", data_type="BIGINT"),
            DbtColumn(name="email", data_type="VARCHAR", tests=["unique", "not_null"]),
            DbtColumn(name="status", data_type="VARCHAR")
        ]
        
        model = DbtModel(
            name="users",
            database="analytics", 
            schema="public",
            columns=columns
        )
        
        # Test column access methods that actually exist
        id_column = model.get_column("id")
        assert id_column is not None
        assert id_column.name == "id"
        assert id_column.data_type == "BIGINT"
        
        # Test has_column method
        assert model.has_column("email") is True
        assert model.has_column("nonexistent") is False
        
        # Test column with tests
        email_column = model.get_column("email")
        assert "unique" in email_column.tests
        assert "not_null" in email_column.tests
