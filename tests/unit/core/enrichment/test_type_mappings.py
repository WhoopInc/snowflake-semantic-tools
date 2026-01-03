"""Unit tests for type_mappings module."""

import pytest

from snowflake_semantic_tools.core.enrichment.type_mappings import determine_column_type, map_snowflake_to_sst_datatype


class TestMapSnowflakeToSSTDatatype:
    """Test Snowflake to SST data type mapping."""

    def test_number_types(self):
        """Test numeric type mappings (returns uppercase Snowflake native types)."""
        assert map_snowflake_to_sst_datatype("NUMBER(38,0)") == "NUMBER"
        assert map_snowflake_to_sst_datatype("NUMBER(18,2)") == "NUMBER"
        assert map_snowflake_to_sst_datatype("INT") == "NUMBER"
        assert map_snowflake_to_sst_datatype("INTEGER") == "NUMBER"
        assert map_snowflake_to_sst_datatype("BIGINT") == "NUMBER"
        assert map_snowflake_to_sst_datatype("SMALLINT") == "NUMBER"
        assert map_snowflake_to_sst_datatype("TINYINT") == "NUMBER"
        assert map_snowflake_to_sst_datatype("BYTEINT") == "NUMBER"
        assert map_snowflake_to_sst_datatype("FLOAT") == "FLOAT"
        assert map_snowflake_to_sst_datatype("FLOAT4") == "FLOAT"
        assert map_snowflake_to_sst_datatype("FLOAT8") == "FLOAT"
        assert map_snowflake_to_sst_datatype("DOUBLE") == "FLOAT"
        assert map_snowflake_to_sst_datatype("DOUBLE PRECISION") == "FLOAT"
        assert map_snowflake_to_sst_datatype("REAL") == "FLOAT"
        assert map_snowflake_to_sst_datatype("DECIMAL(10,2)") == "NUMBER"
        assert map_snowflake_to_sst_datatype("NUMERIC(15,3)") == "NUMBER"

    def test_string_types(self):
        """Test string type mappings (returns uppercase TEXT)."""
        assert map_snowflake_to_sst_datatype("VARCHAR(255)") == "TEXT"
        assert map_snowflake_to_sst_datatype("VARCHAR") == "TEXT"
        assert map_snowflake_to_sst_datatype("CHAR(10)") == "TEXT"
        assert map_snowflake_to_sst_datatype("CHARACTER(5)") == "TEXT"
        assert map_snowflake_to_sst_datatype("STRING") == "TEXT"
        assert map_snowflake_to_sst_datatype("TEXT") == "TEXT"

    def test_boolean_types(self):
        """Test boolean type mappings."""
        assert map_snowflake_to_sst_datatype("BOOLEAN") == "BOOLEAN"
        assert map_snowflake_to_sst_datatype("BOOL") == "BOOLEAN"

    def test_date_types(self):
        """Test date type mappings."""
        assert map_snowflake_to_sst_datatype("DATE") == "DATE"

    def test_timestamp_types(self):
        """Test timestamp type mappings."""
        assert map_snowflake_to_sst_datatype("TIMESTAMP") == "TIMESTAMP_NTZ"
        assert map_snowflake_to_sst_datatype("TIMESTAMP_NTZ") == "TIMESTAMP_NTZ"
        assert map_snowflake_to_sst_datatype("TIMESTAMP_LTZ") == "TIMESTAMP_LTZ"
        assert map_snowflake_to_sst_datatype("TIMESTAMP_TZ") == "TIMESTAMP_TZ"
        assert map_snowflake_to_sst_datatype("DATETIME") == "TIMESTAMP_NTZ"

    def test_time_types(self):
        """Test time type mappings."""
        assert map_snowflake_to_sst_datatype("TIME") == "TIME"

    def test_binary_types(self):
        """Test binary type mappings."""
        assert map_snowflake_to_sst_datatype("BINARY") == "TEXT"
        assert map_snowflake_to_sst_datatype("VARBINARY") == "TEXT"

    def test_variant_types(self):
        """Test variant type mappings."""
        assert map_snowflake_to_sst_datatype("VARIANT") == "VARIANT"
        assert map_snowflake_to_sst_datatype("OBJECT") == "OBJECT"
        assert map_snowflake_to_sst_datatype("ARRAY") == "ARRAY"

    def test_case_insensitive(self):
        """Test case insensitivity (input can be any case, output is uppercase)."""
        assert map_snowflake_to_sst_datatype("number(38,0)") == "NUMBER"
        assert map_snowflake_to_sst_datatype("varchar(255)") == "TEXT"
        assert map_snowflake_to_sst_datatype("timestamp_ntz") == "TIMESTAMP_NTZ"

    def test_unknown_type(self):
        """Test unknown type defaults to TEXT."""
        assert map_snowflake_to_sst_datatype("UNKNOWN_TYPE") == "TEXT"
        assert map_snowflake_to_sst_datatype("CUSTOM_TYPE") == "TEXT"


class TestDetermineColumnType:
    """Test column type determination logic."""

    def test_time_dimension_types(self):
        """Test time dimension classification."""
        # Date types
        assert determine_column_type("created_at", "DATE") == "time_dimension"
        assert determine_column_type("updated_at", "DATE") == "time_dimension"

        # Timestamp types
        assert determine_column_type("timestamp", "TIMESTAMP") == "time_dimension"
        assert determine_column_type("event_time", "TIMESTAMP_NTZ") == "time_dimension"
        assert determine_column_type("created_at", "TIMESTAMP_LTZ") == "time_dimension"
        assert determine_column_type("updated_at", "TIMESTAMP_TZ") == "time_dimension"
        assert determine_column_type("datetime", "DATETIME") == "time_dimension"

        # Time type
        assert determine_column_type("time_of_day", "TIME") == "time_dimension"

    def test_boolean_dimension(self):
        """Test boolean columns are dimensions."""
        assert determine_column_type("is_active", "BOOLEAN") == "dimension"
        assert determine_column_type("has_subscription", "BOOLEAN") == "dimension"
        assert determine_column_type("enabled", "BOOL") == "dimension"

    def test_text_dimension(self):
        """Test text columns are dimensions."""
        assert determine_column_type("name", "VARCHAR(255)") == "dimension"
        assert determine_column_type("description", "TEXT") == "dimension"
        assert determine_column_type("status", "STRING") == "dimension"
        assert determine_column_type("category", "CHAR(10)") == "dimension"

    def test_numeric_is_has_dimension(self):
        """Test numeric columns with is_/has_ prefix are dimensions."""
        assert determine_column_type("is_active", "NUMBER(38,0)") == "dimension"
        assert determine_column_type("is_deleted", "INT") == "dimension"
        assert determine_column_type("has_subscription", "NUMBER(1,0)") == "dimension"
        assert determine_column_type("has_payment_method", "TINYINT") == "dimension"

        # Case insensitive
        assert determine_column_type("IS_ACTIVE", "NUMBER(38,0)") == "dimension"
        assert determine_column_type("HAS_SUBSCRIPTION", "INT") == "dimension"

    def test_numeric_fact(self):
        """Test numeric columns without is_/has_ prefix are facts."""
        assert determine_column_type("revenue", "NUMBER(18,2)") == "fact"
        assert determine_column_type("quantity", "INT") == "fact"
        assert determine_column_type("amount", "DECIMAL(10,2)") == "fact"
        assert determine_column_type("count", "BIGINT") == "fact"
        assert determine_column_type("total", "FLOAT") == "fact"
        assert determine_column_type("average", "DOUBLE") == "fact"
        assert determine_column_type("user_id", "NUMBER(38,0)") == "fact"

    def test_edge_cases(self):
        """Test edge cases."""
        # Numeric column starting with "is" but not "is_"
        assert determine_column_type("issue_count", "NUMBER(38,0)") == "fact"

        # Numeric column starting with "has" but not "has_"
        assert determine_column_type("hash_value", "NUMBER(38,0)") == "fact"

        # Empty column name
        assert determine_column_type("", "NUMBER(38,0)") == "fact"

        # Column name with only "is_"
        assert determine_column_type("is_", "NUMBER(38,0)") == "dimension"

    def test_case_sensitivity(self):
        """Test case sensitivity in column names."""
        # Column type should be case-insensitive for is_/has_ check
        assert determine_column_type("IS_ACTIVE", "NUMBER(38,0)") == "dimension"
        assert determine_column_type("HAS_SUBSCRIPTION", "NUMBER(38,0)") == "dimension"
        assert determine_column_type("Is_Active", "NUMBER(38,0)") == "dimension"
        assert determine_column_type("Has_Subscription", "NUMBER(38,0)") == "dimension"
