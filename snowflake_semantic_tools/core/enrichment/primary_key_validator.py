"""
Primary Key Validator

Validates primary key candidates by checking uniqueness in Snowflake.

This module does NOT generate primary key candidates - it only validates
candidates provided by:
- LLM agents (analyzing table structure and relationships)
- User input (via --pk-candidates flag)
- dbt tests (unique + not_null tests on columns)
- External analysis tools

The validator checks each candidate against the actual data in Snowflake
to confirm uniqueness.
"""

from typing import Any, Dict, List, Optional


class PrimaryKeyValidator:
    """Validates primary key candidates for dbt models."""

    def __init__(self, snowflake_client):
        """
        Initialize with Snowflake client.

        Args:
            snowflake_client: SnowflakeClient instance for validation queries
        """
        self.client = snowflake_client

    def detect_primary_key(
        self,
        table_name: str,
        schema_name: str,
        database_name: str,
        columns: List[Dict[str, Any]],
        candidates: Optional[List[List[str]]] = None,
    ) -> Optional[List[str]]:
        """
        Detect primary key for a table by validating provided candidates.

        This method validates candidate primary keys by checking uniqueness in Snowflake.
        It does NOT generate candidates - those should be provided by the caller
        (e.g., from LLM analysis, dbt tests, or user specification).

        Args:
            table_name: Name of the table
            schema_name: Schema containing the table
            database_name: Database containing the table
            columns: List of column information from DESCRIBE TABLE
            candidates: List of primary key candidates to validate (REQUIRED for detection)

        Returns:
            List of column names forming the primary key, or None if no valid candidate found
        """
        # If no candidates provided, return None (don't auto-detect)
        if not candidates:
            return None

        # Validate each candidate in order
        for candidate in candidates:
            if self._validate_candidate(table_name, schema_name, database_name, candidate):
                return candidate

        # No valid candidates found
        return None

    def _validate_candidate(self, table_name: str, schema_name: str, database_name: str, candidate: List[str]) -> bool:
        """
        Validate if a candidate forms a unique primary key.

        Args:
            table_name: Name of the table
            schema_name: Schema containing the table
            database_name: Database containing the table
            candidate: List of column names to test

        Returns:
            bool: True if candidate is valid primary key
        """
        try:
            # Check table size first - skip validation for very large tables
            try:
                row_count = self.client.metadata_manager.get_row_count(table_name, schema_name, database_name)
                # Skip validation if table has more than 10 million rows
                if row_count > 10_000_000:
                    return False
            except:
                pass  # If we can't get row count, proceed with validation

            return self.client.metadata_manager.validate_primary_key(table_name, schema_name, candidate, database_name)
        except Exception as e:
            return False
