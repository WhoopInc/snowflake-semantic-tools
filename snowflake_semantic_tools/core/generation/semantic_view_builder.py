#!/usr/bin/env python3
"""
Semantic View Builder

Generates native Snowflake SEMANTIC VIEW objects from metadata tables.

Creates first-class Snowflake objects that expose semantic models to BI tools
and Cortex Analyst. Semantic views define:
- Business metrics with aggregation logic
- Dimensional attributes for filtering and grouping
- Relationships between tables for join paths
- Custom instructions for AI-guided query generation

The builder queries metadata tables (SM_*) populated by the extract command
and generates CREATE OR REPLACE SEMANTIC VIEW statements executed directly
in Snowflake, making the semantic layer immediately available for consumption.
"""

import json
import re
from typing import Any, Dict, List, Optional

from snowflake_semantic_tools.core.parsing.join_condition_parser import JoinConditionParser, JoinType
from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeClient
from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig
from snowflake_semantic_tools.shared import get_logger
from snowflake_semantic_tools.shared.utils.character_sanitizer import CharacterSanitizer

logger = get_logger("core.generation.semantic_view_builder")


class SemanticViewBuilder:
    """
    Transforms metadata into Snowflake SEMANTIC VIEW SQL statements.

    Queries semantic metadata tables to construct complete semantic view
    definitions including:

    **Structure Components**:
    - Base tables with primary keys and relationships
    - Dimensions and time dimensions for context
    - Facts for row-level numeric values
    - Metrics for aggregated business KPIs

    **Enhanced Features**:
    - Custom instructions for domain-specific AI guidance
    - Verified queries as validated examples
    - Filters for common WHERE conditions

    The generated SQL creates native Snowflake objects that BI tools
    and Cortex Analyst can directly query for semantic-aware analytics.
    """

    def __init__(self, config: SnowflakeConfig, snowflake_loader: Optional[SnowflakeClient] = None):
        """
        Initialize the semantic view builder with required dependencies.

        Args:
            config: SnowflakeConfig instance
            snowflake_loader: Optional SnowflakeClient instance. If None, creates a new one with config.
        """
        self.config = config
        self.snowflake_loader = snowflake_loader or SnowflakeClient(config=config)

        # Set up table references - these will be set by the service
        self.metadata_database = None
        self.metadata_schema = None
        self.target_database = None
        self.target_schema = None

    def build_semantic_view(
        self,
        table_names: List[str],
        view_name: str,
        description: str = "",
        execute: bool = True,
        defer_database: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Build a semantic view for specified tables.

        Args:
            table_names: List of table names to include in the semantic view
            view_name: Name of the semantic view to create
            description: Optional description for the semantic view
            execute: If True, executes SQL in Snowflake (default). If False, returns SQL only (dry run).

        Returns:
            Dictionary containing SQL statement and execution results
        """
        logger.info(f"Building semantic view '{view_name}' with tables: {table_names}")

        # Use a single connection for the entire semantic view building process
        with self.snowflake_loader.get_connection() as conn:
            try:
                # Generate SQL statement using the shared connection
                sql_statement = self._generate_sql(
                    conn, table_names, view_name, description, defer_database=defer_database
                )

                result = {
                    "view_name": view_name,
                    "sql_statement": sql_statement,
                    "success": True,
                    "message": f"Semantic view '{view_name}' SQL generated successfully",
                    "target_location": f"{self.target_database}.{self.target_schema}.{view_name.upper()}",
                }

                if execute:
                    # Execute the SQL statement using the same connection
                    cursor = conn.cursor()
                    logger.info(f"Executing CREATE OR REPLACE SEMANTIC VIEW statement...")
                    cursor.execute(sql_statement)
                    logger.info(f"Semantic view '{view_name}' created successfully")
                    result["message"] = f"Semantic view '{view_name}' created successfully"

                return result

            except Exception as e:
                error_msg = str(e)
                if "does not exist or not authorized" in error_msg and "Schema" in error_msg:
                    logger.error(f"Target schema '{self.target_database}.{self.target_schema}' does not exist")
                    logger.error(
                        f"Please ask your admin to create schema: CREATE SCHEMA IF NOT EXISTS {self.target_database}.{self.target_schema}"
                    )
                else:
                    logger.error(f"Error building semantic view '{view_name}': {e}")

                return {
                    "view_name": view_name,
                    "sql_statement": None,
                    "success": False,
                    "message": f"Error building semantic view '{view_name}': {error_msg}",
                    "target_location": f"{self.target_database}.{self.target_schema}.{view_name.upper()}",
                }

    def build_all_semantic_views(self, execute: bool = True) -> Dict[str, Any]:
        """
        Build all semantic views configured in the sm_semantic_views table.

        Returns:
            Dict containing information about all created views including:
                success_count: Number of views created successfully
                error_count: Number of views that failed to create
                views_created: List of successfully created views
                errors: List of error messages for failed views
                total_processed: Total number of view configurations processed

        Raises:
            Exception: If unable to query the semantic views configuration table
        """
        logger.info("Building all semantic views from configuration table")

        # Use a single connection for the entire process
        with self.snowflake_loader.get_connection() as conn:
            try:
                # Query the semantic views configuration table
                semantic_views_configs = self._get_semantic_views_configs(conn)

                if not semantic_views_configs:
                    logger.warning("No semantic views configurations found in the database")
                    return {
                        "success_count": 0,
                        "error_count": 0,
                        "views_created": [],
                        "errors": [],
                        "total_processed": 0,
                    }

                total_views = len(semantic_views_configs)
                logger.info(f"Found {total_views} semantic view configuration{'s' if total_views != 1 else ''}")

                # Process each configuration
                views_created = []
                errors = []

                for i, config in enumerate(semantic_views_configs, 1):
                    try:
                        view_name = config["name"]
                        tables = config["tables"]
                        description = config.get("description", "")

                        # Show progress for multiple views
                        if total_views > 1:
                            logger.info(f"Processing view {i} of {total_views}: {view_name}")
                        else:
                            logger.info(f"Processing semantic view: {view_name}")

                        # Build the semantic view using the shared connection
                        result = self._build_semantic_view(conn, tables, view_name, description, execute)

                        # Add description to the result
                        result["description"] = description
                        views_created.append(result)

                        # Show completion progress for multiple views
                        if total_views > 1:
                            logger.info(f"Successfully created view {i} of {total_views}: {view_name}")
                        else:
                            logger.info(f"Successfully created semantic view: {view_name}")

                    except Exception as e:
                        error_msg = f"Failed to create semantic view '{config.get('name', 'unknown')}': {str(e)}"
                        logger.error(error_msg)
                        errors.append(error_msg)
                        continue

                success_count = len(views_created)
                error_count = len(errors)
                total_processed = len(semantic_views_configs)

                result = {
                    "success_count": success_count,
                    "error_count": error_count,
                    "views_created": views_created,
                    "errors": errors,
                    "total_processed": total_processed,
                }

                logger.info(
                    f"Completed semantic view generation: {success_count} successful, {error_count} failed, {total_processed} total"
                )

                return result

            except Exception as e:
                error_msg = f"Error building semantic views from configuration: {str(e)}"
                logger.error(error_msg)
                raise Exception(error_msg)

    def _get_semantic_views_configs(self, conn) -> List[Dict[str, Any]]:
        """
        Query the sm_semantic_views table to get all semantic view configurations.

        Returns:
            List of dictionaries containing semantic view configurations
        """
        try:
            semantic_views_table = "SM_SEMANTIC_VIEWS"
            sql = f"SELECT NAME, DESCRIPTION, TABLES FROM {self.metadata_database}.{self.metadata_schema}.{semantic_views_table}"

            rows = self._execute_query(conn, sql)

            # Parse the configurations
            configs = []
            for row in rows:
                config = {
                    "name": row["NAME"],
                    "description": row["DESCRIPTION"],
                    "tables": self._parse_semantic_views_table_list(row["TABLES"]),
                }
                configs.append(config)

            return configs

        except Exception as e:
            logger.error(f"Error querying semantic views configurations: {e}")
            raise

    def _parse_semantic_views_table_list(self, raw: Any) -> List[str]:
        """Parse the tables column from sm_semantic_views table."""
        if raw is None:
            return []

        # Convert to string if needed
        if not isinstance(raw, str):
            raw = str(raw)

        raw_str = raw.strip()

        # Handle multiple levels of JSON encoding by repeatedly decoding
        current = raw_str
        max_attempts = 5  # Prevent infinite loops
        attempts = 0

        while attempts < max_attempts:
            try:
                # Try to decode JSON
                decoded = json.loads(current)

                # If we get a list, we're done
                if isinstance(decoded, list):
                    # Flatten and deduplicate the list
                    tables = []
                    for item in decoded:
                        # If the item is itself a JSON string, decode it
                        if isinstance(item, str) and item.strip().startswith("[") and item.strip().endswith("]"):
                            try:
                                nested = json.loads(item)
                                if isinstance(nested, list):
                                    tables.extend([str(t).lower().strip() for t in nested])
                                else:
                                    tables.append(str(item).lower().strip())
                            except json.JSONDecodeError:
                                tables.append(str(item).lower().strip())
                        else:
                            tables.append(str(item).lower().strip())

                    # Remove duplicates while preserving order
                    unique_tables = []
                    seen = set()
                    for table in tables:
                        if table and table not in seen:
                            unique_tables.append(table)
                            seen.add(table)

                    return unique_tables

                # If we get a string, try to decode it again
                if isinstance(decoded, str):
                    current = decoded
                    attempts += 1
                    continue

                # If we get something else, break
                break

            except json.JSONDecodeError:
                # If JSON decoding fails, check if it looks like a simple list
                if current.startswith("[") and current.endswith("]"):
                    # Try to extract table names with simple parsing
                    try:
                        # Remove brackets and split by comma
                        content = current[1:-1].strip()
                        if content:
                            # Split by comma and clean up each table name
                            tables = []
                            for item in content.split(","):
                                # Remove quotes and whitespace
                                table = item.strip().strip("\"'")
                                if table:
                                    tables.append(table.lower())
                            return list(dict.fromkeys(tables))  # Remove duplicates preserving order
                    except Exception:
                        pass

                # If all else fails, treat as single table
                break

        logger.warning(f"Could not parse tables as JSON after {attempts} attempts: {raw_str}")
        # Fallback: treat as single table name, removing any quotes
        fallback = raw_str.strip().strip("\"'").lower()
        return [fallback] if fallback else []

    def _parse_table_list(self, raw: Any) -> List[str]:
        """Parse a TABLE_NAME column that can be various forms of encoded JSON."""
        if raw is None:
            return []
        if isinstance(raw, list):
            return [str(x).lower() for x in raw]

        raw_str = str(raw).strip()

        # Try to extract table names using multiple patterns
        # Pattern 1: Look for ['TABLE_NAME'] anywhere in the string
        single_quote_matches = re.findall(r"\['([^']+)'\]", raw_str)
        if single_quote_matches:
            return [match.lower() for match in single_quote_matches]

        # Pattern 2: Look for ["TABLE_NAME"] anywhere in the string
        double_quote_matches = re.findall(r'\["([^"]+)"\]', raw_str)
        if double_quote_matches:
            return [match.lower() for match in double_quote_matches]

        # Pattern 3: Look for simple table names between quotes
        simple_matches = re.findall(r'"([a-zA-Z_][a-zA-Z0-9_]*)"', raw_str)
        if simple_matches:
            # Filter out obvious non-table names
            table_names = [match for match in simple_matches if len(match) > 2 and "_" in match.upper()]
            if table_names:
                return [name.lower() for name in table_names]

        # Handle regular JSON array
        try:
            if raw_str.startswith("[") and raw_str.endswith("]"):
                parsed = json.loads(raw_str)
                return [str(x).lower() for x in parsed]
        except Exception:
            pass

        # Handle single string
        return [raw_str.lower()]

    def _all_tables_present(self, tables: List[str], selected: List[str]) -> bool:
        """Return True if every table in `tables` is contained in `selected`."""
        sel_set = {t.lower() for t in selected}
        return all(t in sel_set for t in tables)

    def _parse_json_field(self, field_value: Any, field_name: str = "field") -> Any:
        """
        Parse a field that might be JSON-encoded (possibly multiple times).

        Args:
            field_value: Raw field value (could be string, list, dict, etc.)
            field_name: Name of field for logging

        Returns:
            Parsed Python object (list, dict, str, etc.)
        """
        if field_value is None:
            return None

        # If already a list or dict, return as-is
        if isinstance(field_value, (list, dict)):
            return field_value

        # Try to parse as JSON (handle double/triple encoding)
        current = str(field_value).strip()
        for _ in range(3):  # Max 3 decode attempts
            try:
                decoded = json.loads(current)
                if isinstance(decoded, str):
                    current = decoded  # Decode again
                else:
                    return decoded  # Got the actual object
            except json.JSONDecodeError:
                break

        # Couldn't parse as JSON, return as string
        return field_value

    def _sanitize_description(self, description: str) -> str:
        """Sanitize description for SQL string literal."""
        return CharacterSanitizer.sanitize_for_sql_string(description)

    def _execute_query(self, conn, sql: str) -> List[Dict]:
        """Execute a SQL query using the provided connection and return results as list of dictionaries."""
        try:
            cursor = conn.cursor()
            cursor.execute(sql)

            # Get column names (keep uppercase as returned by Snowflake)
            columns = [desc[0] for desc in cursor.description]

            # Fetch all rows and convert to dictionaries
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            logger.error(f"Error executing query: {sql} - {e}")
            raise

    def _get_table_info(self, conn, table_name: str) -> Dict:
        """Get basic table metadata."""
        try:
            table_table = "SM_TABLES"
            sql = f"SELECT * EXCLUDE TABLE_NAME FROM {self.metadata_database}.{self.metadata_schema}.{table_table} WHERE LOWER(TABLE_NAME) = '{table_name.lower()}'"
            rows = self._execute_query(conn, sql)

            if rows:
                return rows[0]
        except Exception:
            pass

        # Return minimal defaults if no metadata found - caller must validate required fields
        logger.warning(f"No metadata found for table '{table_name}' in SM_TABLES")
        return {
            "table_name": table_name.upper(),
            "description": f"Table: {table_name}",
        }

    def _query_metadata_table(
        self, conn, metadata_table: str, table_name: str, exclude_table_name: bool = True
    ) -> List[Dict]:
        """
        Generic method to query metadata tables by table name.

        Args:
            conn: Database connection
            metadata_table: Name of metadata table (SM_DIMENSIONS, SM_FACTS, etc.)
            table_name: Table name to filter by
            exclude_table_name: If True, excludes TABLE_NAME column from results

        Returns:
            List of dictionaries with query results
        """
        try:
            exclude_clause = " EXCLUDE TABLE_NAME" if exclude_table_name else ""
            sql = f"SELECT *{exclude_clause} FROM {self.metadata_database}.{self.metadata_schema}.{metadata_table} WHERE LOWER(TABLE_NAME) = '{table_name.lower()}'"
            return self._execute_query(conn, sql)
        except Exception as e:
            logger.error(f"Error querying {metadata_table} for {table_name}: {e}")
            return []

    def _get_dimensions(self, conn, table_name: str) -> List[Dict]:
        """Get dimensions for a table."""
        return self._query_metadata_table(conn, "SM_DIMENSIONS", table_name)

    def _get_facts(self, conn, table_name: str) -> List[Dict]:
        """Get facts for a table."""
        return self._query_metadata_table(conn, "SM_FACTS", table_name)

    def _get_time_dimensions(self, conn, table_name: str) -> List[Dict]:
        """Get time dimensions for a table."""
        return self._query_metadata_table(conn, "SM_TIME_DIMENSIONS", table_name)

    def _find_table_schema(self, conn, table_name: str, database: str) -> Optional[str]:
        """
        Find the schema where a table actually exists by querying Snowflake's information schema.

        Args:
            conn: Database connection
            table_name: Name of the table to find
            database: Database to search in

        Returns:
            Schema name where the table exists, or None if not found
        """
        try:
            # Query information schema to find the table
            sql = f"""
            SELECT table_schema
            FROM information_schema.tables 
            WHERE UPPER(table_catalog) = UPPER('{database}')
            AND UPPER(table_name) = UPPER('{table_name}')
            AND table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY table_schema
            LIMIT 1
            """

            rows = self._execute_query(conn, sql)
            if rows:
                schema = rows[0]["TABLE_SCHEMA"]
                logger.debug(f"Found table '{table_name}' in schema '{schema}' via information_schema")
                return schema
            else:
                logger.warning(f"Table '{table_name}' not found in database '{database}' via information_schema")
                return None

        except Exception as e:
            logger.error(f"Error querying information_schema for table '{table_name}': {e}")
            return None

    def _get_metrics_for_selected_tables(self, conn, table_names: List[str]) -> List[Dict]:
        """Get metrics for a list of selected tables."""
        normalized_table_names = [t.lower() for t in table_names]

        # Fetch all metrics once
        table_name = "SM_METRICS"
        sql = f"SELECT * FROM {self.metadata_database}.{self.metadata_schema}.{table_name}"
        rows = self._execute_query(conn, sql)

        # Process metrics
        metrics = []

        for row in rows:
            referenced_tables = self._parse_table_list(row.get("TABLE_NAME"))

            # Only include if ALL referenced tables are in our selected tables
            if not self._all_tables_present(referenced_tables, normalized_table_names):
                continue

            metrics.append(row)

        return metrics

    def _get_relationships(self, conn, table_list: List[str]) -> List[Dict]:
        """Get relationships between tables."""
        tlist = ",".join([f"'{t.lower()}'" for t in table_list])
        relationship_table = "SM_RELATIONSHIPS"
        sql = f"SELECT * FROM {self.metadata_database}.{self.metadata_schema}.{relationship_table} WHERE LOWER(LEFT_TABLE_NAME) IN ({tlist}) AND LOWER(RIGHT_TABLE_NAME) IN ({tlist})"
        rows = self._execute_query(conn, sql)

        relationships = []
        for row in rows:
            # Get relationship columns
            rel_columns = self._get_relationship_columns(conn, row["RELATIONSHIP_NAME"])
            row["RELATIONSHIP_COLUMNS"] = rel_columns
            relationships.append(row)

        return relationships

    def _get_relationship_columns(self, conn, rel_name: str) -> List[Dict]:
        """Get relationship columns (now join conditions) helper."""
        relationship_column_table = "SM_RELATIONSHIP_COLUMNS"
        sql = f"SELECT JOIN_CONDITION, CONDITION_TYPE, LEFT_EXPRESSION, RIGHT_EXPRESSION, OPERATOR FROM {self.metadata_database}.{self.metadata_schema}.{relationship_column_table} WHERE LOWER(RELATIONSHIP_NAME) = '{rel_name.lower()}'"
        return self._execute_query(conn, sql)

    def _build_tables_clause(self, conn, table_names: List[str], defer_database: Optional[str] = None) -> str:
        """
        Build the TABLES clause of the CREATE SEMANTIC VIEW statement.

        Args:
            conn: Database connection
            table_names: List of table names
            defer_database: If set, override database from metadata (like dbt defer)
        """
        table_definitions = []

        for table_name in table_names:
            table_info = self._get_table_info(conn, table_name)

            # Get physical table reference
            database = table_info.get("DATABASE")
            if not database:
                if not self.target_database:
                    raise ValueError(
                        f"No database reference found for table '{table_name}' and no target_database configured"
                    )
                database = self.target_database
                logger.warning(
                    f"No database reference found in metadata for table '{table_name}', using target_database: {database}"
                )

            # DEFER MODE: Override database if defer_database is set (like dbt defer)
            if defer_database:
                logger.debug(f"Defer mode: Using {defer_database} instead of {database} for table {table_name}")
                database = defer_database

            schema = table_info.get("SCHEMA")
            if not schema:
                # Try to find the table in Snowflake's information schema
                logger.warning(
                    f"No schema reference found for table '{table_name}' in metadata. Attempting to find table in Snowflake..."
                )
                schema = self._find_table_schema(conn, table_name, database)
                if not schema:
                    raise ValueError(
                        f"Table '{table_name}' not found in database '{database}'. Please ensure the table exists and metadata is properly extracted."
                    )
                logger.info(f"Found table '{table_name}' in schema '{schema}'")

            physical_table = table_info.get("TABLE_NAME", table_name.upper())

            table_def = f"    {table_name.upper()} AS {database}.{schema}.{physical_table}"

            # Add primary key if available
            if table_info.get("PRIMARY_KEY"):
                primary_key_cols = self._parse_json_field(table_info["PRIMARY_KEY"], "primary_key")
                if primary_key_cols and isinstance(primary_key_cols, list):
                    pk_cols = ", ".join([col.upper() for col in primary_key_cols])
                    table_def += f"\n      PRIMARY KEY ({pk_cols})"

            # Add unique keys if available
            if table_info.get("UNIQUE_KEYS"):
                unique_key_cols = self._parse_json_field(table_info["UNIQUE_KEYS"], "unique_keys")
                if unique_key_cols and isinstance(unique_key_cols, list):
                    uk_cols = ", ".join([col.upper() for col in unique_key_cols])
                    table_def += f"\n      UNIQUE ({uk_cols})"

            # Add synonyms if available
            if table_info.get("SYNONYMS"):
                synonyms = self._parse_json_field(table_info["SYNONYMS"], "synonyms")
                if synonyms and isinstance(synonyms, list):
                    synonyms_cleaned = CharacterSanitizer.sanitize_synonym_list(synonyms)
                    if synonyms_cleaned:
                        synonyms_str = ", ".join([f"'{syn}'" for syn in synonyms_cleaned])
                        table_def += f"\n      WITH SYNONYMS ({synonyms_str})"

            # Add comment
            description = table_info.get("DESCRIPTION", f"Table: {table_name}")
            description = self._sanitize_description(description)
            table_def += f"\n      COMMENT = '{description}'"

            table_definitions.append(table_def)

        return ",\n".join(table_definitions)

    def _build_relationships_clause(self, conn, table_names: List[str]) -> str:
        """Build the RELATIONSHIPS clause of the CREATE SEMANTIC VIEW statement ."""
        relationships = self._get_relationships(conn, table_names)

        if not relationships:
            return ""

        rel_definitions = []

        for rel in relationships:
            rel_name = rel["RELATIONSHIP_NAME"]
            left_table = rel["LEFT_TABLE_NAME"].upper()
            right_table = rel["RIGHT_TABLE_NAME"].upper()

            # Get relationship conditions (new format)
            rel_conditions = rel.get("RELATIONSHIP_COLUMNS", [])  # Still called RELATIONSHIP_COLUMNS in the result
            if not rel_conditions:
                logger.warning(f"No relationship conditions found for {rel_name}")
                continue

            # Parse join conditions
            parsed_conditions = []
            for condition_row in rel_conditions:
                join_condition = condition_row.get("JOIN_CONDITION")
                if join_condition:
                    parsed = JoinConditionParser.parse(join_condition)
                    parsed_conditions.append(parsed)

            if not parsed_conditions:
                logger.warning(f"Could not parse conditions for {rel_name}")
                continue

            # Generate SQL based on parsed conditions
            sql_ref = JoinConditionParser.generate_sql_references(parsed_conditions, left_table, right_table)

            if sql_ref:
                rel_def = f"    {rel_name.upper()} AS\n      {sql_ref}"
                rel_definitions.append(rel_def)

        return ",\n".join(rel_definitions)

    def _build_facts_clause(self, conn, table_names: List[str]) -> str:
        """Build the FACTS clause of the CREATE SEMANTIC VIEW statement ."""
        all_facts = []

        for table_name in table_names:
            facts = self._get_facts(conn, table_name)
            for fact in facts:
                fact["source_table"] = table_name
                all_facts.append(fact)

        if not all_facts:
            return ""

        fact_definitions = []

        for fact in all_facts:
            table_name = fact["source_table"].upper()
            fact_name = fact["NAME"].upper()
            expression = fact["EXPR"]

            fact_def = f"    {table_name}.{fact_name} AS {expression}"

            # Add comment if available
            if fact.get("DESCRIPTION"):
                description = self._sanitize_description(fact["DESCRIPTION"])
                fact_def += f"\n      COMMENT = '{description}'"

            fact_definitions.append(fact_def)

        return ",\n".join(fact_definitions)

    def _build_dimensions_clause(self, conn, table_names: List[str]) -> str:
        """Build the DIMENSIONS clause of the CREATE SEMANTIC VIEW statement ."""
        all_dimensions = []

        for table_name in table_names:
            dimensions = self._get_dimensions(conn, table_name)
            time_dimensions = self._get_time_dimensions(conn, table_name)

            # Add regular dimensions
            for dim in dimensions:
                dim["source_table"] = table_name
                all_dimensions.append(dim)

            # Add time dimensions
            for time_dim in time_dimensions:
                time_dim["source_table"] = table_name
                all_dimensions.append(time_dim)

        if not all_dimensions:
            return ""

        dim_definitions = []

        for dim in all_dimensions:
            table_name = dim["source_table"].upper()
            dim_name = dim["NAME"].upper()
            expression = dim["EXPR"]

            dim_def = f"    {table_name}.{dim_name} AS {expression}"

            # Add synonyms if available
            if dim.get("SYNONYMS"):
                synonyms = self._parse_json_field(dim["SYNONYMS"], "synonyms")
                if synonyms and isinstance(synonyms, list):
                    synonyms_filtered = [s for s in synonyms if s is not None]
                    if synonyms_filtered:
                        synonyms_cleaned = CharacterSanitizer.sanitize_synonym_list(synonyms_filtered)
                        if synonyms_cleaned:
                            synonyms_str = ", ".join([f"'{syn}'" for syn in synonyms_cleaned])
                            dim_def += f"\n      WITH SYNONYMS = ({synonyms_str})"

            # Note: Sample values are stored in metadata tables for future use
            # They will be included automatically when Snowflake releases SAMPLE VALUES support

            # Add comment if available
            if dim.get("DESCRIPTION"):
                description = self._sanitize_description(dim["DESCRIPTION"])
                dim_def += f"\n      COMMENT = '{description}'"

            dim_definitions.append(dim_def)

        return ",\n".join(dim_definitions)

    def _extract_table_references_from_expression(self, expression: str) -> List[str]:
        """
        Extract all table references from a metric expression.

        Looks for patterns like TABLE_NAME.COLUMN_NAME in SQL expressions.
        Returns a list of unique table names referenced in the expression.

        Args:
            expression: SQL expression that may contain table.column references

        Returns:
            List of unique table names found in the expression
        """
        # Pattern to match table.column references
        # Handles uppercase, lowercase, and mixed case table names
        pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\b"
        matches = re.findall(pattern, expression)

        # Extract unique table names (first group in each match)
        table_names = set()
        for table_ref, column_ref in matches:
            # Skip common SQL keywords that might match the pattern
            if table_ref.upper() not in ["CAST", "EXTRACT", "TRIM", "CONVERT"]:
                table_names.add(table_ref.lower())

        return sorted(list(table_names))

    def _build_metrics_clause(self, conn, table_names: List[str]) -> str:
        """Build the METRICS clause of the CREATE SEMANTIC VIEW statement ."""
        metrics = self._get_metrics_for_selected_tables(conn, table_names)

        if not metrics:
            return ""

        # Create a set of normalized table names for quick lookup
        available_tables = set(t.lower() for t in table_names)

        # Get all defined facts and dimensions to validate metric references
        defined_columns = set()
        for table_name in table_names:
            dimensions = self._get_dimensions(conn, table_name)
            facts = self._get_facts(conn, table_name)
            time_dimensions = self._get_time_dimensions(conn, table_name)

            # Add all defined fact and dimension column names
            for dim in dimensions + time_dimensions + facts:
                defined_columns.add(f"{table_name.upper()}.{dim['NAME'].upper()}")

        metric_definitions = []
        skipped_metrics = []

        for metric in metrics:
            metric_name = metric["NAME"].upper()
            expression = metric["EXPR"]

            # Extract all table references from the metric expression
            referenced_tables_in_expr = self._extract_table_references_from_expression(expression)

            # Validate that all referenced tables are available in this semantic view
            missing_tables = []
            for table_ref in referenced_tables_in_expr:
                if table_ref not in available_tables:
                    missing_tables.append(table_ref)

            # Skip metric if it references tables not in the semantic view
            if missing_tables:
                skipped_metrics.append(
                    {"metric": metric_name, "missing_tables": missing_tables, "available": list(available_tables)}
                )
                logger.debug(
                    f"Skipping metric '{metric_name}' - references table(s) not in semantic view: "
                    f"{', '.join(missing_tables)}. Available tables: {', '.join(available_tables)}"
                )
                continue

            # Find primary table for this metric
            referenced_tables = self._parse_table_list(metric.get("TABLE_NAME"))
            primary_table = None
            if referenced_tables:
                # Use first referenced table that's in our selected tables
                for table in referenced_tables:
                    if table in [t.lower() for t in table_names]:
                        primary_table = table.upper()
                        break

            if not primary_table:
                primary_table = table_names[0].upper()  # Fallback to first table

            metric_def = f"    {primary_table}.{metric_name} AS {expression}"

            # Add comment if available
            if metric.get("DESCRIPTION"):
                description = metric["DESCRIPTION"].replace("'", "''")
                metric_def += f"\n      COMMENT = '{description}'"

            metric_definitions.append(metric_def)

        # Log summary of skipped metrics if any
        if skipped_metrics:
            logger.info(
                f"Skipped {len(skipped_metrics)} metric(s) due to missing table references: "
                f"{', '.join([m['metric'] for m in skipped_metrics])}"
            )

        return ",\n".join(metric_definitions)

    def _generate_sql(
        self, conn, table_names: List[str], view_name: str, description: str = "", defer_database: Optional[str] = None
    ) -> str:
        """Generate the CREATE OR REPLACE SEMANTIC VIEW SQL statement ."""
        logger.info(f"Generating SQL for semantic view '{view_name}'")

        if defer_database:
            logger.info(
                f"Using defer mode: table references will use database '{defer_database}' instead of metadata database"
            )

        # Always use CREATE OR REPLACE for atomic operation
        sql_parts = [f"CREATE OR REPLACE SEMANTIC VIEW {self.target_database}.{self.target_schema}.{view_name.upper()}"]

        # Build TABLES clause
        logger.info("Building TABLES clause...")
        tables_clause = self._build_tables_clause(conn, table_names, defer_database=defer_database)
        sql_parts.append(f"  TABLES (\n{tables_clause}\n  )")

        # Build RELATIONSHIPS clause
        logger.info("Building RELATIONSHIPS clause...")
        relationships_clause = self._build_relationships_clause(conn, table_names)
        if relationships_clause:
            sql_parts.append(f"  RELATIONSHIPS (\n{relationships_clause}\n  )")

        # Build FACTS clause
        logger.info("Building FACTS clause...")
        facts_clause = self._build_facts_clause(conn, table_names)
        if facts_clause:
            sql_parts.append(f"  FACTS (\n{facts_clause}\n  )")

        # Build DIMENSIONS clause
        logger.info("Building DIMENSIONS clause...")
        dimensions_clause = self._build_dimensions_clause(conn, table_names)
        if dimensions_clause:
            sql_parts.append(f"  DIMENSIONS (\n{dimensions_clause}\n  )")

        # Build METRICS clause
        logger.info("Building METRICS clause...")
        metrics_clause = self._build_metrics_clause(conn, table_names)
        if metrics_clause:
            sql_parts.append(f"  METRICS (\n{metrics_clause}\n  )")

        # Add comment - use provided description or fallback to generic message
        if description and description.strip():
            # Escape single quotes in description
            comment = description.replace("'", "''")
        else:
            comment = f"Semantic view generated for tables: {', '.join(table_names)}"
        sql_parts.append(f"  COMMENT = '{comment}'")

        # Join all parts
        logger.info("Finalizing SQL statement...")
        full_sql = "\n".join(sql_parts) + ";"

        # Log SQL in structured format
        logger.debug(
            f"Generated SQL for view '{view_name}' ({len(full_sql)} characters)",
            extra={"sql": full_sql, "view_name": view_name},
        )
        logger.info(f"SQL generation completed for '{view_name}'")

        return full_sql

    def _build_semantic_view(
        self, conn, table_names: List[str], view_name: str, description: str = "", execute: bool = True
    ) -> Dict[str, Any]:
        """
        Build a semantic view .

        Args:
            conn: Shared database connection
            table_names: List of table names to include in the semantic view
            view_name: Name of the semantic view to create
            description: Optional description for the semantic view
            execute: If True, executes SQL in Snowflake (default). If False, returns SQL only (dry run).

        Returns:
            Dictionary containing SQL statement and execution results
        """
        logger.info(f"Building semantic view '{view_name}' with tables: {table_names}")

        try:
            # Generate SQL statement using the shared connection
            sql_statement = self._generate_sql(conn, table_names, view_name, description)

            result = {
                "view_name": view_name,
                "sql_statement": sql_statement,
                "success": True,
                "message": f"Semantic view '{view_name}' SQL generated successfully",
                "target_location": f"{self.target_database}.{self.target_schema}.{view_name.upper()}",
            }

            if execute:
                # Execute the SQL statement using the same connection
                cursor = conn.cursor()
                logger.info(f"Executing CREATE OR REPLACE SEMANTIC VIEW statement...")
                cursor.execute(sql_statement)
                logger.info(f"Semantic view '{view_name}' created successfully")
                result["message"] = f"Semantic view '{view_name}' created successfully"

            return result

        except Exception as e:
            error_msg = str(e)
            if "does not exist or not authorized" in error_msg and "Schema" in error_msg:
                logger.error(f"Target schema '{self.target_database}.{self.target_schema}' does not exist")
                logger.error(
                    f"Please ask your admin to create schema: CREATE SCHEMA IF NOT EXISTS {self.target_database}.{self.target_schema}"
                )
            else:
                logger.error(f"Error building semantic view '{view_name}': {e}")

            return {
                "view_name": view_name,
                "sql_statement": None,
                "success": False,
                "message": f"Error building semantic view '{view_name}': {error_msg}",
                "target_location": f"{self.target_database}.{self.target_schema}.{view_name.upper()}",
            }
