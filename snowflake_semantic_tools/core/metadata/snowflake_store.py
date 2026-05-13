"""
SnowflakeStore

Reads semantic metadata from SM_* tables in Snowflake.
Backward-compatible store for --from-snowflake mode.
"""

import json
from typing import Any, Dict, List, Optional

from snowflake_semantic_tools.core.metadata.store import MetadataStore
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


class SnowflakeStore(MetadataStore):
    """Reads from SM_* tables in Snowflake (legacy mode)."""

    def __init__(self, conn, metadata_database: str, metadata_schema: str):
        self._conn = conn
        self._db = metadata_database
        self._schema = metadata_schema

    def _execute(self, sql: str) -> List[Dict[str, Any]]:
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Error executing query: {sql} - {e}")
            raise

    def _fq(self, table: str) -> str:
        return f"{self._db}.{self._schema}.{table}"

    def get_semantic_views(self) -> List[Dict[str, Any]]:
        sql = (
            f"SELECT NAME, TABLES, DESCRIPTION, CUSTOM_INSTRUCTIONS FROM {self._fq('SM_SEMANTIC_VIEWS')} ORDER BY NAME"
        )
        return self._execute(sql)

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        try:
            sql = f"SELECT * EXCLUDE TABLE_NAME FROM {self._fq('SM_TABLES')} WHERE LOWER(TABLE_NAME) = '{table_name.lower()}'"
            rows = self._execute(sql)
            if rows:
                return rows[0]
        except Exception:
            pass
        logger.warning(f"No metadata found for table '{table_name}' in SM_TABLES")
        return {
            "TABLE_NAME": table_name.upper(),
            "DESCRIPTION": f"Table: {table_name}",
        }

    def get_dimensions(self, table_name: str) -> List[Dict[str, Any]]:
        return self._query_by_table("SM_DIMENSIONS", table_name)

    def get_time_dimensions(self, table_name: str) -> List[Dict[str, Any]]:
        return self._query_by_table("SM_TIME_DIMENSIONS", table_name)

    def get_facts(self, table_name: str) -> List[Dict[str, Any]]:
        return self._query_by_table("SM_FACTS", table_name)

    def get_metrics(self, table_names: List[str]) -> List[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._fq('SM_METRICS')}"
        all_metrics = self._execute(sql)
        normalized = {t.lower() for t in table_names}
        result = []
        for metric in all_metrics:
            tables_field = metric.get("TABLE_NAME", "")
            try:
                metric_tables = json.loads(tables_field) if isinstance(tables_field, str) else tables_field
            except (json.JSONDecodeError, TypeError):
                metric_tables = [tables_field] if tables_field else []
            if not isinstance(metric_tables, list):
                metric_tables = [str(metric_tables)]
            metric_table_names = {str(t).lower() for t in metric_tables}
            if metric_table_names and metric_table_names.issubset(normalized):
                result.append(metric)
        return result

    def get_relationships(self, table_names: List[str]) -> List[Dict[str, Any]]:
        quoted = ", ".join(f"'{t.lower()}'" for t in table_names)
        sql = (
            f"SELECT * FROM {self._fq('SM_RELATIONSHIPS')} "
            f"WHERE LOWER(LEFT_TABLE_NAME) IN ({quoted}) "
            f"AND LOWER(RIGHT_TABLE_NAME) IN ({quoted})"
        )
        return self._execute(sql)

    def get_relationship_columns(self, relationship_name: str) -> List[Dict[str, Any]]:
        sql = (
            f"SELECT JOIN_CONDITION, CONDITION_TYPE, LEFT_EXPRESSION, RIGHT_EXPRESSION, OPERATOR "
            f"FROM {self._fq('SM_RELATIONSHIP_COLUMNS')} "
            f"WHERE LOWER(RELATIONSHIP_NAME) = '{relationship_name.lower()}'"
        )
        return self._execute(sql)

    def get_verified_queries(self, table_names: List[str]) -> List[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._fq('SM_VERIFIED_QUERIES')}"
        all_vqs = self._execute(sql)
        normalized = {t.lower() for t in table_names}
        result = []
        for vq in all_vqs:
            tables_field = vq.get("TABLES", "")
            try:
                vq_tables = json.loads(tables_field) if isinstance(tables_field, str) else tables_field
            except (json.JSONDecodeError, TypeError):
                vq_tables = [tables_field] if tables_field else []
            if not isinstance(vq_tables, list):
                vq_tables = [str(vq_tables)]
            vq_table_names = {str(t).lower() for t in vq_tables}
            if vq_table_names and vq_table_names.issubset(normalized):
                result.append(vq)
        return result

    def get_custom_instructions(self, names: List[str]) -> List[Dict[str, Any]]:
        if not names:
            return []
        placeholders = ", ".join(f"'{n.upper()}'" for n in names)
        sql = (
            f"SELECT NAME, QUESTION_CATEGORIZATION, SQL_GENERATION "
            f"FROM {self._fq('SM_CUSTOM_INSTRUCTIONS')} "
            f"WHERE UPPER(NAME) IN ({placeholders})"
        )
        return self._execute(sql)

    def get_filters(self, table_names: List[str]) -> List[Dict[str, Any]]:
        if not table_names:
            return []
        placeholders = ", ".join(f"'{t.upper()}'" for t in table_names)
        sql = (
            f"SELECT NAME, TABLE_NAME, DESCRIPTION, EXPR "
            f"FROM {self._fq('SM_FILTERS')} "
            f"WHERE UPPER(TABLE_NAME) IN ({placeholders})"
        )
        return self._execute(sql)

    def _query_by_table(self, table: str, table_name: str) -> List[Dict[str, Any]]:
        try:
            sql = (
                f"SELECT * EXCLUDE TABLE_NAME FROM {self._fq(table)} "
                f"WHERE LOWER(TABLE_NAME) = '{table_name.lower()}'"
            )
            return self._execute(sql)
        except Exception as e:
            logger.error(f"Error querying {table} for {table_name}: {e}")
            return []
