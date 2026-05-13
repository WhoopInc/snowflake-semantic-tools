"""
InMemoryStore

Reads compiled semantic metadata from an in-memory dict (loaded from
sst_manifest.json). No Snowflake connection required.

All returned records use UPPERCASE column keys to match Snowflake
convention and maintain compatibility with SemanticViewBuilder.
"""

import json
from typing import Any, Dict, List, Optional

from snowflake_semantic_tools.core.metadata.store import MetadataStore
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


def _upper_keys(record: Dict[str, Any]) -> Dict[str, Any]:
    return {k.upper(): v for k, v in record.items()}


def _index_by_table(records: List[Dict]) -> Dict[str, List[Dict]]:
    index: Dict[str, List[Dict]] = {}
    for r in records:
        rec = _upper_keys(r)
        table = (rec.get("TABLE_NAME") or "").lower()
        if table not in index:
            index[table] = []
        without_table = {k: v for k, v in rec.items() if k != "TABLE_NAME"}
        index[table].append(without_table)
    return index


def _parse_json_field(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            pass
    return value


class InMemoryStore(MetadataStore):
    """Reads from compiled manifest dict. No Snowflake connection needed."""

    def __init__(self, tables_data: Dict[str, List[Dict]]):
        self._raw = tables_data

        expected_keys = {
            "tables",
            "dimensions",
            "time_dimensions",
            "facts",
            "metrics",
            "relationships",
            "relationship_columns",
            "filters",
            "verified_queries",
            "custom_instructions",
            "semantic_views",
        }
        actual_keys = set(tables_data.keys())
        missing = expected_keys - actual_keys
        if missing:
            logger.warning(f"Manifest tables section missing keys: {', '.join(sorted(missing))}")

        raw_tables = tables_data.get("tables", [])
        self._tables: Dict[str, Dict] = {}
        for t in raw_tables:
            rec = _upper_keys(t)
            name = (rec.get("TABLE_NAME") or "").lower()
            if name:
                without_name = {k: v for k, v in rec.items() if k != "TABLE_NAME"}
                self._tables[name] = without_name

        self._dimensions = _index_by_table(tables_data.get("dimensions", []))
        self._time_dimensions = _index_by_table(tables_data.get("time_dimensions", []))
        self._facts = _index_by_table(tables_data.get("facts", []))

        self._metrics_raw = [_upper_keys(m) for m in tables_data.get("metrics", [])]

        self._relationships_raw = [_upper_keys(r) for r in tables_data.get("relationships", [])]
        self._rel_columns: Dict[str, List[Dict]] = {}
        for rc in tables_data.get("relationship_columns", []):
            rec = _upper_keys(rc)
            rel_name = (rec.get("RELATIONSHIP_NAME") or "").lower()
            if rel_name not in self._rel_columns:
                self._rel_columns[rel_name] = []
            without_name = {k: v for k, v in rec.items() if k != "RELATIONSHIP_NAME"}
            self._rel_columns[rel_name].append(without_name)

        self._verified_queries_raw = [_upper_keys(v) for v in tables_data.get("verified_queries", [])]

        self._custom_instructions: Dict[str, Dict] = {}
        for ci in tables_data.get("custom_instructions", []):
            rec = _upper_keys(ci)
            name = (rec.get("NAME") or "").upper()
            if name:
                self._custom_instructions[name] = rec

        self._filters_raw = [_upper_keys(f) for f in tables_data.get("filters", [])]

        self._semantic_views_raw = [_upper_keys(sv) for sv in tables_data.get("semantic_views", [])]

    def get_semantic_views(self) -> List[Dict[str, Any]]:
        return self._semantic_views_raw

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        info = self._tables.get(table_name.lower())
        if info:
            return info
        logger.warning(f"No metadata found for table '{table_name}' in manifest")
        return {
            "TABLE_NAME": table_name.upper(),
            "DESCRIPTION": f"Table: {table_name}",
        }

    def get_dimensions(self, table_name: str) -> List[Dict[str, Any]]:
        return self._dimensions.get(table_name.lower(), [])

    def get_time_dimensions(self, table_name: str) -> List[Dict[str, Any]]:
        return self._time_dimensions.get(table_name.lower(), [])

    def get_facts(self, table_name: str) -> List[Dict[str, Any]]:
        return self._facts.get(table_name.lower(), [])

    def get_metrics(self, table_names: List[str]) -> List[Dict[str, Any]]:
        normalized = {t.lower() for t in table_names}
        result = []
        for metric in self._metrics_raw:
            tables_field = metric.get("TABLE_NAME", "")
            metric_tables = _parse_json_field(tables_field)
            if isinstance(metric_tables, str):
                metric_tables = [t.strip() for t in metric_tables.split(",") if t.strip()]
            if not isinstance(metric_tables, list):
                metric_tables = [str(metric_tables)]
            metric_table_names = {str(t).lower() for t in metric_tables}
            if metric_table_names and metric_table_names.issubset(normalized):
                result.append(metric)
        return result

    def get_relationships(self, table_names: List[str]) -> List[Dict[str, Any]]:
        normalized = {t.lower() for t in table_names}
        return [
            dict(r)
            for r in self._relationships_raw
            if (r.get("LEFT_TABLE_NAME") or "").lower() in normalized
            and (r.get("RIGHT_TABLE_NAME") or "").lower() in normalized
        ]

    def get_relationship_columns(self, relationship_name: str) -> List[Dict[str, Any]]:
        return self._rel_columns.get(relationship_name.lower(), [])

    def get_verified_queries(self, table_names: List[str]) -> List[Dict[str, Any]]:
        normalized = {t.lower() for t in table_names}
        result = []
        for vq in self._verified_queries_raw:
            tables_field = vq.get("TABLES", "")
            vq_tables = _parse_json_field(tables_field)
            if isinstance(vq_tables, str):
                vq_tables = [t.strip() for t in vq_tables.split(",") if t.strip()]
            if not isinstance(vq_tables, list):
                vq_tables = [str(vq_tables)]
            vq_table_names = {str(t).lower() for t in vq_tables}
            if vq_table_names and vq_table_names.issubset(normalized):
                result.append(vq)
        return result

    def get_custom_instructions(self, names: List[str]) -> List[Dict[str, Any]]:
        result = []
        for name in names:
            ci = self._custom_instructions.get(name.upper())
            if ci:
                result.append(ci)
        return result

    def get_filters(self, table_names: List[str]) -> List[Dict[str, Any]]:
        normalized = {t.upper() for t in table_names}
        return [f for f in self._filters_raw if (f.get("TABLE_NAME") or "").upper() in normalized]
