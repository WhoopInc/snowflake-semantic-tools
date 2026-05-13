"""
List Semantic Components Service

Provides introspection of semantic model components from parsed YAML files.
Enables users to discover metrics, relationships, filters, semantic views,
custom instructions, verified queries, and tables without requiring a
Snowflake connection or compiled manifest.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from snowflake_semantic_tools.core.parsing import Parser
from snowflake_semantic_tools.core.parsing.parser import ParsingCriticalError
from snowflake_semantic_tools.services.compile import MANIFEST_FILENAME
from snowflake_semantic_tools.shared.utils import get_logger
from snowflake_semantic_tools.shared.utils.file_utils import find_dbt_model_files, find_semantic_model_files

logger = get_logger("list_semantic_components")


def _safe_str(value: Any) -> str:
    """Safely convert a value to string, handling None."""
    if value is None:
        return ""
    return str(value)


def _parse_tables_json(tables_field: Any) -> List[str]:
    """Parse a tables field that may be a JSON string or a list."""
    if isinstance(tables_field, list):
        return tables_field
    if isinstance(tables_field, str):
        try:
            parsed = json.loads(tables_field)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []


@dataclass
class ListConfig:
    """Configuration for listing semantic components."""

    dbt_path: Optional[Path] = None
    semantic_path: Optional[Path] = None
    exclude_dirs: Optional[List[str]] = None
    table_filter: Optional[str] = None
    no_manifest: bool = False


@dataclass
class ListResult:
    """Results from listing semantic model components."""

    tables: List[Dict[str, Any]] = field(default_factory=list)
    metrics: List[Dict[str, Any]] = field(default_factory=list)
    relationships: List[Dict[str, Any]] = field(default_factory=list)
    filters: List[Dict[str, Any]] = field(default_factory=list)
    semantic_views: List[Dict[str, Any]] = field(default_factory=list)
    custom_instructions: List[Dict[str, Any]] = field(default_factory=list)
    verified_queries: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def total_count(self) -> int:
        return (
            len(self.tables)
            + len(self.metrics)
            + len(self.relationships)
            + len(self.filters)
            + len(self.semantic_views)
            + len(self.custom_instructions)
            + len(self.verified_queries)
        )


class SemanticComponentListService:
    """
    Service for listing semantic model components.

    Parses dbt and semantic model YAML files and returns structured
    component inventories. No Snowflake connection required.
    """

    def __init__(self):
        self.parser = Parser(enable_template_resolution=True)

    def execute(self, config: ListConfig) -> ListResult:
        """
        Execute the list workflow.

        Reads from sst_manifest.json when available (fast, consistent with generate).
        Falls back to YAML parsing if no manifest exists or --no-manifest is set.

        Args:
            config: List configuration with paths and filters

        Returns:
            ListResult with all discovered components
        """
        if not config.no_manifest and not config.dbt_path and not config.semantic_path:
            manifest_result = self._load_from_manifest(config)
            if manifest_result is not None:
                return manifest_result
            logger.debug("No manifest found — falling back to YAML parsing")

        result = ListResult()

        if config.exclude_dirs is None:
            config = ListConfig(
                dbt_path=config.dbt_path,
                semantic_path=config.semantic_path,
                exclude_dirs=self._get_config_exclusions(),
                table_filter=config.table_filter,
            )

        dbt_files = self._find_dbt_files(config)
        semantic_files = self._find_semantic_files(config)

        if not dbt_files and not semantic_files:
            result.errors.append("No dbt or semantic model files found in project.")
            return result

        self._try_load_manifest()

        try:
            parse_result = self.parser.parse_all_files(dbt_files, semantic_files)
        except ParsingCriticalError as e:
            logger.debug(f"Parsing completed with errors (partial results available): {e}")
            parse_result = {
                "dbt": self._safe_parse_dbt(dbt_files),
                "semantic": self._safe_parse_semantic(semantic_files),
            }
            result.errors.extend(e.errors)

        self._extract_tables(parse_result, result, config)
        self._extract_metrics(parse_result, result, config)
        self._extract_relationships(parse_result, result, config)
        self._extract_filters(parse_result, result, config)
        self._extract_semantic_views(parse_result, result, config)
        self._extract_custom_instructions(parse_result, result, config)
        self._extract_verified_queries(parse_result, result, config)

        return result

    def _get_config_exclusions(self) -> Optional[List[str]]:
        """Load exclusion patterns from sst_config.yml if available."""
        try:
            from snowflake_semantic_tools.shared.config import get_config

            config = get_config()
            exclude_dirs = config.get("validation.exclude_dirs", [])
            return exclude_dirs if exclude_dirs else None
        except Exception:
            return None

    def _find_dbt_files(self, config: ListConfig) -> List[Path]:
        if config.dbt_path:
            dbt_path = config.dbt_path if config.dbt_path.is_absolute() else Path.cwd() / config.dbt_path
            from snowflake_semantic_tools.shared.utils.file_utils import _is_dbt_model_file

            all_yml = list(dbt_path.rglob("*.yml")) + list(dbt_path.rglob("*.yaml"))
            return [f for f in all_yml if _is_dbt_model_file(f)]
        return find_dbt_model_files(exclude_dirs=config.exclude_dirs)

    def _find_semantic_files(self, config: ListConfig) -> List[Path]:
        if config.semantic_path:
            sem_path = config.semantic_path if config.semantic_path.is_absolute() else Path.cwd() / config.semantic_path
            return list(sem_path.rglob("*.yml")) + list(sem_path.rglob("*.yaml"))
        try:
            return find_semantic_model_files()
        except Exception:
            return []

    def _try_load_manifest(self):
        try:
            from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

            manifest_parser = ManifestParser()
            if manifest_parser.load():
                self.parser.manifest_parser = manifest_parser
                logger.debug("Loaded manifest for template resolution")
        except ImportError as e:
            logger.warning(f"Could not import manifest parser: {e}")
        except Exception as e:
            logger.warning(f"Could not load manifest (template resolution may be limited): {e}")

    def _safe_parse_dbt(self, dbt_files: List[Path]) -> Dict[str, Any]:
        try:
            fresh_parser = Parser(enable_template_resolution=False)
            fresh_parser._build_dbt_catalog(dbt_files)
            return {"models": list(fresh_parser.dbt_catalog.values()), "sm_tables": []}
        except Exception:
            return {"models": [], "sm_tables": []}

    def _safe_parse_semantic(self, semantic_files: List[Path]) -> Dict[str, Any]:
        return {}

    def _extract_tables(self, parse_result: Dict[str, Any], result: ListResult, config: ListConfig):
        dbt_data = parse_result.get("dbt", {})
        tables = dbt_data.get("sm_tables", [])
        for table in tables:
            if not isinstance(table, dict):
                continue
            table_name = _safe_str(table.get("table_name"))
            if config.table_filter and config.table_filter.upper() not in table_name.upper():
                continue
            result.tables.append(table)

    def _extract_metrics(self, parse_result: Dict[str, Any], result: ListResult, config: ListConfig):
        semantic_data = parse_result.get("semantic", {})
        metrics_data = semantic_data.get("metrics", {})
        items = metrics_data.get("items", []) if isinstance(metrics_data, dict) else []
        for metric in items:
            if not isinstance(metric, dict):
                continue
            if config.table_filter:
                tables = metric.get("tables") or []
                table_name = _safe_str(metric.get("table_name"))
                filter_upper = config.table_filter.upper()
                if filter_upper not in table_name.upper() and not any(
                    filter_upper in _safe_str(t).upper() for t in tables
                ):
                    continue
            result.metrics.append(metric)

    def _extract_relationships(self, parse_result: Dict[str, Any], result: ListResult, config: ListConfig):
        semantic_data = parse_result.get("semantic", {})
        rel_data = semantic_data.get("relationships", {})
        items = rel_data.get("items", []) if isinstance(rel_data, dict) else []
        for rel in items:
            if not isinstance(rel, dict):
                continue
            if config.table_filter:
                filter_upper = config.table_filter.upper()
                left = _safe_str(rel.get("left_table_name"))
                right = _safe_str(rel.get("right_table_name"))
                if filter_upper not in left.upper() and filter_upper not in right.upper():
                    continue
            result.relationships.append(rel)

    def _extract_filters(self, parse_result: Dict[str, Any], result: ListResult, config: ListConfig):
        semantic_data = parse_result.get("semantic", {})
        filters_data = semantic_data.get("filters", {})
        items = filters_data.get("items", []) if isinstance(filters_data, dict) else []
        for f in items:
            if not isinstance(f, dict):
                continue
            table_name = _safe_str(f.get("table_name"))
            if config.table_filter and config.table_filter.upper() not in table_name.upper():
                continue
            result.filters.append(f)

    def _extract_semantic_views(self, parse_result: Dict[str, Any], result: ListResult, config: ListConfig):
        semantic_data = parse_result.get("semantic", {})
        views_data = semantic_data.get("semantic_views", {})
        items = views_data.get("items", []) if isinstance(views_data, dict) else []
        for view in items:
            if not isinstance(view, dict):
                continue
            if config.table_filter:
                tables = _parse_tables_json(view.get("tables", "[]"))
                if not any(config.table_filter.upper() in _safe_str(t).upper() for t in tables):
                    continue
            result.semantic_views.append(view)

    def _extract_custom_instructions(self, parse_result: Dict[str, Any], result: ListResult, config: ListConfig):
        semantic_data = parse_result.get("semantic", {})
        ci_data = semantic_data.get("custom_instructions", {})
        items = ci_data.get("items", []) if isinstance(ci_data, dict) else []
        for ci in items:
            if isinstance(ci, dict):
                result.custom_instructions.append(ci)

    def _extract_verified_queries(self, parse_result: Dict[str, Any], result: ListResult, config: ListConfig):
        semantic_data = parse_result.get("semantic", {})
        vq_data = semantic_data.get("verified_queries", {})
        items = vq_data.get("items", []) if isinstance(vq_data, dict) else []
        for vq in items:
            if isinstance(vq, dict):
                result.verified_queries.append(vq)

    def _load_from_manifest(self, config: ListConfig) -> Optional[ListResult]:
        manifest_path = Path("target") / MANIFEST_FILENAME
        if not manifest_path.exists():
            return None

        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not load manifest: {e}")
            return None

        tables_data = data.get("tables", {})
        if not tables_data:
            return None

        result = ListResult()
        result.tables = tables_data.get("tables", [])
        result.metrics = tables_data.get("metrics", [])
        result.relationships = tables_data.get("relationships", [])
        result.filters = tables_data.get("filters", [])
        result.semantic_views = tables_data.get("semantic_views", [])
        result.custom_instructions = tables_data.get("custom_instructions", [])
        result.verified_queries = tables_data.get("verified_queries", [])

        if config.table_filter:
            self._apply_table_filter(result, config.table_filter)

        return result

    @staticmethod
    def _apply_table_filter(result: ListResult, table_filter: str):
        f = table_filter.upper()

        result.tables = [t for t in result.tables if f in _safe_str(t.get("table_name")).upper()]

        result.metrics = [
            m
            for m in result.metrics
            if f in _safe_str(m.get("table_name")).upper()
            or any(f in _safe_str(t).upper() for t in (m.get("tables") or []))
        ]

        result.relationships = [
            r
            for r in result.relationships
            if f in _safe_str(r.get("left_table_name")).upper() or f in _safe_str(r.get("right_table_name")).upper()
        ]

        result.filters = [fl for fl in result.filters if f in _safe_str(fl.get("table_name")).upper()]

        result.semantic_views = [
            v
            for v in result.semantic_views
            if any(f in _safe_str(t).upper() for t in _parse_tables_json(v.get("tables", [])))
        ]
