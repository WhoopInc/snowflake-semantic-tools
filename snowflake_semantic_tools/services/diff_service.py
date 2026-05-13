"""
Diff Service

Compares proposed semantic view DDL (from manifest) against currently
deployed views in Snowflake, showing what would change before deploying.
"""

import difflib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeConfig
from snowflake_semantic_tools.services.compile import MANIFEST_FILENAME
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("diff_service")

_KW_PATTERN = re.compile(
    r"\b(CREATE|OR|REPLACE|SEMANTIC|VIEW|TABLES|RELATIONSHIPS|FACTS|DIMENSIONS|METRICS|"
    r"PRIMARY|KEY|UNIQUE|COMMENT|WITH|SYNONYMS|AS|REFERENCES|OVER|PARTITION|BY|ORDER|"
    r"AI_VERIFIED_QUERIES|AI_SQL_GENERATION|AI_QUESTION_CATEGORIZATION|EXTENSION)\b",
    re.IGNORECASE,
)


@dataclass
class DiffConfig:
    database: str
    schema: str
    views_filter: Optional[List[str]] = None


@dataclass
class ViewDiff:
    name: str
    status: str
    proposed_sql: Optional[str] = None
    existing_sql: Optional[str] = None
    unified_diff: Optional[str] = None


@dataclass
class DiffResult:
    success: bool = True
    new: List[ViewDiff] = field(default_factory=list)
    modified: List[ViewDiff] = field(default_factory=list)
    unchanged: List[ViewDiff] = field(default_factory=list)
    extra_deployed: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(self.new or self.modified or self.extra_deployed)


class DiffService:
    def __init__(self, snowflake_config: SnowflakeConfig):
        self.snowflake_config = snowflake_config

    def diff(self, config: DiffConfig) -> DiffResult:
        result = DiffResult()

        proposed = self._get_proposed_ddl(config, result)
        if not result.success:
            return result

        deployed_names, existing_ddl = self._get_existing_ddl(config, result, proposed_names=set(proposed.keys()))
        if not result.success:
            return result

        if not proposed and not deployed_names:
            result.errors.append(
                "SST-D003: No semantic views found in manifest or Snowflake. "
                "Define semantic views in YAML and run 'sst compile'."
            )
            result.success = False
            return result

        proposed_upper = {k.upper(): v for k, v in proposed.items()}
        deployed_upper = {n.upper() for n in deployed_names}
        existing_upper = {k.upper(): v for k, v in existing_ddl.items()}

        proposed_names_set = set(proposed_upper.keys())

        if config.views_filter:
            filter_set = {v.upper() for v in config.views_filter}
            proposed_names_set = proposed_names_set & filter_set
            deployed_upper = deployed_upper & filter_set

        for name in sorted(proposed_names_set):
            p_sql = proposed_upper[name]
            if name not in deployed_upper:
                result.new.append(ViewDiff(name=name, status="new", proposed_sql=p_sql))
            elif name in existing_upper:
                e_sql = existing_upper[name]
                if self._normalize_ddl(p_sql) == self._normalize_ddl(e_sql):
                    result.unchanged.append(ViewDiff(name=name, status="unchanged"))
                else:
                    diff_text = self._compute_diff(e_sql, p_sql, name)
                    result.modified.append(
                        ViewDiff(
                            name=name,
                            status="modified",
                            proposed_sql=p_sql,
                            existing_sql=e_sql,
                            unified_diff=diff_text,
                        )
                    )

        for name in sorted(deployed_upper - proposed_names_set):
            result.extra_deployed.append(name)
            result.warnings.append(f"View '{name}' exists in Snowflake but not in manifest")

        return result

    def _get_proposed_ddl(self, config: DiffConfig, result: DiffResult) -> Dict[str, str]:
        manifest_path = Path("target") / MANIFEST_FILENAME
        if not manifest_path.exists():
            result.errors.append("SST-D002: sst_manifest.json not found. Run 'sst compile' first.")
            result.success = False
            return {}

        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest_data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            result.errors.append(f"SST-D002: Could not load manifest: {e}")
            result.success = False
            return {}

        tables_data = manifest_data.get("tables", {})
        if not tables_data:
            return {}

        try:
            from snowflake_semantic_tools.core.metadata.in_memory_store import InMemoryStore
            from snowflake_semantic_tools.services.generate_semantic_views import (
                SemanticViewGenerationService,
                UnifiedGenerationConfig,
            )

            service = SemanticViewGenerationService(self.snowflake_config)
            try:
                gen_config = UnifiedGenerationConfig(
                    metadata_database=config.database,
                    metadata_schema=config.schema,
                    target_database=config.database,
                    target_schema=config.schema,
                    dry_run=True,
                )

                gen_result = service.generate(gen_config)
                return gen_result.sql_statements or {}
            finally:
                service.close()
        except Exception as e:
            result.errors.append(f"SST-D002: Failed to generate proposed DDL: {e}")
            result.success = False
            return {}

    def _get_existing_ddl(self, config: DiffConfig, result: DiffResult, proposed_names: Optional[set] = None) -> tuple:
        try:
            from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeClient

            client = SnowflakeClient(self.snowflake_config)
            fq_schema = f"{config.database}.{config.schema}"

            show_sql = f"SHOW SEMANTIC VIEWS IN {fq_schema}"
            df = client.execute_query(show_sql)

            if df.empty:
                return set(), {}

            name_col = "name" if "name" in df.columns else "NAME"
            all_names = set(df[name_col].tolist())

            proposed_upper = {n.upper() for n in (proposed_names or set())}
            views_to_fetch = [n for n in all_names if n.upper() in proposed_upper]

            existing_ddl: Dict[str, str] = {}
            for name in views_to_fetch:
                fq_name = f"{config.database}.{config.schema}.{name}"
                try:
                    ddl_sql = f"SELECT GET_DDL('SEMANTIC_VIEW', '{fq_name}')"
                    ddl_df = client.execute_query(ddl_sql)
                    if not ddl_df.empty:
                        ddl = str(ddl_df.iloc[0, 0])
                        existing_ddl[name.upper()] = ddl
                except Exception as e:
                    result.warnings.append(f"SST-D004: Could not retrieve DDL for '{name}': {e}")

            return all_names, existing_ddl

        except Exception as e:
            result.errors.append(f"SST-D001: Could not connect to Snowflake: {e}")
            result.success = False
            return set(), {}

    @staticmethod
    def _normalize_ddl(sql: str) -> str:
        lines = sql.strip().rstrip(";").splitlines()
        normalized = []
        for line in lines:
            stripped = line.rstrip()
            if stripped:
                normalized.append(stripped)
        text = "\n".join(normalized)
        text = _KW_PATTERN.sub(lambda m: m.group(0).upper(), text)
        return text

    @staticmethod
    def _compute_diff(existing: str, proposed: str, view_name: str) -> str:
        existing_lines = existing.strip().splitlines()
        proposed_lines = proposed.strip().splitlines()
        diff = difflib.unified_diff(
            existing_lines,
            proposed_lines,
            fromfile=f"{view_name} (deployed)",
            tofile=f"{view_name} (proposed)",
            lineterm="",
        )
        return "\n".join(diff)
