"""
Diff Service

Compares proposed semantic view components (from manifest) against
currently deployed views in Snowflake using DESCRIBE SEMANTIC VIEW.
Shows only what changed: new, removed, and modified components.
"""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeConfig
from snowflake_semantic_tools.services.compile import MANIFEST_FILENAME
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("diff_service")

COMPONENT_TYPES = ("TABLE", "METRIC", "DIMENSION", "FACT", "RELATIONSHIP", "AI_VERIFIED_QUERY", "CUSTOM_INSTRUCTION")


@dataclass
class DiffConfig:
    database: str
    schema: str
    views_filter: Optional[List[str]] = None
    full: bool = False


@dataclass
class ComponentChange:
    kind: str
    name: str
    table: str
    status: str
    detail: Optional[str] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None


@dataclass
class ViewDiff:
    name: str
    status: str
    changes: List[ComponentChange] = field(default_factory=list)
    proposed_counts: Dict[str, int] = field(default_factory=dict)

    @property
    def has_changes(self) -> bool:
        return self.status != "unchanged"


@dataclass
class DiffResult:
    success: bool = True
    views: List[ViewDiff] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def changed_count(self) -> int:
        return sum(1 for v in self.views if v.has_changes)

    @property
    def unchanged_count(self) -> int:
        return sum(1 for v in self.views if not v.has_changes)


_WINDOW_TAIL_RE = re.compile(r"\s+OVER\s*\(", re.IGNORECASE)


def _safe_str(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val)


def _normalize_expr(expr: str) -> str:
    return " ".join(expr.upper().split())


def _is_window_extension(base: str, full: str) -> bool:
    if not base or not full or len(full) <= len(base):
        return False
    if not full.startswith(base):
        return False
    remainder = full[len(base) :]
    return bool(_WINDOW_TAIL_RE.match(remainder))


class DiffService:
    def __init__(self, snowflake_config: SnowflakeConfig):
        self.snowflake_config = snowflake_config

    def diff(self, config: DiffConfig) -> DiffResult:
        result = DiffResult()

        proposed_views = self._load_proposed(result)
        if not result.success:
            return result

        from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeClient

        try:
            client = SnowflakeClient(self.snowflake_config)
        except Exception as e:
            result.errors.append(f"SST-D001: Could not connect to Snowflake: {e}")
            result.success = False
            return result

        deployed_names = self._get_deployed_view_names(config, client, result)
        if not result.success:
            return result

        if not proposed_views and not deployed_names:
            result.errors.append("SST-D003: No semantic views found in manifest or Snowflake.")
            result.success = False
            return result

        proposed_upper = {k.upper(): v for k, v in proposed_views.items()}

        target_views = set(proposed_upper.keys())
        if config.views_filter:
            target_views = target_views & {v.upper() for v in config.views_filter}

        deployed_upper = {n.upper() for n in deployed_names}

        for name in sorted(target_views):
            proposed = proposed_upper[name]

            if name not in deployed_upper:
                counts = {}
                for kind, components in proposed.items():
                    counts[kind] = len(components)
                result.views.append(ViewDiff(name=name, status="new", proposed_counts=counts))
                continue

            deployed = self._describe_view(config, client, name, result)
            if deployed is None:
                continue

            changes = self._compare_components(proposed, deployed)
            status = "changed" if changes else "unchanged"
            result.views.append(ViewDiff(name=name, status=status, changes=changes))

        return result

    def _load_proposed(self, result: DiffResult) -> Dict[str, Dict[str, Dict[str, Dict]]]:
        manifest_path = Path("target") / MANIFEST_FILENAME
        if not manifest_path.exists():
            result.errors.append("SST-D002: sst_manifest.json not found. Run 'sst compile' first.")
            result.success = False
            return {}

        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            result.errors.append(f"SST-D002: Could not load manifest: {e}")
            result.success = False
            return {}

        tables_data = manifest.get("tables", {})

        ci_lookup = {}
        for ci in tables_data.get("custom_instructions", []):
            ci_name = (ci.get("name") or "").upper()
            ci_lookup[ci_name] = ci

        views = {}
        for sv in tables_data.get("semantic_views", []):
            view_name = (sv.get("name") or "").upper()
            if not view_name:
                continue

            sv_tables = sv.get("tables", [])
            if isinstance(sv_tables, str):
                try:
                    sv_tables = json.loads(sv_tables)
                except (json.JSONDecodeError, TypeError):
                    sv_tables = []
            table_set = {t.upper() for t in sv_tables}

            components: Dict[str, Dict[str, Dict]] = {}

            for t in tables_data.get("tables", []):
                tn = (t.get("table_name") or "").upper()
                if tn in table_set:
                    components.setdefault("TABLE", {})[tn] = {}

            for d in tables_data.get("dimensions", []):
                tn = (d.get("table_name") or "").upper()
                if tn in table_set:
                    name = (d.get("name") or "").upper()
                    components.setdefault("DIMENSION", {})[f"{tn}.{name}"] = {
                        "EXPRESSION": d.get("expr", ""),
                        "TABLE": tn,
                    }

            for td in tables_data.get("time_dimensions", []):
                tn = (td.get("table_name") or "").upper()
                if tn in table_set:
                    name = (td.get("name") or "").upper()
                    components.setdefault("DIMENSION", {})[f"{tn}.{name}"] = {
                        "EXPRESSION": td.get("expr", ""),
                        "TABLE": tn,
                    }

            for fact in tables_data.get("facts", []):
                tn = (fact.get("table_name") or "").upper()
                if tn in table_set:
                    name = (fact.get("name") or "").upper()
                    components.setdefault("FACT", {})[f"{tn}.{name}"] = {
                        "EXPRESSION": fact.get("expr", ""),
                        "TABLE": tn,
                    }

            for m in tables_data.get("metrics", []):
                mt = m.get("tables", m.get("table_name", ""))
                if isinstance(mt, list):
                    metric_tables = {t.upper() for t in mt}
                elif isinstance(mt, str):
                    metric_tables = {mt.upper()}
                else:
                    metric_tables = set()
                if metric_tables and metric_tables.issubset(table_set):
                    name = (m.get("name") or "").upper()
                    parent = (m.get("table_name") or "").upper()
                    components.setdefault("METRIC", {})[name] = {
                        "EXPRESSION": m.get("expr", ""),
                        "TABLE": parent,
                    }

            for r in tables_data.get("relationships", []):
                left = (r.get("left_table_name") or "").upper()
                right = (r.get("right_table_name") or "").upper()
                if left in table_set and right in table_set:
                    rname = (r.get("relationship_name") or "").upper()
                    components.setdefault("RELATIONSHIP", {})[rname] = {
                        "LEFT": left,
                        "RIGHT": right,
                    }

            for vq in tables_data.get("verified_queries", []):
                vq_tables = vq.get("tables", [])
                if isinstance(vq_tables, list):
                    vq_table_set = {t.upper() for t in vq_tables}
                else:
                    vq_table_set = set()
                if vq_table_set and vq_table_set.issubset(table_set):
                    name = (vq.get("name") or "").upper()
                    components.setdefault("AI_VERIFIED_QUERY", {})[name] = {
                        "QUESTION": vq.get("question", ""),
                    }

            sv_ci_names = sv.get("custom_instructions", [])
            if isinstance(sv_ci_names, str):
                try:
                    sv_ci_names = json.loads(sv_ci_names)
                except (json.JSONDecodeError, TypeError):
                    sv_ci_names = []
            sql_parts = []
            qcat_parts = []
            for ci_name in sv_ci_names:
                ci = ci_lookup.get(ci_name.upper(), {})
                sql_gen = (ci.get("sql_generation") or "").strip()
                q_cat = (ci.get("question_categorization") or "").strip()
                if sql_gen:
                    sql_parts.append(sql_gen)
                if q_cat:
                    qcat_parts.append(q_cat)
            if sql_parts:
                components.setdefault("CUSTOM_INSTRUCTION", {})["AI_SQL_GENERATION"] = {
                    "VALUE": "\n".join(sql_parts),
                }
            if qcat_parts:
                components.setdefault("CUSTOM_INSTRUCTION", {})["AI_QUESTION_CATEGORIZATION"] = {
                    "VALUE": "\n".join(qcat_parts),
                }

            views[view_name] = components

        return views

    def _get_deployed_view_names(self, config: DiffConfig, client, result: DiffResult) -> Set[str]:
        try:
            df = client.execute_query(f"SHOW SEMANTIC VIEWS IN {config.database}.{config.schema}")
            if df.empty:
                return set()
            name_col = "name" if "name" in df.columns else "NAME"
            return set(df[name_col].tolist())
        except Exception as e:
            result.errors.append(f"SST-D001: Could not connect to Snowflake: {e}")
            result.success = False
            return set()

    def _describe_view(
        self, config: DiffConfig, client, view_name: str, result: DiffResult
    ) -> Optional[Dict[str, Dict[str, Dict]]]:
        try:
            fq = f"{config.database}.{config.schema}.{view_name}"
            df = client.execute_query(f"DESCRIBE SEMANTIC VIEW {fq}")

            components: Dict[str, Dict[str, Dict]] = {}
            for _, row in df.iterrows():
                kind = _safe_str(row.get("object_kind"))
                obj_name = _safe_str(row.get("object_name"))
                parent = _safe_str(row.get("parent_entity"))
                prop = _safe_str(row.get("property"))
                value = _safe_str(row.get("property_value"))

                if kind not in COMPONENT_TYPES:
                    continue

                if kind == "METRIC":
                    key = obj_name
                elif kind in ("DIMENSION", "FACT"):
                    key = f"{parent}.{obj_name}"
                elif kind == "RELATIONSHIP":
                    key = obj_name
                elif kind == "AI_VERIFIED_QUERY":
                    key = obj_name
                elif kind == "TABLE":
                    key = obj_name
                elif kind == "CUSTOM_INSTRUCTION":
                    key = prop
                else:
                    continue

                if kind != "CUSTOM_INSTRUCTION" and (not obj_name or obj_name.startswith("_")):
                    continue

                entry = components.setdefault(kind, {}).setdefault(key, {"TABLE": parent})
                entry[prop] = value

            return components
        except Exception as e:
            result.warnings.append(f"SST-D004: Could not describe '{view_name}': {e}")
            return None

    @staticmethod
    def _compare_components(
        proposed: Dict[str, Dict[str, Dict]], deployed: Dict[str, Dict[str, Dict]]
    ) -> List[ComponentChange]:
        changes = []

        all_kinds = set(proposed.keys()) | set(deployed.keys())

        for kind in sorted(all_kinds):
            p_items = proposed.get(kind, {})
            d_items = deployed.get(kind, {})

            p_names = set(p_items.keys())
            d_names = set(d_items.keys())

            for name in sorted(p_names - d_names):
                info = p_items[name]
                table = info.get("TABLE", "")
                expr = info.get("EXPRESSION", info.get("QUESTION", ""))
                changes.append(
                    ComponentChange(
                        kind=kind,
                        name=name,
                        table=table,
                        status="new",
                        detail=expr[:80] if expr else None,
                    )
                )

            for name in sorted(d_names - p_names):
                info = d_items[name]
                table = info.get("TABLE", "")
                changes.append(ComponentChange(kind=kind, name=name, table=table, status="removed"))

            for name in sorted(p_names & d_names):
                p_info = p_items[name]
                d_info = d_items[name]

                if kind in ("METRIC", "DIMENSION", "FACT"):
                    p_expr = _normalize_expr((p_info.get("EXPRESSION") or "").strip())
                    d_expr = _normalize_expr((d_info.get("EXPRESSION") or "").strip())
                    if kind == "METRIC" and _is_window_extension(p_expr, d_expr):
                        continue
                    if p_expr != d_expr:
                        changes.append(
                            ComponentChange(
                                kind=kind,
                                name=name,
                                table=p_info.get("TABLE", ""),
                                status="modified",
                                detail="expression changed",
                                old_value=d_expr,
                                new_value=p_expr,
                            )
                        )
                elif kind == "RELATIONSHIP":
                    p_ref = p_info.get("RIGHT", "")
                    d_ref = d_info.get("REF_TABLE", "")
                    if p_ref.upper() != d_ref.upper():
                        changes.append(
                            ComponentChange(
                                kind=kind,
                                name=name,
                                table="",
                                status="modified",
                                detail="reference changed",
                                old_value=d_ref,
                                new_value=p_ref,
                            )
                        )
                elif kind == "AI_VERIFIED_QUERY":
                    p_q = (p_info.get("QUESTION") or "").strip()
                    d_q = (d_info.get("QUESTION") or "").strip()
                    if p_q != d_q:
                        changes.append(
                            ComponentChange(
                                kind=kind,
                                name=name,
                                table="",
                                status="modified",
                                detail="question changed",
                                old_value=d_q,
                                new_value=p_q,
                            )
                        )
                elif kind == "CUSTOM_INSTRUCTION":
                    p_val = (p_info.get("VALUE") or "").strip()
                    d_val = (d_info.get(name) or "").strip()
                    if p_val != d_val:
                        changes.append(
                            ComponentChange(
                                kind=kind,
                                name=name,
                                table="",
                                status="modified",
                                detail="instruction changed",
                            )
                        )

        return changes
