"""
Compile Service

Parses all SST YAML and dbt manifest files, resolves templates, and writes
a compiled manifest to target/sst_manifest.json. This manifest contains all
metadata needed for validate and generate commands without a Snowflake
connection.
"""

import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml as pyyaml

from snowflake_semantic_tools.core.parsing import Parser
from snowflake_semantic_tools.core.parsing.file_detector import FileTypeDetector
from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser
from snowflake_semantic_tools.shared.config import get_config
from snowflake_semantic_tools.shared.utils import get_logger
from snowflake_semantic_tools.shared.utils.file_utils import find_dbt_model_files, find_semantic_model_files

logger = get_logger(__name__)

MANIFEST_FILENAME = "sst_manifest.json"
MANIFEST_VERSION = 1

SM_PREFIX_MAP = {
    "sm_tables": "tables",
    "sm_dimensions": "dimensions",
    "sm_time_dimensions": "time_dimensions",
    "sm_facts": "facts",
    "sm_metrics": "metrics",
    "sm_semantic_views": "semantic_views",
    "sm_relationships": "relationships",
    "sm_relationship_columns": "relationship_columns",
    "sm_filters": "filters",
    "sm_verified_queries": "verified_queries",
    "sm_custom_instructions": "custom_instructions",
    "metrics": "metrics",
    "relationships": "relationships",
    "relationship_columns": "relationship_columns",
    "filters": "filters",
    "verified_queries": "verified_queries",
    "custom_instructions": "custom_instructions",
}


@dataclass
class CompileConfig:
    dbt_path: Optional[Path] = None
    semantic_path: Optional[Path] = None
    target_database: Optional[str] = None


@dataclass
class CompileResult:
    success: bool
    manifest_path: Optional[Path] = None
    tables_count: int = 0
    metrics_count: int = 0
    views_count: int = 0
    files_tracked: int = 0
    errors: List[str] = field(default_factory=list)
    duration: float = 0.0


def _manifest_json_serializer(obj):
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class CompileService:
    def __init__(self):
        self.parser = Parser()

    def compile(self, config: CompileConfig) -> CompileResult:
        start = time.time()
        errors: List[str] = []

        manifest_parser = ManifestParser()
        if manifest_parser.load():
            self.parser.manifest_parser = manifest_parser
            logger.info(f"Loaded dbt manifest: {manifest_parser.manifest_path}")
        else:
            logger.info("No dbt manifest found — database/schema resolution may be incomplete")

        if config.target_database:
            self.parser.target_database = config.target_database

        try:
            dbt_files = (
                list(config.dbt_path.rglob("*.yml")) + list(config.dbt_path.rglob("*.yaml"))
                if config.dbt_path
                else find_dbt_model_files()
            )
        except Exception as e:
            errors.append(f"SST-C005: Could not find dbt model files: {e}")
            return CompileResult(success=False, errors=errors, duration=time.time() - start)

        try:
            semantic_files = (
                list(config.semantic_path.rglob("*.yml")) + list(config.semantic_path.rglob("*.yaml"))
                if config.semantic_path
                else find_semantic_model_files()
            )
        except Exception as e:
            semantic_files = []
            logger.debug(f"No semantic model files found: {e}")

        try:
            parse_result = self.parser.parse_all_files(dbt_files, semantic_files)
        except Exception as e:
            errors.append(f"SST-C005: Parsing failed: {e}")
            return CompileResult(success=False, errors=errors, duration=time.time() - start)

        tables_data = self._prepare_tables_data(parse_result)

        file_checksums = self._build_file_checksums(dbt_files, semantic_files)

        manifest = self._build_manifest(tables_data, file_checksums, manifest_parser)

        target_dir = Path("target")
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = target_dir / MANIFEST_FILENAME
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2, default=_manifest_json_serializer)
            logger.info(f"SST manifest written: {manifest_path}")
        except OSError as e:
            errors.append(f"SST-C006: Could not write manifest: {e}")
            return CompileResult(success=False, errors=errors, duration=time.time() - start)

        return CompileResult(
            success=True,
            manifest_path=manifest_path,
            tables_count=len(tables_data.get("tables", [])),
            metrics_count=len(tables_data.get("metrics", [])),
            views_count=len(tables_data.get("semantic_views", [])),
            files_tracked=len(file_checksums),
            duration=time.time() - start,
        )

    def _prepare_tables_data(self, parse_result: Dict[str, Any]) -> Dict[str, List]:
        raw = {}

        if "semantic" in parse_result:
            for model_type, model_data in parse_result["semantic"].items():
                if isinstance(model_data, dict):
                    if "items" in model_data:
                        key = f"sm_{model_type}" if model_type == "semantic_views" else model_type
                        raw[key] = model_data["items"]
                    if "relationship_columns" in model_data:
                        raw["relationship_columns"] = model_data["relationship_columns"]
                elif isinstance(model_data, list):
                    key = f"sm_{model_type}" if model_type == "semantic_views" else model_type
                    raw[key] = model_data

        if "dbt" in parse_result:
            dbt_data = parse_result["dbt"]
            if "sm_tables" in dbt_data:
                raw.setdefault("sm_tables", []).extend([t for t in dbt_data["sm_tables"] if t.get("table_name")])
            for key in [
                "sm_dimensions",
                "sm_time_dimensions",
                "sm_facts",
                "sm_metrics",
                "sm_relationships",
                "sm_relationship_columns",
                "sm_semantic_views",
                "sm_filters",
                "sm_verified_queries",
                "sm_custom_instructions",
            ]:
                if key in dbt_data and dbt_data[key]:
                    raw.setdefault(key, []).extend(dbt_data[key])

        project_dir = Path.cwd().resolve()
        clean: Dict[str, List] = {}
        for raw_key, records in raw.items():
            clean_key = SM_PREFIX_MAP.get(raw_key, raw_key.replace("sm_", ""))
            if clean_key in clean:
                clean[clean_key].extend(self._clean_records(records, project_dir))
            else:
                clean[clean_key] = self._clean_records(records, project_dir)

        referenced_tables = set()
        for sv in clean.get("semantic_views", []):
            sv_tables = sv.get("tables", [])
            if isinstance(sv_tables, list):
                referenced_tables.update(t.upper() for t in sv_tables)

        if referenced_tables:
            for section in ("tables", "dimensions", "time_dimensions", "facts"):
                if section in clean:
                    clean[section] = [
                        rec for rec in clean[section] if (rec.get("table_name") or "").upper() in referenced_tables
                    ]
            if "filters" in clean:
                clean["filters"] = [
                    rec
                    for rec in clean["filters"]
                    if (rec.get("table_name") or rec.get("TABLE_NAME") or "").upper() in referenced_tables
                ]
            ref_lower = {t.lower() for t in referenced_tables}
            if "metrics" in clean:
                pruned = []
                for m in clean["metrics"]:
                    mt = m.get("tables", m.get("table_name", ""))
                    if isinstance(mt, list):
                        names = {str(t).lower() for t in mt}
                    elif isinstance(mt, str):
                        names = {mt.lower()}
                    else:
                        names = set()
                    if not names or names.issubset(ref_lower):
                        pruned.append(m)
                clean["metrics"] = pruned
            if "relationships" in clean:
                clean["relationships"] = [
                    r
                    for r in clean["relationships"]
                    if (r.get("left_table_name") or "").upper() in referenced_tables
                    and (r.get("right_table_name") or "").upper() in referenced_tables
                ]
            if "relationship_columns" in clean:
                kept_rels = {(r.get("relationship_name") or "").upper() for r in clean.get("relationships", [])}
                clean["relationship_columns"] = [
                    rc
                    for rc in clean["relationship_columns"]
                    if (rc.get("relationship_name") or "").upper() in kept_rels
                ]
            if "verified_queries" in clean:
                pruned_vq = []
                for vq in clean["verified_queries"]:
                    vq_tables = vq.get("tables", [])
                    if isinstance(vq_tables, list):
                        names = {str(t).lower() for t in vq_tables}
                    else:
                        names = set()
                    if not names or names.issubset(ref_lower):
                        pruned_vq.append(vq)
                clean["verified_queries"] = pruned_vq

        return clean

    @staticmethod
    def _clean_records(records: List[Dict], project_dir: Optional[Path] = None) -> List[Dict]:
        cleaned = []
        for rec in records:
            clean = {}
            for k, v in rec.items():
                if k.startswith("_"):
                    continue
                if v is None or v == []:
                    continue
                if k == "source_file" and isinstance(v, str) and project_dir:
                    try:
                        clean[k] = str(Path(v).resolve().relative_to(project_dir))
                    except ValueError:
                        clean[k] = v
                    continue
                if k == "tables" and isinstance(v, str):
                    try:
                        parsed = json.loads(v)
                        if isinstance(parsed, list):
                            clean[k] = parsed
                            continue
                    except (json.JSONDecodeError, TypeError):
                        pass
                if k == "custom_instructions" and isinstance(v, str):
                    try:
                        parsed = json.loads(v)
                        if isinstance(parsed, list):
                            clean[k] = parsed
                            continue
                    except (json.JSONDecodeError, TypeError):
                        pass
                clean[k] = v
            cleaned.append(clean)
        return cleaned

    def _build_file_checksums(self, dbt_files: List[Path], semantic_files: List[Path]) -> Dict[str, Dict]:
        checksums: Dict[str, Dict] = {}
        project_dir = Path.cwd()

        config = get_config()
        view_table_map = self._parse_view_tables_for_checksums(config)
        table_to_views = {}
        for view_name, tables in view_table_map.items():
            for table in tables:
                tl = table.lower()
                if tl not in table_to_views:
                    table_to_views[tl] = []
                if view_name not in table_to_views[tl]:
                    table_to_views[tl].append(view_name)

        for f in dbt_files:
            try:
                raw = f.read_bytes()
                data = pyyaml.safe_load(raw)
                if not isinstance(data, dict) or "models" not in data:
                    continue
                model_names = []
                for m in data.get("models", []):
                    if isinstance(m, dict) and m.get("name"):
                        model_names.append(m["name"])
                views = set()
                for name in model_names:
                    matched = table_to_views.get(name.lower())
                    if matched:
                        views.update(matched)
                rel = str(f.resolve().relative_to(project_dir.resolve()))
                cs = f"sha256:{hashlib.sha256(raw).hexdigest()}"
                checksums[rel] = {
                    "checksum": cs,
                    "type": "dbt",
                    "views_impacted": sorted(views) if views else None,
                }
            except Exception:
                continue

        for f in semantic_files:
            try:
                raw = f.read_bytes()
                rel = str(f.resolve().relative_to(project_dir.resolve()))
                cs = f"sha256:{hashlib.sha256(raw).hexdigest()}"
                file_type = FileTypeDetector.detect_semantic_type(f) or "unknown"
                checksums[rel] = {
                    "checksum": cs,
                    "type": file_type,
                    "views_impacted": None,
                }
            except Exception:
                continue

        for name in ("sst_config.yml", "sst_config.yaml", ".sst_config.yml", ".sst_config.yaml"):
            cfg = project_dir / name
            if cfg.exists():
                try:
                    raw = cfg.read_bytes()
                    checksums["__config__"] = {
                        "checksum": f"sha256:{hashlib.sha256(raw).hexdigest()}",
                        "type": "config",
                    }
                except Exception:
                    pass
                break

        return checksums

    @staticmethod
    def _parse_view_tables_for_checksums(config) -> Dict[str, List[str]]:
        try:
            sem_dir_name = config.get("project.semantic_models_dir")
        except Exception:
            sem_dir_name = None
        if not sem_dir_name:
            try:
                project = config.get("project", {})
                if isinstance(project, dict):
                    sem_dir_name = project.get("semantic_models_dir")
            except Exception:
                pass
        if not sem_dir_name:
            return {}

        sem_dir = Path.cwd() / sem_dir_name
        if not sem_dir.exists():
            return {}

        view_map: Dict[str, List[str]] = {}
        ref_pattern = re.compile(r"\{\{\s*(?:ref|table)\(['\"]([^'\"]+)['\"]\)\s*\}\}")
        name_pattern = re.compile(r"^\s*-\s*name:\s*(.+)", re.MULTILINE)

        for yaml_file in sorted(list(sem_dir.rglob("*.yml")) + list(sem_dir.rglob("*.yaml"))):
            try:
                content = yaml_file.read_text(encoding="utf-8")
                if "semantic_views:" not in content:
                    continue
                try:
                    data = pyyaml.safe_load(content)
                except pyyaml.YAMLError:
                    data = None

                if data and isinstance(data, dict) and "semantic_views" in data:
                    for view_def in data["semantic_views"]:
                        if not isinstance(view_def, dict):
                            continue
                        view_name = view_def.get("name", "")
                        if not view_name:
                            continue
                        tables = view_def.get("tables", [])
                        if not isinstance(tables, list):
                            tables = []
                        resolved = []
                        for t in tables:
                            match = ref_pattern.search(str(t))
                            if match:
                                resolved.append(match.group(1))
                            else:
                                resolved.append(str(t).split(".")[-1].strip().lower())
                        view_map[view_name] = resolved
                else:
                    blocks = re.split(r"(?=^\s*-\s*name:)", content, flags=re.MULTILINE)
                    for block in blocks:
                        nm = name_pattern.search(block)
                        if not nm:
                            continue
                        vn = nm.group(1).strip().strip("'\"")
                        if vn:
                            refs = ref_pattern.findall(block)
                            if refs:
                                view_map[vn] = refs
            except Exception:
                continue

        return view_map

    @staticmethod
    def _build_manifest(
        tables_data: Dict[str, List],
        file_checksums: Dict[str, Dict],
        manifest_parser: Optional[ManifestParser],
    ) -> Dict[str, Any]:
        from snowflake_semantic_tools._version import __version__

        manifest = {
            "metadata": {
                "sst_version": __version__,
                "schema_version": MANIFEST_VERSION,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "dbt_manifest_path": (
                    str(manifest_parser.manifest_path) if manifest_parser and manifest_parser.manifest_path else None
                ),
                "project_dir": str(Path.cwd()),
            },
            "file_checksums": file_checksums,
            "tables": tables_data,
        }
        return manifest
