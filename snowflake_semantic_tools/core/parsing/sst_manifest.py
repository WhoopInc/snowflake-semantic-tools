"""
SST Manifest

Tracks checksums of SST-relevant YAML files for change detection.

Used by `sst generate --only-modified` to detect when SST YAML files
(metrics, relationships, verified queries, custom instructions, semantic
views, and dbt model YAMLs) have changed since the last generation,
even when the underlying .sql files haven't.

The manifest is saved as `sst_manifest.json` in the target directory
alongside dbt's `manifest.json`.
"""

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml as pyyaml

from snowflake_semantic_tools.shared.config import get_config
from snowflake_semantic_tools.shared.utils import get_logger
from snowflake_semantic_tools.shared.utils.file_utils import get_dbt_model_paths

logger = get_logger(__name__)

MANIFEST_FILENAME = "sst_manifest.json"
MANIFEST_VERSION = 1


@dataclass
class SSTManifestDiff:
    """Result of comparing two SST manifests."""

    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    modified: List[str] = field(default_factory=list)
    unchanged: List[str] = field(default_factory=list)
    config_changed: bool = False

    @property
    def changed_files(self) -> List[str]:
        return sorted(self.added + self.modified + self.removed)

    @property
    def total_changes(self) -> int:
        return len(self.added) + len(self.modified) + len(self.removed)

    def summary(self) -> str:
        parts = []
        if self.added:
            parts.append(f"{len(self.added)} added")
        if self.modified:
            parts.append(f"{len(self.modified)} modified")
        if self.removed:
            parts.append(f"{len(self.removed)} removed")
        if self.unchanged:
            parts.append(f"{len(self.unchanged)} unchanged")
        if self.config_changed:
            parts.append("config changed")
        return ", ".join(parts) if parts else "no changes"

    def get_impacted_views(
        self,
        file_view_map: Dict[str, Optional[List[str]]],
        baseline_file_view_map: Optional[Dict[str, Optional[List[str]]]] = None,
    ) -> Optional[List[str]]:
        """
        Map changed files to impacted semantic views.

        Args:
            file_view_map: Current manifest's file → views mapping.
            baseline_file_view_map: Baseline manifest's file → views mapping
                (used for removed files).

        Returns:
            Sorted list of unique view names to regenerate, or None if all
            views should be regenerated.
        """
        if self.config_changed:
            return None

        views: set = set()

        for rel_path in self.added + self.modified:
            impacted = file_view_map.get(rel_path)
            if impacted is None:
                return None
            views.update(impacted)

        for rel_path in self.removed:
            lookup = baseline_file_view_map or file_view_map
            impacted = lookup.get(rel_path)
            if impacted is None:
                return None
            views.update(impacted)

        return sorted(views)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


def _get_semantic_models_dir(config: Any) -> Optional[str]:
    try:
        val = config.get("project.semantic_models_dir")
        if val:
            return val
    except Exception:
        pass
    try:
        project = config.get("project", {})
        if isinstance(project, dict):
            return project.get("semantic_models_dir")
    except Exception:
        pass
    return None


class SSTManifest:
    """Build, save, load, and compare SST YAML file checksums."""

    def __init__(self, project_dir: Optional[Path] = None):
        self.project_dir = project_dir or Path.cwd()
        self.files: Dict[str, Dict[str, Any]] = {}
        self.metadata: Dict[str, Any] = {}
        self.config_checksum: Optional[str] = None

    def build(self) -> "SSTManifest":
        """
        Discover SST-relevant YAML files, compute checksums, detect types,
        and map each file to the semantic views it impacts.
        """
        from snowflake_semantic_tools._version import __version__

        self.metadata = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sst_version": __version__,
            "schema_version": MANIFEST_VERSION,
            "project_dir": str(self.project_dir),
        }

        config = get_config()
        view_table_map, view_source_map = self._parse_view_tables(config)
        table_to_views = self._invert_view_table_map(view_table_map)

        self._collect_dbt_model_yamls(table_to_views)
        self._collect_semantic_model_yamls(config, view_table_map)
        self._collect_config_checksum()

        for rel_path, entry in self.files.items():
            file_type = entry.get("type")
            if file_type == "dbt":
                model_names = entry.get("model_names", [])
                views = set()
                for name in model_names:
                    matched = table_to_views.get(name.lower())
                    if matched:
                        views.update(matched)
                entry["views_impacted"] = sorted(views) if views else None
                entry.pop("model_names", None)
            elif file_type == "semantic_views":
                views_in_file = [vn for vn, src in view_source_map.items() if self._rel(Path(src)) == rel_path]
                entry["views_impacted"] = views_in_file if views_in_file else None
            else:
                entry["views_impacted"] = None

        logger.info(f"SST manifest built: {len(self.files)} file(s), {len(view_table_map)} view(s)")
        return self

    def save(self, target_dir: Path) -> Path:
        target_dir.mkdir(parents=True, exist_ok=True)
        out_path = target_dir / MANIFEST_FILENAME
        payload = {
            "metadata": self.metadata,
            "config_checksum": self.config_checksum,
            "file_checksums": self.files,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        logger.info(f"SST manifest saved: {out_path}")
        return out_path

    @classmethod
    def load(cls, path: Path) -> Optional["SSTManifest"]:
        manifest_path = path / MANIFEST_FILENAME if path.is_dir() else path
        if not manifest_path.exists():
            logger.debug(f"SST manifest not found: {manifest_path}")
            return None
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            schema_ver = data.get("metadata", {}).get("schema_version")
            if schema_ver and schema_ver > MANIFEST_VERSION:
                logger.warning(
                    f"SST manifest schema version {schema_ver} is newer than "
                    f"supported version {MANIFEST_VERSION}. Results may be inaccurate."
                )
            inst = cls()
            inst.metadata = data.get("metadata", {})
            inst.files = data.get("file_checksums", data.get("files", {}))
            inst.config_checksum = data.get("config_checksum")
            if not inst.config_checksum and "__config__" in inst.files:
                inst.config_checksum = inst.files.pop("__config__", {}).get("checksum")
            logger.info(f"SST manifest loaded: {manifest_path} ({len(inst.files)} files)")
            return inst
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not load SST manifest: {e}")
            return None

    def compare_to(self, other: "SSTManifest") -> SSTManifestDiff:
        diff = SSTManifestDiff()

        current_keys = set(self.files.keys())
        other_keys = set(other.files.keys())

        diff.added = sorted(current_keys - other_keys)
        diff.removed = sorted(other_keys - current_keys)

        for key in sorted(current_keys & other_keys):
            current_cs = self.files[key].get("checksum")
            other_cs = other.files[key].get("checksum")
            if current_cs != other_cs:
                diff.modified.append(key)
            else:
                diff.unchanged.append(key)

        diff.config_changed = self.config_checksum != other.config_checksum

        logger.info(f"SST manifest comparison: {diff.summary()}")
        return diff

    def get_file_view_map(self) -> Dict[str, Optional[List[str]]]:
        return {rel_path: entry.get("views_impacted") for rel_path, entry in self.files.items()}

    def _rel(self, abs_path: Path) -> str:
        try:
            return str(abs_path.resolve().relative_to(self.project_dir.resolve()))
        except ValueError:
            return str(abs_path)

    def _collect_dbt_model_yamls(self, table_to_views: Dict[str, List[str]]):
        for models_dir in get_dbt_model_paths():
            if not models_dir.exists():
                continue
            for yaml_file in sorted(list(models_dir.rglob("*.yml")) + list(models_dir.rglob("*.yaml"))):
                if not yaml_file.is_file():
                    continue
                try:
                    raw = yaml_file.read_bytes()
                except IOError:
                    logger.warning(f"Could not read {yaml_file}")
                    continue
                try:
                    data = pyyaml.safe_load(raw)
                except Exception:
                    continue
                if not isinstance(data, dict) or "models" not in data:
                    continue
                model_names = []
                models_list = data.get("models", [])
                if isinstance(models_list, list):
                    for m in models_list:
                        if isinstance(m, dict) and m.get("name"):
                            model_names.append(m["name"])
                checksum = f"sha256:{hashlib.sha256(raw).hexdigest()}"
                rel = self._rel(yaml_file)
                self.files[rel] = {
                    "checksum": checksum,
                    "type": "dbt",
                    "model_names": model_names,
                    "views_impacted": None,
                }

    def _collect_semantic_model_yamls(
        self,
        config: Any,
        view_table_map: Dict[str, List[str]],
    ):
        from snowflake_semantic_tools.core.parsing.file_detector import FileTypeDetector

        sem_dir_name = _get_semantic_models_dir(config)
        if not sem_dir_name:
            logger.debug("No semantic_models_dir configured — skipping semantic files")
            return

        sem_dir = self.project_dir / sem_dir_name
        if not sem_dir.exists():
            logger.debug(f"Semantic models directory does not exist: {sem_dir}")
            return

        for yaml_file in sorted(list(sem_dir.rglob("*.yml")) + list(sem_dir.rglob("*.yaml"))):
            if not yaml_file.is_file():
                continue
            rel = self._rel(yaml_file)
            try:
                checksum = _sha256(yaml_file)
            except IOError:
                logger.warning(f"Could not read {yaml_file}")
                continue
            file_type = FileTypeDetector.detect_semantic_type(yaml_file) or "unknown"
            self.files[rel] = {
                "checksum": checksum,
                "type": file_type,
                "views_impacted": None,
            }

    def _collect_config_checksum(self):
        for name in ("sst_config.yml", "sst_config.yaml", ".sst_config.yml", ".sst_config.yaml"):
            cfg_path = self.project_dir / name
            if cfg_path.exists():
                try:
                    self.config_checksum = _sha256(cfg_path)
                except IOError:
                    pass
                return

    def _parse_view_tables(self, config: Any):
        """
        Parse semantic_views YAML to build two maps:
        1. view_name -> [table_name, ...] (for table-to-view inversion)
        2. view_name -> source_file_path (for semantic_views file mapping)

        Handles both valid YAML and Jinja-templated files that can't be parsed.
        """
        sem_dir_name = _get_semantic_models_dir(config)
        if not sem_dir_name:
            return {}, {}

        sem_dir = self.project_dir / sem_dir_name
        if not sem_dir.exists():
            return {}, {}

        view_map: Dict[str, List[str]] = {}
        source_map: Dict[str, str] = {}
        ref_pattern = re.compile(r"\{\{\s*(?:ref|table)\(['\"]([^'\"]+)['\"]\)\s*\}\}")
        name_pattern = re.compile(r"^\s*-\s*name:\s*(.+)", re.MULTILINE)

        for yaml_file in sorted(list(sem_dir.rglob("*.yml")) + list(sem_dir.rglob("*.yaml"))):
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    content = f.read()

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
                        resolved_tables = []
                        for t in tables:
                            t_str = str(t)
                            match = ref_pattern.search(t_str)
                            if match:
                                resolved_tables.append(match.group(1))
                            else:
                                resolved_tables.append(t_str.split(".")[-1].strip().lower())
                        view_map[view_name] = resolved_tables
                        source_map[view_name] = str(yaml_file)
                else:
                    self._parse_view_tables_from_raw(content, ref_pattern, name_pattern, view_map)
                    for vn in view_map:
                        if vn not in source_map:
                            source_map[vn] = str(yaml_file)

            except Exception as e:
                logger.debug(f"Could not parse view tables from {yaml_file}: {e}")

        return view_map, source_map

    @staticmethod
    def _parse_view_tables_from_raw(
        content: str,
        ref_pattern,
        name_pattern,
        view_map: Dict[str, List[str]],
    ):
        """
        Fallback parser for Jinja-templated YAML that can't be parsed by PyYAML.

        Extracts view names and {{ ref('...') }} table references by splitting
        on `- name:` lines and scanning each block. This is intentionally
        approximate — it handles the common case of semantic_views YAML with
        Jinja ref() calls.
        """
        blocks = re.split(r"(?=^\s*-\s*name:)", content, flags=re.MULTILINE)
        for block in blocks:
            name_match = name_pattern.search(block)
            if not name_match:
                continue
            view_name = name_match.group(1).strip().strip("'\"")
            if not view_name:
                continue
            refs = ref_pattern.findall(block)
            if refs:
                view_map[view_name] = refs

    @staticmethod
    def _invert_view_table_map(
        view_table_map: Dict[str, List[str]],
    ) -> Dict[str, List[str]]:
        table_to_views: Dict[str, List[str]] = {}
        for view_name, tables in view_table_map.items():
            for table in tables:
                table_lower = table.lower()
                if table_lower not in table_to_views:
                    table_to_views[table_lower] = []
                if view_name not in table_to_views[table_lower]:
                    table_to_views[table_lower].append(view_name)
        return table_to_views
