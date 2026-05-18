"""
View-Table Parser

Shared utility for parsing semantic_views YAML to extract view-name → table-name
mappings. Used by both SSTManifest and CompileService to avoid code duplication.
"""

import re
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("view_table_parser")

REF_PATTERN = re.compile(r"\{\{\s*(?:ref|table)\(['\"]([^'\"]+)['\"]\)\s*\}\}")
NAME_PATTERN = re.compile(r"^\s*-\s*name:\s*(.+)", re.MULTILINE)


def parse_view_tables(sem_dir: Path) -> Tuple[Dict[str, List[str]], Dict[str, str]]:
    """
    Parse all semantic_views YAML files in a directory to build view-table mappings.

    Handles both valid YAML and Jinja-templated files that can't be parsed by PyYAML.

    Args:
        sem_dir: Path to the semantic models directory.

    Returns:
        Tuple of:
            view_map: view_name -> [table_name, ...] (resolved from ref()/table() templates)
            source_map: view_name -> source file path
    """
    if not sem_dir.exists():
        return {}, {}

    view_map: Dict[str, List[str]] = {}
    source_map: Dict[str, str] = {}

    for yaml_file in sorted(list(sem_dir.rglob("*.yml")) + list(sem_dir.rglob("*.yaml"))):
        try:
            content = yaml_file.read_text(encoding="utf-8")

            if "semantic_views:" not in content:
                continue

            try:
                data = yaml.safe_load(content)
            except yaml.YAMLError:
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
                        match = REF_PATTERN.search(t_str)
                        if match:
                            resolved_tables.append(match.group(1))
                        else:
                            resolved_tables.append(t_str.split(".")[-1].strip().lower())
                    view_map[view_name] = resolved_tables
                    source_map[view_name] = str(yaml_file)
            else:
                _parse_from_raw(content, view_map)
                for vn in view_map:
                    if vn not in source_map:
                        source_map[vn] = str(yaml_file)

        except Exception as e:
            logger.debug(f"Could not parse view tables from {yaml_file}: {e}")

    return view_map, source_map


def _parse_from_raw(content: str, view_map: Dict[str, List[str]]):
    """
    Fallback parser for Jinja-templated YAML that can't be parsed by PyYAML.

    Extracts view names and {{ ref('...') }} table references by splitting
    on `- name:` lines and scanning each block.
    """
    blocks = re.split(r"(?=^\s*-\s*name:)", content, flags=re.MULTILINE)
    for block in blocks:
        name_match = NAME_PATTERN.search(block)
        if not name_match:
            continue
        view_name = name_match.group(1).strip().strip("'\"")
        if not view_name:
            continue
        refs = REF_PATTERN.findall(block)
        if refs:
            view_map[view_name] = refs
