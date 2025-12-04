"""
File Utilities

Simple file finding operations for dbt and semantic model files.
Replaces Git infrastructure - assumes running from dbt project root.
"""

from pathlib import Path
from typing import List

import yaml


def find_dbt_model_files(exclude_dirs: List[str] = None) -> List[Path]:
    """
    Find all dbt model YAML files in the project.

    Args:
        exclude_dirs: Directory names or glob patterns to exclude
                     - Simple: "_intermediate" excludes any dir with that name
                     - Pattern: "models/amplitude/*" excludes specific paths

    Returns:
        List of dbt model YAML file paths
    """
    import fnmatch

    from snowflake_semantic_tools.shared.config import get_config

    config = get_config()
    models_dir_name = config.get("project", {}).get("dbt_models_dir")

    if not models_dir_name:
        raise ValueError("dbt_models_dir not configured in sst_config.yml")

    models_dir = Path.cwd() / models_dir_name

    if not models_dir.exists():
        raise FileNotFoundError(f"dbt models directory not found: {models_dir}")

    # Find all YAML files
    all_files = list(models_dir.rglob("*.yml")) + list(models_dir.rglob("*.yaml"))

    # Filter out excluded directories and patterns
    exclude_patterns = exclude_dirs or []
    filtered_files = []

    for file_path in all_files:
        # Get relative path from models directory
        try:
            rel_path = file_path.relative_to(models_dir)
        except ValueError:
            rel_path = file_path

        should_exclude = False

        for pattern in exclude_patterns:
            # If pattern has path separators or wildcards, use glob matching
            if "/" in pattern or "*" in pattern:
                # Glob pattern - match against relative path
                # Remove "models/" prefix if present in pattern for matching
                pattern_normalized = pattern.replace("models/", "")
                if fnmatch.fnmatch(str(rel_path), pattern_normalized):
                    should_exclude = True
                    break
            else:
                # Simple directory name - check if it's in the path parts
                if pattern in file_path.parts:
                    should_exclude = True
                    break

        if should_exclude:
            continue

        # Check if it's a dbt model file (contains 'models:' key)
        if _is_dbt_model_file(file_path):
            filtered_files.append(file_path)

    return filtered_files


def find_semantic_model_files() -> List[Path]:
    """
    Find all semantic model YAML files in the project.

    Returns:
        List of semantic model YAML file paths
    """
    from snowflake_semantic_tools.shared.config import get_config

    config = get_config()
    semantic_dir_name = config.get("project", {}).get("semantic_models_dir")

    if not semantic_dir_name:
        raise ValueError("semantic_models_dir not configured in sst_config.yml")

    semantic_dir = Path.cwd() / semantic_dir_name

    if not semantic_dir.exists():
        # Semantic models are optional - return empty list
        return []

    # Find all YAML files
    return list(semantic_dir.rglob("*.yml")) + list(semantic_dir.rglob("*.yaml"))


def _is_dbt_model_file(file_path: Path) -> bool:
    """
    Check if a YAML file is a dbt model file.

    Args:
        file_path: Path to YAML file

    Returns:
        True if file contains 'models:' key
    """
    try:
        with open(file_path, "r") as f:
            content = yaml.safe_load(f)
            return isinstance(content, dict) and "models" in content
    except Exception:
        return False
