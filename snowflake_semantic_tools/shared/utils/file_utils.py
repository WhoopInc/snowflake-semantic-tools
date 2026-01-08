"""
File Utilities

Simple file finding operations for dbt and semantic model files.
Replaces Git infrastructure - assumes running from dbt project root.
"""

from pathlib import Path
from typing import List

import yaml

from snowflake_semantic_tools.shared.utils.logger import get_logger

logger = get_logger(__name__)


def get_dbt_model_paths() -> List[Path]:
    """
    Get dbt model paths from dbt_project.yml.

    Resolution order:
    1. dbt_project.yml model-paths (primary source)
    2. Default: ["models"] (dbt default)

    Returns:
        List of absolute paths to model directories
    """
    cwd = Path.cwd()
    dbt_project_file = cwd / "dbt_project.yml"

    if dbt_project_file.exists():
        try:
            with open(dbt_project_file, "r", encoding="utf-8") as f:
                dbt_config = yaml.safe_load(f) or {}
                model_paths = dbt_config.get("model-paths", ["models"])
                logger.debug(f"Found model-paths in dbt_project.yml: {model_paths}")
                return [cwd / p for p in model_paths]
        except yaml.YAMLError as e:
            logger.warning(f"Error parsing dbt_project.yml: {e}. Using default model path.")
        except Exception as e:
            logger.warning(f"Error reading dbt_project.yml: {e}. Using default model path.")

    logger.debug("No dbt_project.yml found or no model-paths specified. Using default: models/")
    return [cwd / "models"]


def find_dbt_model_files(exclude_dirs: List[str] = None) -> List[Path]:
    """
    Find all dbt model YAML files in the project.

    Reads model paths from dbt_project.yml (or defaults to "models").

    Args:
        exclude_dirs: Directory names or glob patterns to exclude
                     - Simple: "_intermediate" excludes any dir with that name
                     - Pattern: "models/amplitude/*" excludes specific paths

    Returns:
        List of dbt model YAML file paths
    """
    import fnmatch

    model_paths = get_dbt_model_paths()

    # Collect all YAML files from all model directories
    all_files = []
    for models_dir in model_paths:
        if not models_dir.exists():
            logger.warning(f"Model directory not found: {models_dir}")
            continue
        all_files.extend(list(models_dir.rglob("*.yml")) + list(models_dir.rglob("*.yaml")))

    if not all_files:
        # No files found in any model directory
        return []

    # Filter out excluded directories and patterns
    exclude_patterns = exclude_dirs or []
    filtered_files = []

    for file_path in all_files:
        # Get relative path from the file's parent model directory
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
