"""
File Utilities

Simple file finding operations for dbt and semantic model files.
Replaces Git infrastructure - assumes running from dbt project root.
"""

from pathlib import Path
from typing import List, Optional, Tuple

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


def expand_path_pattern(pattern: str) -> List[Path]:
    """
    Expand wildcard patterns to matching file/directory paths.

    Supports:
    - Single file: path/to/file.yml
    - Directory: path/to/dir/
    - Wildcard: path/to/prefix_*.yml
    - Subdirectory: path/to/subdir/*

    Args:
        pattern: Path pattern that may contain wildcards (*, ?)

    Returns:
        List of matching Path objects (absolute paths)
    """
    pattern_path = Path(pattern)

    # Check if pattern contains wildcards
    has_wildcards = "*" in pattern or "?" in pattern

    if not has_wildcards:
        # No wildcards - return as-is if it exists
        if pattern_path.exists():
            return [pattern_path.resolve()]
        return []

    # Has wildcards - need to expand
    # Find the base directory (everything up to the first wildcard)
    parts = pattern.split("/")
    base_parts = []
    pattern_parts = []

    for part in parts:
        if "*" in part or "?" in part:
            # Found first wildcard - everything before is base, this and after is pattern
            pattern_parts = parts[len(base_parts) :]
            break
        base_parts.append(part)

    if not base_parts:
        # Pattern starts with wildcard - use current directory as base
        base_dir = Path.cwd()
        glob_pattern = pattern
    else:
        # Build base directory from parts before first wildcard
        base_dir = Path("/".join(base_parts))
        if not base_dir.is_absolute():
            base_dir = Path.cwd() / base_dir
        # Build glob pattern from remaining parts
        glob_pattern = "/".join(pattern_parts)

    if not base_dir.exists():
        # Base directory doesn't exist - no matches possible
        return []

    # Use pathlib glob to find matches
    try:
        last_part = glob_pattern.split("/")[-1]
        has_extension = "." in last_part

        # If pattern doesn't have extension, try to match files with common extensions first
        # This handles patterns like "profound_*" which should match profound_*.yml, profound_*.sql, etc.
        matches = []
        if not has_extension and "*" in last_part:
            # Pattern like "profound_*" - try with common extensions to find files
            for ext in [".yml", ".yaml", ".sql"]:
                extended_pattern = glob_pattern + ext
                extended_matches = list(base_dir.glob(extended_pattern))
                matches.extend([m for m in extended_matches if m.is_file()])

        # Also try the pattern as-is (might match files or directories)
        direct_matches = list(base_dir.glob(glob_pattern))
        matches.extend(direct_matches)

        # If we still don't have file matches and pattern doesn't have extension,
        # try to find files by prefix (handles any extension)
        if not has_extension and "*" in last_part:
            prefix = last_part.replace("*", "").replace("?", "")
            if prefix:
                # Find all files in base_dir that start with the prefix
                for item in base_dir.iterdir():
                    if item.name.startswith(prefix) and item.is_file() and item not in matches:
                        matches.append(item)

        if not matches:
            return []

        # Separate files and directories
        file_matches = [m for m in matches if m.is_file()]
        dir_matches = [m for m in matches if m.is_dir()]

        # Prefer files over directories (user likely wants files when using wildcards)
        # Only use directories if no files were found
        final_matches = file_matches if file_matches else dir_matches

        # Resolve all paths to absolute and remove duplicates
        resolved_matches = list(set([m.resolve() for m in final_matches]))
        return sorted(resolved_matches)
    except Exception as e:
        logger.warning(f"Error expanding pattern '{pattern}': {e}")
        return []


def _convert_to_sql_files(file_paths: List[Path], seen_stems: set, verbose: bool = False) -> List[str]:
    """
    Convert YAML/SQL file paths to SQL file paths, deduplicating by stem.

    Args:
        file_paths: List of file paths (YAML or SQL)
        seen_stems: Set of already-seen file stems (modified in-place)
        verbose: Whether to show verbose output

    Returns:
        List of SQL file paths as strings
    """
    sql_files = []
    for file_path in file_paths:
        stem = file_path.stem
        if stem in seen_stems:
            continue

        if file_path.suffix in [".yml", ".yaml"]:
            sql_path = file_path.with_suffix(".sql")
            if sql_path.exists():
                sql_files.append(str(sql_path))
                seen_stems.add(stem)
            elif verbose:
                logger.debug(f"No corresponding SQL file found for {file_path}")
        elif file_path.suffix == ".sql":
            sql_files.append(str(file_path))
            seen_stems.add(stem)
    return sql_files


def resolve_wildcard_path_for_enrich(
    pattern: str, output, verbose: bool = False
) -> Tuple[Optional[str], Optional[List[str]]]:
    """
    Resolve a wildcard pattern for the enrich command.

    This is a standardized function that handles wildcard expansion and converts
    the results to the appropriate format for the enrich command (target_path or model_files).

    Args:
        pattern: Path pattern that may contain wildcards
        output: CLIOutput instance for logging
        verbose: Whether to show verbose output

    Returns:
        Tuple of (target_path, model_files) where:
        - target_path: String path to use (if single file/dir), or None
        - model_files: List of SQL file paths to process, or None
    """
    expanded_paths = expand_path_pattern(pattern)
    if not expanded_paths:
        raise ValueError(f"No files found matching pattern '{pattern}'")

    output.info(
        f"Wildcard expansion found {len(expanded_paths)} match(es): {', '.join([p.name for p in expanded_paths])}"
    )

    files = [p for p in expanded_paths if p.is_file()]
    dirs = [p for p in expanded_paths if p.is_dir()]
    seen_stems = set()

    if len(expanded_paths) == 1:
        single_path = expanded_paths[0]
        if single_path.is_file():
            sql_files = _convert_to_sql_files([single_path], seen_stems, verbose)
            if sql_files:
                if verbose:
                    output.info(f"Processing single file: {Path(sql_files[0]).name}")
                return None, sql_files
            return str(single_path), None
        else:
            output.warning(f"Wildcard matched a directory instead of files: {single_path.name}")
            return str(single_path), None
    elif files:
        sql_files = _convert_to_sql_files(files, seen_stems, verbose)
        if sql_files:
            output.info(f"Wildcard matched {len(sql_files)} file(s): {', '.join([Path(f).stem for f in sql_files])}")
            return None, sql_files
        parent_dirs = {p.parent for p in files}
        return str(parent_dirs.pop() if len(parent_dirs) == 1 else expanded_paths[0]), None
    elif dirs:
        all_files = []
        for dir_path in dirs:
            all_files.extend(dir_path.glob("*.yml"))
            all_files.extend(dir_path.glob("*.yaml"))
            all_files.extend(dir_path.glob("*.sql"))
        sql_files = _convert_to_sql_files(all_files, seen_stems, verbose)
        if sql_files:
            output.info(
                f"Wildcard matched {len(dirs)} directory/directories, found {len(sql_files)} model file(s): {', '.join([Path(f).stem for f in sql_files])}"
            )
            return None, sql_files
        output.warning(
            f"Wildcard matched {len(dirs)} directory/directories but no model files found, using first directory: {dirs[0].name}"
        )
        return str(dirs[0]), None
    else:
        return str(expanded_paths[0]), None
