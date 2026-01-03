"""
dbt-specific exceptions.

Provides clear, actionable error messages for dbt command failures
and profile configuration issues.
"""

from pathlib import Path
from typing import List, Optional


class DbtError(Exception):
    """Base exception for dbt-related errors."""

    pass


# =============================================================================
# Profile Exceptions
# =============================================================================


class DbtProfileError(DbtError):
    """Base exception for dbt profile-related errors."""

    pass


class DbtProfileNotFoundError(DbtProfileError):
    """profiles.yml file not found."""

    def __init__(self, searched_paths: Optional[List[str]] = None):
        self.searched_paths = searched_paths or []
        paths_str = "\n".join(f"  - {p}" for p in self.searched_paths) if self.searched_paths else "  (none)"

        self.message = (
            "No dbt profiles.yml found.\n\n"
            f"Searched locations:\n{paths_str}\n\n"
            "SST requires profiles.yml for Snowflake authentication.\n\n"
            "To fix:\n"
            "  1. Create ~/.dbt/profiles.yml\n"
            "  2. Add a profile matching your dbt_project.yml 'profile:' setting\n\n"
            "See: https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup"
        )
        super().__init__(self.message)


class DbtProjectNotFoundError(DbtProfileError):
    """dbt_project.yml not found."""

    def __init__(self, project_dir: Optional[Path] = None):
        self.project_dir = project_dir
        dir_str = str(project_dir) if project_dir else "current directory"

        self.message = (
            f"No dbt_project.yml found in {dir_str}.\n\n"
            "SST must be run from a dbt project directory.\n\n"
            "To fix:\n"
            "  1. Navigate to your dbt project root (where dbt_project.yml exists)\n"
            "  2. Run SST commands from there\n"
        )
        super().__init__(self.message)


class DbtProfileParseError(DbtProfileError):
    """Error parsing profiles.yml or extracting profile configuration."""

    def __init__(self, message: str, profile_path: Optional[Path] = None):
        self.profile_path = profile_path
        path_str = f" ({profile_path})" if profile_path else ""

        self.message = f"Error parsing dbt profile{path_str}:\n\n{message}"
        super().__init__(self.message)


class DbtNotFoundError(DbtError):
    """dbt command not found in PATH."""

    def __init__(self, message: str = None):
        self.message = message or (
            "dbt command not found.\n\n"
            "Install dbt:\n\n"
            "  dbt Core:\n"
            "    pip install dbt-snowflake\n"
            "    https://docs.getdbt.com/docs/core/installation\n\n"
            "  dbt Cloud CLI:\n"
            "    pip install dbt\n"
            "    https://docs.getdbt.com/docs/cloud/cloud-cli-installation\n"
        )
        super().__init__(self.message)


class DbtCompileError(DbtError):
    """dbt compile command failed."""

    def __init__(self, stderr: str, target: str = None):
        self.stderr = stderr
        self.target = target

        message = f"dbt compile failed"
        if target:
            message += f" (target: {target})"
        message += ":\n\n" + stderr

        message += "\n\nCommon causes:\n"
        message += "  1. Missing or incorrect ~/.dbt/profiles.yml configuration\n"
        message += "  2. Profile name mismatch between dbt_project.yml and profiles.yml\n"
        message += "  3. Model SQL errors\n"
        message += "  4. Missing dbt dependencies (run: dbt deps)\n"

        super().__init__(message)
