"""
dbt-specific exceptions.

Provides clear, actionable error messages for dbt command failures.
"""


class DbtError(Exception):
    """Base exception for dbt-related errors."""

    pass


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
        message += "  1. Missing Snowflake credentials (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, etc.)\n"
        message += "  2. Incorrect profiles.yml configuration\n"
        message += "  3. Model SQL errors\n"
        message += "  4. Missing dbt dependencies (run: dbt deps)\n"

        super().__init__(message)
