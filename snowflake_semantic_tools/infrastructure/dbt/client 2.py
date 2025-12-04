"""
dbt CLI Client

Wrapper for dbt command-line interface operations.

Provides a consistent interface for running dbt commands with automatic detection
of dbt Core vs dbt Cloud CLI (pip). Adjusts command syntax automatically:
- dbt Core: Adds --target flag
- dbt Cloud CLI: Omits --target flag

Auto-detection works for:
- pip-installed Cloud CLI (outputs "dbt Cloud CLI - X.X.X")
- dbt Core (outputs "Core:")

Note: brew-installed Cloud CLI is not auto-detectable. Use pip: `pip install dbt`
"""

import subprocess
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional

from snowflake_semantic_tools.infrastructure.dbt.exceptions import DbtCompileError, DbtNotFoundError
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


class DbtType(Enum):
    """Type of dbt installation."""

    CORE = "core"
    CLOUD_CLI = "cloud_cli"
    UNKNOWN = "unknown"


@dataclass
class DbtResult:
    """
    Result from a dbt command execution.

    Provides structured access to dbt command results with
    success indicators and output for error handling.
    """

    success: bool
    command: str
    stdout: str
    stderr: str
    returncode: int
    dbt_type: DbtType

    @property
    def output(self) -> str:
        """Combined stdout and stderr for display."""
        return f"{self.stdout}\n{self.stderr}".strip()


class DbtClient:
    """
    Client for executing dbt CLI commands.

    Automatically detects whether dbt Core or dbt Cloud CLI is installed
    and adjusts command behavior accordingly:

    - dbt Core: Uses --target flag for environment selection
    - dbt Cloud CLI: Omits --target (uses cloud environment)

    Detection is automatic via `dbt --version` output:
    - pip Cloud CLI: "dbt Cloud CLI - X.X.X" → Auto-detected ✓
    - dbt Core: "Core:\n  - dbt-core: X.X.X" → Auto-detected ✓

    NOTE: brew-installed Cloud CLI outputs "Core:" and cannot be distinguished
    from dbt Core. Use pip installation: `pip install dbt`

    Usage:
        client = DbtClient()  # Auto-detects from dbt --version
        result = client.compile(target='prod')
        if not result.success:
            raise DbtCompileError(result.stderr)
    """

    def __init__(self, project_dir: Optional[Path] = None, verbose: bool = False):
        """
        Initialize dbt client.

        Automatically detects whether dbt Core or dbt Cloud CLI (pip) is installed
        by analyzing dbt --version output.

        Args:
            project_dir: dbt project directory (default: current directory)
            verbose: Enable verbose logging
        """
        self.project_dir = project_dir or Path.cwd()
        self.verbose = verbose
        self.dbt_type = DbtType.UNKNOWN
        self.version = None

        # Auto-detect dbt type from version output
        self._detect_dbt_type()

    def compile(
        self, target: Optional[str] = None, select: Optional[str] = None, exclude: Optional[str] = None
    ) -> DbtResult:
        """
        Run dbt compile to generate manifest.json.

        dbt compile:
        - Parses all project files
        - Compiles Jinja templates
        - Connects to database to introspect schemas
        - Generates fully resolved manifest.json
        - Generates compiled SQL files in target/compiled/

        Args:
            target: dbt target name (ignored for Cloud CLI)
            select: Model selection criteria (e.g., 'model_name', 'tag:semantic')
            exclude: Models to exclude

        Returns:
            DbtResult with execution details
        """
        cmd = ["dbt", "compile"]

        # Handle --target based on detected dbt type
        if target:
            if self.dbt_type == DbtType.CORE:
                cmd.extend(["--target", target])
                logger.debug(f"Using --target {target} (dbt Core)")
            elif self.dbt_type == DbtType.CLOUD_CLI:
                logger.debug(f"Ignoring --target flag (dbt Cloud CLI uses cloud environment)")
            elif self.dbt_type == DbtType.UNKNOWN:
                # Don't add --target when type is unknown
                # If it's Core, it will use default target
                # If it's Cloud CLI, target is ignored anyway
                logger.warning(f"dbt type unknown - running compile without --target flag (will use dbt's default)")

        if select:
            cmd.extend(["--select", select])

        if exclude:
            cmd.extend(["--exclude", exclude])

        return self._run_command(cmd)

    def get_manifest_path(self) -> Path:
        """
        Get the expected path to manifest.json.

        Returns:
            Path to manifest.json (may not exist yet)
        """
        return self.project_dir / "target" / "manifest.json"

    def _run_command(self, cmd: List[str]) -> DbtResult:
        """
        Execute a dbt command with consistent error handling.

        Args:
            cmd: Command and arguments as list

        Returns:
            DbtResult with execution details

        Raises:
            DbtNotFoundError: If dbt command not found
        """
        cmd_str = " ".join(cmd)
        logger.debug(f"Executing: {cmd_str}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=self.project_dir)

            if self.verbose:
                logger.debug(f"Return code: {result.returncode}")
                if result.stdout:
                    logger.debug(f"stdout: {result.stdout[:500]}")
                if result.stderr:
                    logger.debug(f"stderr: {result.stderr[:500]}")

            return DbtResult(
                success=result.returncode == 0,
                command=cmd_str,
                stdout=result.stdout,
                stderr=result.stderr,
                returncode=result.returncode,
                dbt_type=self.dbt_type,
            )

        except FileNotFoundError:
            logger.error("dbt command not found in PATH")
            raise DbtNotFoundError()

    def _detect_dbt_type(self):
        """
        Auto-detect dbt type from --version output.

        Detection based on version output:
        - pip Cloud CLI: "dbt Cloud CLI - X.X.X" → CLOUD_CLI
        - dbt Core: "Core:\n  - dbt-core: X.X.X" → CORE
        - Uncertain: Defaults to CORE

        NOTE: brew-installed Cloud CLI outputs "Core:" (indistinguishable from dbt Core).
        We recommend pip installation for Cloud CLI which has distinct output.

        References:
        - dbt Core: https://docs.getdbt.com/docs/core/installation
        - dbt Cloud CLI (pip): https://docs.getdbt.com/docs/cloud/cloud-cli-installation
        """
        try:
            result = subprocess.run(["dbt", "--version"], capture_output=True, text=True, timeout=5)

            version_output = result.stdout + result.stderr
            version_lower = version_output.lower()

            # Pattern 1: pip-installed Cloud CLI (RELIABLE)
            # Outputs: "dbt Cloud CLI - 0.40.7 (...)"
            if "dbt cloud cli" in version_lower or "cloud cli -" in version_lower:
                self.dbt_type = DbtType.CLOUD_CLI
                self.version = version_output.strip().split("\n")[0]
                logger.info(f"Detected dbt Cloud CLI: {self.version}")
                return

            # Pattern 2: dbt Core (or brew Cloud CLI)
            # Outputs: "Core:\n  - dbt-core: 1.7.0"
            if "dbt-core:" in version_lower or ("core:" in version_lower and "installed:" in version_lower):
                self.dbt_type = DbtType.CORE
                self.version = version_output.strip().split("\n")[0]
                logger.info(f"Detected dbt Core: {self.version}")
                return

            # Pattern not recognized - default to Core (safest)
            self.dbt_type = DbtType.CORE
            self.version = version_output.strip()
            logger.warning(
                f"Could not clearly identify dbt type from version output. Defaulting to Core.\n"
                f"  Output: {version_output[:150]}"
            )

        except FileNotFoundError:
            logger.debug("dbt command not found - will raise error when commands are executed")
            self.dbt_type = DbtType.UNKNOWN
            self.version = None
        except subprocess.TimeoutExpired:
            logger.warning("dbt --version timed out - defaulting to Core")
            self.dbt_type = DbtType.CORE
            self.version = None
        except Exception as e:
            logger.warning(f"Error detecting dbt type: {e} - defaulting to Core")
            self.dbt_type = DbtType.CORE
            self.version = None
