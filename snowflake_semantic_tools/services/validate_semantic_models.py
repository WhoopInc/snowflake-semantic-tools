"""
Validate Semantic Models Service

Comprehensive validation orchestration for semantic model quality assurance.

Ensures semantic models are production-ready by validating against multiple
criteria including dbt model existence, reference integrity, naming uniqueness,
and best practices. Critical for CI/CD pipelines to catch issues before
deployment to production environments.

The service provides both strict mode (errors block deployment) and standard
mode (warnings allowed) to support different stages of the development lifecycle.
"""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from snowflake_semantic_tools.core.models import ValidationResult
from snowflake_semantic_tools.core.models.validation import ValidationSeverity
from snowflake_semantic_tools.core.parsing import Parser
from snowflake_semantic_tools.core.parsing.parser import ParsingCriticalError
from snowflake_semantic_tools.core.validation import SemanticValidator
from snowflake_semantic_tools.shared.events import (
    ValidationCompleted,
    ValidationError,
    ValidationStarted,
    ValidationWarning,
    fire_event,
)
from snowflake_semantic_tools.shared.utils import get_logger
from snowflake_semantic_tools.shared.utils.file_utils import find_dbt_model_files, find_semantic_model_files

logger = get_logger("validate_semantic_models")


@dataclass
class ValidateConfig:
    """Configuration for model validation."""

    dbt_path: Optional[Path] = None  # Optional override for dbt models directory
    semantic_path: Optional[Path] = None  # Optional override for semantic models directory
    strict_mode: bool = False
    enable_template_resolution: bool = True
    check_duplicates: bool = True
    check_hardcoded: bool = True
    exclude_dirs: Optional[List[str]] = None  # Directories to exclude from discovery


class SemanticMetadataCollectionValidationService:
    """
    Service for validating semantic models against dbt definitions.

    This service:
    1. Finds dbt and semantic model files in current directory
    2. Parses YAML files
    3. Validates semantic models against dbt definitions
    4. Checks for duplicates, performance issues, and hardcoded values
    5. Returns comprehensive validation results
    """

    def __init__(self, parser: Parser, validator: SemanticValidator):
        """
        Initialize the service.

        Args:
            parser: Parser for YAML files
            validator: Semantic model validator
        """
        self.parser = parser
        self.validator = validator

    def execute(self, config: ValidateConfig, verbose: bool = False) -> ValidationResult:
        """
        Execute the validation workflow.

        Args:
            config: Validation configuration
            verbose: Whether to output detailed progress

        Returns:
            Validation result with all issues found
        """
        logger.debug("Starting validation")
        start_time = time.time()

        result = ValidationResult()

        try:
            # CRITICAL: Load manifest.json (REQUIRED for validation)
            import sys

            import click

            from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

            manifest_parser = ManifestParser()

            # Check if manifest exists and load it
            if not manifest_parser.load():
                # HARD REQUIREMENT: Manifest must exist
                click.echo("\nERROR: manifest.json not found", err=True)
                click.echo("\nmanifest.json is REQUIRED for validation.", err=True)
                click.echo(
                    "SST uses dbt's compiled manifest as the single source of truth for database/schema resolution.\n",
                    err=True,
                )
                click.echo("To fix this, run ONE of the following:\n", err=True)
                click.echo("  Option 1 (Recommended): Use auto-compile flag", err=True)
                click.echo("    sst validate --dbt-compile\n", err=True)
                click.echo("  Option 2: Manually compile dbt first", err=True)
                click.echo("    dbt compile --target prod", err=True)
                click.echo("    sst validate\n", err=True)
                sys.exit(1)

            model_count = len(manifest_parser.model_locations) if manifest_parser.model_locations else 0
            logger.info(f"Loaded manifest with {model_count} models")

            # Check if manifest is stale
            is_stale, stale_reason = manifest_parser.is_manifest_stale(threshold_hours=24)

            if is_stale:
                click.echo(f"\nWARNING: Manifest may be outdated", err=True)
                click.echo(f"  Reason: {stale_reason}\n", err=True)

                # Check if we're in an interactive terminal
                if sys.stdin.isatty() and sys.stdout.isatty():
                    # Interactive mode - prompt user
                    click.echo("Options:", err=True)
                    click.echo("  1. Continue with current manifest (may produce incorrect results)", err=True)
                    click.echo("  2. Exit and recompile (recommended)", err=True)
                    click.echo("     Run: sst validate --dbt-compile\n", err=True)

                    choice = click.prompt(
                        "Choose an option", type=click.Choice(["1", "2"]), default="2", err=True, show_choices=True
                    )

                    if choice == "2":
                        click.echo("\nExiting. Please run: sst validate --dbt-compile", err=True)
                        sys.exit(1)
                    else:
                        click.echo("\nWARNING: Continuing with stale manifest (results may be incorrect)\n", err=True)
                else:
                    # Non-interactive mode (CI/CD) - just warn and continue
                    click.echo("Continuing in non-interactive mode. To refresh manifest, use:", err=True)
                    click.echo("  sst validate --dbt-compile\n", err=True)

            # Set manifest on parser
            self.parser.manifest_parser = manifest_parser

            # Step 1: Find dbt model files
            click.echo("Scanning project for dbt models...")

            if config.dbt_path:
                # Custom path provided
                dbt_path = config.dbt_path if config.dbt_path.is_absolute() else Path.cwd() / config.dbt_path

                # Find all YAML files
                all_yml_files = list(dbt_path.rglob("*.yml")) + list(dbt_path.rglob("*.yaml"))

                # Filter for dbt model files
                from snowflake_semantic_tools.shared.utils.file_utils import _is_dbt_model_file

                dbt_files = [f for f in all_yml_files if _is_dbt_model_file(f)]
            else:
                # Use default from config
                dbt_files = find_dbt_model_files(exclude_dirs=config.exclude_dirs)

            click.echo(f"Found {len(dbt_files)} dbt models")

            # Validate exclusion patterns if verbose mode
            if config.exclude_dirs and verbose:
                self._validate_exclusion_patterns(config.exclude_dirs)

            # Step 2: Find semantic model files
            if config.semantic_path:
                semantic_path = (
                    config.semantic_path if config.semantic_path.is_absolute() else Path.cwd() / config.semantic_path
                )
                semantic_files = list(semantic_path.rglob("*.yml")) + list(semantic_path.rglob("*.yaml"))
            else:
                semantic_files = find_semantic_model_files()

            logger.debug(f"Found {len(dbt_files)} dbt files and {len(semantic_files)} semantic files")

            # Fire event: Validation started
            fire_event(ValidationStarted(model_count=len(dbt_files)))

            # Log file breakdown for debugging
            if verbose:
                metrics_files = [f for f in semantic_files if "metrics" in str(f)]
                relationships_files = [f for f in semantic_files if "relationships" in str(f)]
                views_files = [f for f in semantic_files if "semantic_views" in str(f)]
                custom_instructions_files = [f for f in semantic_files if "custom_instructions" in str(f)]
                filters_files = [f for f in semantic_files if "filters" in str(f)]
                verified_queries_files = [f for f in semantic_files if "verified_queries" in str(f)]

                logger.debug(
                    f"File breakdown: {len(dbt_files)} dbt, {len(metrics_files)} metrics, {len(relationships_files)} relationships, {len(views_files)} views"
                )

            # Step 3: Parse files
            try:
                parse_result = self.parser.parse_all_files(dbt_files, semantic_files)

                # Log parsed entities for debugging
                if "dbt" in parse_result:
                    dbt_data = parse_result["dbt"]
                    total_tables = len(dbt_data.get("sm_tables", []))
                    total_dimensions = len(dbt_data.get("sm_dimensions", []))
                    total_facts = len(dbt_data.get("sm_facts", []))
                    logger.debug(
                        f"Parsed {total_tables} tables with {total_dimensions} dimensions, {total_facts} facts"
                    )

            except ParsingCriticalError as e:
                # Critical parsing errors prevent further validation
                logger.error(f"Critical parsing errors detected: {e}")
                for error in e.errors:
                    result.add_error(f"CRITICAL: {error}")
                    logger.error(f"Parsing error: {error}")

                # Return early - no point in validating malformed data
                result.total_time = time.time() - start_time
                fire_event(
                    ValidationCompleted(
                        total_models=0, error_count=len(e.errors), warning_count=0, duration_seconds=result.total_time
                    )
                )
                return result

            # Step 4: Validate semantic models
            # (Internal validation steps - no need to display)

            validation_result = self.validator.validate(parse_result, check_duplicates=config.check_duplicates)

            result.merge(validation_result)

            # Step 5: Add hardcoded value warnings if enabled
            if config.check_hardcoded and "semantic" in parse_result:
                for component_type, component_data in parse_result["semantic"].items():
                    if isinstance(component_data, dict) and "warnings" in component_data:
                        for warning in component_data["warnings"]:
                            result.add_warning(warning)

            logger.debug(f"Validation complete: {result.error_count} errors, {result.warning_count} warnings")

            # Fire event: Validation completed
            duration = time.time() - start_time
            fire_event(
                ValidationCompleted(
                    total_models=len(dbt_files),
                    error_count=result.error_count,
                    warning_count=result.warning_count,
                    duration_seconds=duration,
                )
            )

        except Exception as e:
            logger.error(f"Validation failed: {e}")
            result.add_error(f"Validation failed: {str(e)}")

        return result

    def _validate_exclusion_patterns(self, exclude_patterns: List[str]):
        """
        Validate that exclusion patterns match actual directories/files.
        Warns about patterns that don't exclude anything (likely typos or outdated config).

        Args:
            exclude_patterns: List of exclusion patterns to validate
        """
        import fnmatch

        import click

        from snowflake_semantic_tools.shared.config import get_config

        config = get_config()
        models_dir_name = config.get("project", {}).get("dbt_models_dir", "models")
        models_dir = Path.cwd() / models_dir_name

        if not models_dir.exists():
            return

        # Get all YAML files
        all_files = list(models_dir.rglob("*.yml")) + list(models_dir.rglob("*.yaml"))

        # Check each exclusion pattern
        unused_patterns = []

        for pattern in exclude_patterns:
            matched_any = False

            for file_path in all_files:
                try:
                    rel_path = file_path.relative_to(models_dir)
                except ValueError:
                    rel_path = file_path

                # Check if pattern matches this file
                if "/" in pattern or "*" in pattern:
                    # Glob pattern
                    pattern_normalized = pattern.replace("models/", "")
                    if fnmatch.fnmatch(str(rel_path), pattern_normalized):
                        matched_any = True
                        break
                else:
                    # Simple directory name
                    if pattern in file_path.parts:
                        matched_any = True
                        break

            if not matched_any:
                unused_patterns.append(pattern)

        # Warn about unused patterns (possible typos or outdated config)
        if unused_patterns:
            click.echo(
                "\nWARNING: Exclusion patterns that matched no files (check for typos or outdated config):", err=True
            )
            for pattern in unused_patterns:
                click.echo(f"  - {pattern}", err=True)
            click.echo("")

    @classmethod
    def create_from_config(cls) -> "SemanticMetadataCollectionValidationService":
        """
        Create validation service.

        Returns:
            Configured service instance
        """
        parser = Parser(enable_template_resolution=True)
        validator = SemanticValidator()
        return cls(parser, validator)
