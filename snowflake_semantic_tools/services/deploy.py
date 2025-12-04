"""
Deploy Service

Orchestrates the complete deployment workflow: validate → extract → generate.

Provides a single entry point for deploying semantic models to Snowflake,
combining validation, metadata extraction, and artifact generation into
one atomic operation.
"""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeConfig
from snowflake_semantic_tools.services.extract_semantic_metadata import ExtractConfig, SemanticMetadataExtractionService
from snowflake_semantic_tools.services.generate_semantic_views import (
    SemanticViewGenerationService,
    UnifiedGenerationConfig,
)
from snowflake_semantic_tools.services.validate_semantic_models import (
    SemanticMetadataCollectionValidationService,
    ValidateConfig,
)
from snowflake_semantic_tools.shared.config_utils import get_exclusion_patterns, is_strict_mode
from snowflake_semantic_tools.shared.progress import NoOpProgressCallback, ProgressCallback
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("deploy")


@dataclass
class DeployConfig:
    """Configuration for deployment workflow."""

    database: str
    schema: str
    skip_validation: bool = False
    verbose: bool = False
    quiet: bool = False


@dataclass
class DeployResult:
    """Result of deployment workflow."""

    success: bool
    validation_passed: bool
    extraction_completed: bool
    generation_completed: bool

    skip_validation: bool = False
    validation_errors: int = 0
    validation_warnings: int = 0
    rows_loaded: int = 0
    models_processed: int = 0
    views_created: int = 0

    errors: List[str] = None
    warnings: List[str] = None

    validation_time: float = 0.0
    extraction_time: float = 0.0
    generation_time: float = 0.0
    total_time: float = 0.0

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []

    def print_summary(self, quiet: bool = False):
        """Print deployment summary."""
        if quiet and self.success:
            # In quiet mode, only show output if there were errors
            return

        print()
        print("=" * 80)
        print("DEPLOYMENT SUMMARY")
        print("=" * 80)

        # Status
        status_icon = "SUCCESS" if self.success else "FAILED"
        print(f"Status: {status_icon}")
        print()

        # Step results (clean format without redundant PASS/FAIL icons)
        print("Workflow Steps:")
        if not self.skip_validation:
            val_status = "PASSED" if self.validation_passed else "FAILED"
            print(f"  Validation: {val_status}")
            if self.validation_errors > 0 or self.validation_warnings > 0:
                print(f"    Errors: {self.validation_errors}, Warnings: {self.validation_warnings}")

        ext_status = "COMPLETED" if self.extraction_completed else "FAILED"
        print(f"  Extraction: {ext_status}")
        if self.extraction_completed:
            print(f"    Loaded {self.rows_loaded:,} rows from {self.models_processed} models")

        gen_status = "COMPLETED" if self.generation_completed else "FAILED"
        print(f"  Generation: {gen_status}")
        if self.generation_completed:
            print(f"    Semantic views: {self.views_created} created")

        print()

        # Timing
        if not quiet:
            print("Execution Time:")
            if not self.skip_validation and self.validation_time > 0:
                print(f"  Validation: {self.validation_time:.1f}s")
            if self.extraction_time > 0:
                print(f"  Extraction: {self.extraction_time:.1f}s")
            if self.generation_time > 0:
                print(f"  Generation: {self.generation_time:.1f}s")
            print(f"  Total: {self.total_time:.1f}s")
            print()

        # Errors and warnings
        if self.errors:
            print(f"Errors ({len(self.errors)}):")
            for error in self.errors[:10]:  # Show first 10
                print(f"  • {error}")
            if len(self.errors) > 10:
                print(f"  ... and {len(self.errors) - 10} more errors")
            print()

        if self.warnings and not quiet:
            print(f"Warnings ({len(self.warnings)}):")
            for warning in self.warnings[:5]:  # Show first 5
                print(f"  • {warning}")
            if len(self.warnings) > 5:
                print(f"  ... and {len(self.warnings) - 5} more warnings")
            print()

        print("=" * 80)


class DeployService:
    """
    Orchestrates the complete deployment workflow.

    Combines validation, extraction, and generation into a single
    atomic operation with consistent error handling and reporting.
    """

    def __init__(self, snowflake_config: SnowflakeConfig):
        """Initialize deploy service with Snowflake configuration."""
        self.snowflake_config = snowflake_config

    def execute(self, config: DeployConfig, progress_callback: Optional[ProgressCallback] = None) -> DeployResult:
        """
        Execute the full deployment workflow.

        Args:
            config: Deployment configuration
            progress_callback: Optional callback for progress reporting

        Returns:
            DeployResult with comprehensive deployment status
        """
        # Use no-op callback if none provided
        progress = progress_callback or NoOpProgressCallback()

        start_time = time.time()
        result = DeployResult(
            success=False,
            validation_passed=False,
            extraction_completed=False,
            generation_completed=False,
            skip_validation=config.skip_validation,
        )

        try:
            # STEP 1: Validation (unless skipped)
            if not config.skip_validation:
                progress.stage("Validating semantic models", current=1, total=3)
                if not config.quiet:
                    logger.info("Step 1/3: Validating semantic models...")

                val_start = time.time()
                validation_result = self._run_validation(config)
                result.validation_time = time.time() - val_start

                result.validation_passed = validation_result.is_valid
                result.validation_errors = validation_result.error_count
                result.validation_warnings = validation_result.warning_count

                # Collect errors and warnings
                for issue in validation_result.issues:
                    if issue.severity.value == "error":
                        result.errors.append(f"[VALIDATION] {issue.message}")
                    elif issue.severity.value == "warning":
                        result.warnings.append(f"[VALIDATION] {issue.message}")

                if not validation_result.is_valid:
                    logger.error(f"Validation failed with {result.validation_errors} errors")
                    return result

                if not config.quiet:
                    logger.info(f"Validation passed ({result.validation_warnings} warnings)")
            else:
                result.validation_passed = True
                if not config.quiet:
                    logger.warning("Skipping validation (--skip-validation flag set)")

            # STEP 2: Extraction
            progress.stage("Extracting metadata to Snowflake", current=2, total=3)
            if not config.quiet:
                logger.info("Step 2/3: Extracting metadata to Snowflake...")

            ext_start = time.time()
            extraction_result = self._run_extraction(config, progress)
            result.extraction_time = time.time() - ext_start

            result.extraction_completed = extraction_result.success
            result.rows_loaded = extraction_result.rows_loaded
            result.models_processed = extraction_result.models_processed

            # Collect extraction errors/warnings
            if extraction_result.errors:
                result.errors.extend([f"[EXTRACTION] {e}" for e in extraction_result.errors])
            if extraction_result.warnings:
                result.warnings.extend([f"[EXTRACTION] {w}" for w in extraction_result.warnings])

            if not extraction_result.success:
                logger.error("Extraction failed")
                return result

            if not config.quiet:
                logger.info(f"Extraction completed ({result.rows_loaded:,} rows from {result.models_processed} models)")

            # STEP 3: Generation
            progress.stage("Generating semantic artifacts", current=3, total=3)
            if not config.quiet:
                logger.info("Step 3/3: Generating semantic artifacts...")

            gen_start = time.time()
            generation_result = self._run_generation(config, progress)
            result.generation_time = time.time() - gen_start

            result.generation_completed = generation_result.success
            result.views_created = generation_result.views_created

            # Collect generation errors
            if generation_result.errors:
                result.errors.extend([f"[GENERATION] {e}" for e in generation_result.errors])

            if not generation_result.success:
                logger.error("Generation failed")
                return result

            if not config.quiet:
                logger.info(f"Generation completed ({result.views_created} views)")

            # All steps succeeded
            result.success = True
            result.total_time = time.time() - start_time

            return result

        except Exception as e:
            logger.error(f"Deployment failed with exception: {e}")
            result.errors.append(f"[FATAL] {str(e)}")
            result.total_time = time.time() - start_time
            return result

    def _run_validation(self, config: DeployConfig):
        """Run validation step with proper exclusion handling."""
        service = SemanticMetadataCollectionValidationService.create_from_config()

        # Use reusable utility to get exclusion patterns (same as validate command)
        exclude_dirs = get_exclusion_patterns()

        validate_config = ValidateConfig(
            dbt_path=None,
            semantic_path=None,
            strict_mode=is_strict_mode(),  # Respect strict mode from config
            exclude_dirs=exclude_dirs,
        )

        return service.execute(validate_config, verbose=config.verbose)

    def _run_extraction(self, config: DeployConfig, progress: ProgressCallback):
        """Run extraction step with progress reporting."""
        service = SemanticMetadataExtractionService.create_from_config(self.snowflake_config)

        extract_config = ExtractConfig(
            database=config.database, schema=config.schema, dbt_path=None, semantic_path=None
        )

        return service.execute(extract_config, progress_callback=progress)

    def _run_generation(self, config: DeployConfig, progress: ProgressCallback):
        """Run generation step with progress reporting."""
        service = SemanticViewGenerationService(self.snowflake_config)

        gen_config = UnifiedGenerationConfig(
            metadata_database=config.database,
            metadata_schema=config.schema,
            target_database=config.database,  # Same as metadata
            target_schema=config.schema,  # Same as metadata
            views_to_generate=None,  # Generate all views
        )

        return service.generate(gen_config, progress_callback=progress)
