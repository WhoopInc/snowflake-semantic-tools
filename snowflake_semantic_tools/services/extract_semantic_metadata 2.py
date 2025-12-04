"""
Extract Semantic Metadata Service

End-to-end orchestration of semantic metadata extraction from source to Snowflake.

Implements the complete extraction pipeline that transforms YAML definitions
into production-ready semantic metadata tables. This is the foundation service
that populates all metadata required for semantic view and model generation.

The service coordinates:
- Repository access (Git clone/update or local paths)
- Multi-pass parsing with template resolution
- Comprehensive validation to ensure data quality
- Atomic data loading to Snowflake
- Cortex Search Service configuration

Designed for both interactive use and CI/CD automation with detailed
progress reporting and error handling.
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from snowflake_semantic_tools.core.parsing import Parser
from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeClient, SnowflakeConfig
from snowflake_semantic_tools.shared.events import ExtractionCompleted, ExtractionStarted, fire_event
from snowflake_semantic_tools.shared.progress import NoOpProgressCallback, ProgressCallback
from snowflake_semantic_tools.shared.utils import get_logger
from snowflake_semantic_tools.shared.utils.file_utils import find_dbt_model_files, find_semantic_model_files

logger = get_logger("extract_semantic_metadata")


@dataclass
class ExtractConfig:
    """Configuration for metadata extraction."""

    database: str
    schema: str
    dbt_path: Optional[Path] = None  # Optional override for dbt models directory
    semantic_path: Optional[Path] = None  # Optional override for semantic models directory
    enable_template_resolution: bool = True


@dataclass
class ExtractResult:
    """Result of metadata extraction."""

    success: bool
    rows_loaded: int  # Total rows loaded across all tables
    models_processed: int  # Actual dbt model count
    errors: list
    warnings: list
    table_counts: list = field(default_factory=list)
    search_service_status: str = "Not attempted"

    def print_summary(self):
        """Print comprehensive extraction summary."""
        print("\n" + "=" * 80)
        print("EXTRACTION SUMMARY")
        print("=" * 80)

        if self.success:
            print(f"Status: COMPLETED")
            print(f"Result: Successfully loaded {self.rows_loaded:,} rows from {self.models_processed} models")
        else:
            print(f"Status: FAILED")
            print(f"Result: Extraction failed with {len(self.errors)} errors")

        print()

        if self.table_counts:
            print("Tables Loaded:")
            for tc in self.table_counts:
                print(f"  {tc}")

        # Show search service status prominently
        print(f"\nCortex Search Service: {self.search_service_status}")

        if self.warnings:
            validation_warnings = sum(1 for w in self.warnings if "[VALIDATION]" in w)
            other_warnings = len(self.warnings) - validation_warnings

            print(f"\nWarnings: {len(self.warnings)} total")
            if validation_warnings > 0:
                print(f"  • Validation: {validation_warnings}")
            if other_warnings > 0:
                print(f"  • Other: {other_warnings}")

        if not self.success and self.errors:
            print(f"\nErrors ({len(self.errors)}):")
            for error in self.errors[:5]:  # Show first 5 errors
                print(f"  • {error}")
            if len(self.errors) > 5:
                print(f"  ... and {len(self.errors) - 5} more errors")

        print("=" * 80)


class SemanticMetadataExtractionService:
    """
    Orchestrates the complete semantic metadata extraction pipeline.

    Transforms YAML-based semantic model definitions into structured
    metadata tables in Snowflake, enabling semantic view generation
    and AI-powered analytics through Cortex Analyst.

    Pipeline Steps:
    1. **Repository Access**: Clone remote repos or use local paths
    2. **File Discovery**: Find all dbt and semantic model YAML files
    3. **Parsing**: Two-pass parsing with template resolution
    4. **Validation**: Multi-layer validation ensuring data quality
    5. **Loading**: Atomic bulk loading to Snowflake tables
    6. **Search Service**: Configure Cortex Search for AI understanding

    The service ensures data consistency through comprehensive validation
    and atomic operations, making it safe for production deployments.
    """

    def __init__(self, parser: Parser, snowflake_client: SnowflakeClient):
        """
        Initialize the service.

        Args:
            parser: Parser for YAML files
            snowflake_client: Client for Snowflake operations
        """
        self.parser = parser
        self.snowflake = snowflake_client

    @classmethod
    def create_from_config(cls, snowflake_config: SnowflakeConfig):
        """
        Create service from configuration.

        Args:
            snowflake_config: Snowflake configuration

        Returns:
            Configured service instance
        """
        from snowflake_semantic_tools.core.parsing import Parser
        from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeClient

        snowflake_client = SnowflakeClient(snowflake_config)
        parser = Parser()

        return cls(parser=parser, snowflake_client=snowflake_client)

    def execute(self, config: ExtractConfig, progress_callback: Optional[ProgressCallback] = None) -> ExtractResult:
        """
        Execute the metadata extraction workflow.

        Args:
            config: Extraction configuration
            progress_callback: Optional callback for progress reporting

        Returns:
            Extraction result with status and details
        """
        # Use no-op callback if none provided (keeps code clean, no None checks)
        progress = progress_callback or NoOpProgressCallback()

        # Event fires at service level, this is just technical log
        logger.debug("Starting metadata extraction service")
        progress.stage("Extracting semantic metadata to Snowflake")

        # NEW: Try to load manifest for auto-detection
        progress.info("Loading manifest...")
        try:
            from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

            manifest_parser = ManifestParser()
            if manifest_parser.load():
                model_count = len(manifest_parser.model_locations) if manifest_parser.model_locations else 0
                logger.debug(f"Loaded manifest with {model_count} models")
                progress.detail(f"Loaded manifest with {model_count} models")
                self.parser.manifest_parser = manifest_parser
            else:
                logger.debug("No manifest found - database/schema must be in YAML")
                progress.detail("No manifest found - using YAML metadata")
        except Exception as e:
            logger.debug(f"Failed to load manifest: {e}. Continuing without manifest auto-detection.")
            progress.detail("Manifest not available - using YAML metadata")
            # Continue extraction without manifest - models must have database/schema in YAML

        # Configure parser with target database for environment-aware extraction
        self.parser.target_database = config.database
        logger.debug(f"Using target database '{config.database}' for table references")

        errors = []
        warnings = []
        total_rows_loaded = 0
        models_processed_count = 0

        try:
            # Step 1: Find files
            progress.info("Scanning for YAML files...")

            if config.dbt_path:
                dbt_path = config.dbt_path if config.dbt_path.is_absolute() else Path.cwd() / config.dbt_path
                from snowflake_semantic_tools.shared.utils.file_utils import _is_dbt_model_file

                all_yml_files = list(dbt_path.rglob("*.yml")) + list(dbt_path.rglob("*.yaml"))
                dbt_files = [f for f in all_yml_files if _is_dbt_model_file(f)]
            else:
                dbt_files = find_dbt_model_files()

            if config.semantic_path:
                semantic_path = (
                    config.semantic_path if config.semantic_path.is_absolute() else Path.cwd() / config.semantic_path
                )
                semantic_files = list(semantic_path.rglob("*.yml")) + list(semantic_path.rglob("*.yaml"))
            else:
                semantic_files = find_semantic_model_files()

            # Track actual model count for accurate reporting
            models_processed_count = len(dbt_files)

            logger.debug(f"Found {len(dbt_files)} dbt files and {len(semantic_files)} semantic files")
            progress.info(f"Found {len(dbt_files)} dbt models and {len(semantic_files)} semantic files", indent=1)

            # Step 3: Parse files
            progress.blank_line()
            progress.info("Parsing YAML files...")
            parse_result = self.parser.parse_all_files(dbt_files, semantic_files)
            progress.detail(f"Parsed {len(dbt_files) + len(semantic_files)} files")

            # Collect warnings and errors
            if "metadata" in parse_result:
                errors.extend(parse_result["metadata"].get("errors", []))

            # Extract warnings from semantic parsing
            if "semantic" in parse_result:
                for component_type, component_data in parse_result["semantic"].items():
                    if isinstance(component_data, dict) and "warnings" in component_data:
                        warnings.extend(component_data["warnings"])

            # Step 4: Prepare data for loading
            # Note: Validation is a separate concern - run 'sst validate' before extraction
            # Extract focuses solely on parsing YAML and loading to Snowflake
            progress.blank_line()
            progress.info("Preparing data for Snowflake...")
            logger.debug("Starting data preparation for Snowflake loading")
            models_to_load = self._prepare_models_for_loading(parse_result)
            logger.debug(f"Prepared {len(models_to_load)} model types for loading")

            # Step 5: Load to Snowflake
            if models_to_load:
                table_counts = []
                total_rows = 0
                for key, data in models_to_load.items():
                    count = (
                        len(data) if isinstance(data, list) else (len(data) if isinstance(data, pd.DataFrame) else 0)
                    )
                    if count > 0:
                        table_counts.append(f"{key}: {count} rows")
                        total_rows += count

                logger.debug(f"Loading {total_rows} total rows across {len(models_to_load)} tables")

                # Show progress for data loading
                progress.blank_line()
                progress.info(f"Loading {total_rows:,} rows to Snowflake...")
                progress.detail(f"Target: {config.database}.{config.schema}")

                # Show table breakdown in verbose mode
                for i, tc in enumerate(table_counts, 1):
                    progress.detail(f"Table {i}/{len(table_counts)}: {tc}")

                try:
                    progress.blank_line()
                    progress.info("Connecting to Snowflake and loading data...")
                    load_start = time.time()

                    success = self.snowflake.data_loader.load_semantic_models(
                        models_to_load, config.database, config.schema
                    )

                    load_duration = time.time() - load_start
                    if success:
                        progress.info(f"Data loaded successfully", indent=1)
                        progress.detail(f"Load time: {load_duration:.1f}s")
                except Exception as load_error:
                    # Catch connection/auth errors and provide clear message
                    error_msg = str(load_error)
                    if "differs from the user currently logged in" in error_msg:
                        errors.append(
                            "Snowflake SSO authentication mismatch - the user authenticated in browser doesn't match SNOWFLAKE_USER in .env. Either update .env or log out of Okta and try again."
                        )
                    elif "250001" in error_msg or "Failed to connect" in error_msg:
                        errors.append(f"Failed to connect to Snowflake: {error_msg.split(':', 1)[-1].strip()}")
                    else:
                        errors.append(f"Failed to load models to Snowflake: {error_msg}")

                    logger.error(f"Load failed: {error_msg}")
                    success = False

                if success:
                    # Calculate total rows loaded (this is what was called "models_extracted" before)
                    total_rows_loaded = sum(
                        len(data) if isinstance(data, list) else 1 for data in models_to_load.values()
                    )
                    logger.debug(
                        f"Successfully loaded {total_rows_loaded} rows from {models_processed_count} models to Snowflake"
                    )

                    # Step 6: Setup Cortex Search Service for table summaries
                    progress.blank_line()
                    progress.info("Configuring Cortex Search Service...")
                    logger.debug("Setting up Cortex Search Service for table summaries")
                    try:
                        search_result = self.snowflake.cortex_search_manager.setup_search_service(
                            tables_were_swapped=True
                        )
                        if search_result.get("success"):
                            logger.debug(
                                f"Cortex Search Service '{search_result.get('service_name')}' ready ({search_result.get('operation')})"
                            )
                            progress.info(f"Cortex Search Service ready ({search_result.get('operation')})", indent=1)
                            search_service_status = "Ready"
                        else:
                            # Don't add to warnings if it's just that there's no data to index
                            error_detail = search_result.get("error", "")
                            if "no rows" in error_detail.lower() or "empty" in error_detail.lower():
                                logger.debug("Cortex Search Service skipped (no data to index)")
                                progress.detail("Skipped (no data to index)")
                                search_service_status = "Skipped (no data)"
                            else:
                                warning_msg = f"Cortex Search Service setup failed: {error_detail}"
                                logger.warning(warning_msg)
                                progress.warning(f"Cortex Search setup failed: {error_detail}")
                                warnings.append(warning_msg)
                                search_service_status = "Failed"
                    except Exception as e:
                        warning_msg = f"Could not setup Cortex Search Service: {e}"
                        logger.warning(warning_msg)
                        progress.warning(f"Cortex Search setup error: {str(e)}")
                        warnings.append(warning_msg)
                        search_service_status = "Failed"
                        # Don't fail the extraction if search service setup fails
                else:
                    errors.append("Failed to load models to Snowflake")
            else:
                logger.warning("No models to load to Snowflake")

            # Use the search service status captured during execution
            if "search_service_status" not in locals():
                search_service_status = "Not attempted"

            result = ExtractResult(
                success=len(errors) == 0,
                rows_loaded=total_rows_loaded,
                models_processed=models_processed_count,
                errors=errors,
                warnings=warnings,
                # Only show table_counts if load actually succeeded
                table_counts=table_counts if "table_counts" in locals() and success else [],
                search_service_status=search_service_status,
            )

            # Summary will be printed by CLI command

            return result

        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            errors.append(str(e))

            return ExtractResult(success=False, rows_loaded=0, models_processed=0, errors=errors, warnings=warnings)

    def _prepare_models_for_loading(self, parse_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare parsed models for loading to Snowflake.
        Filters out tables with missing critical metadata.

        Args:
            parse_result: Result from parser

        Returns:
            Dictionary of models ready for loading
        """
        models = {}
        skipped_tables = []

        # Extract semantic models
        if "semantic" in parse_result:
            for model_type, model_data in parse_result["semantic"].items():
                if isinstance(model_data, dict):
                    # Handle items
                    if "items" in model_data:
                        models[model_type] = model_data["items"]
                    # Handle relationship_columns separately
                    if "relationship_columns" in model_data:
                        models["relationship_columns"] = model_data["relationship_columns"]
                elif isinstance(model_data, list):
                    models[model_type] = model_data

        # Extract dbt metadata (dimensions, facts, time dimensions, tables, etc.)
        if "dbt" in parse_result:
            dbt_data = parse_result["dbt"]

            # Filter sm_tables - environment-agnostic filtering
            # With manifest-based auto-detection, database/schema are now optional
            if "sm_tables" in dbt_data and dbt_data["sm_tables"]:
                filtered_tables = []
                for table in dbt_data["sm_tables"]:
                    table_name = table.get("table_name", "unknown")

                    # Only skip tables that are completely missing table_name
                    # All other metadata (database, schema, primary_key) can be inferred or handled later
                    if not table.get("table_name"):
                        skipped_tables.append(table_name)
                        logger.debug(f"Skipping table '{table_name}' from extraction due to missing table_name")
                        continue

                    # Log warnings for missing metadata but don't skip the table
                    # These will be handled by manifest inference or fallback detection
                    if not table.get("database"):
                        logger.debug(
                            f"Table '{table_name}' missing database metadata - will use manifest/target_database"
                        )
                    if not table.get("schema"):
                        logger.debug(
                            f"Table '{table_name}' missing schema metadata - will use manifest/target_database"
                        )
                    if not table.get("primary_key"):
                        logger.warning(f"Table '{table_name}' missing primary_key metadata - relationships may fail")

                    filtered_tables.append(table)

                if filtered_tables:
                    models["sm_tables"] = filtered_tables

                # Include all dimensions, facts, time_dimensions (no filtering based on skipped tables)
                # Since we're now more lenient with table metadata, we should include all column metadata
                for key in ["sm_dimensions", "sm_time_dimensions", "sm_facts"]:
                    if key in dbt_data and dbt_data[key]:
                        models[key] = dbt_data[key]

            # Add other metadata that doesn't need filtering
            for key in ["sm_relationship_columns", "sm_table_summaries", "sm_semantic_views"]:
                if key in dbt_data and dbt_data[key]:
                    models[key] = dbt_data[key]

        # Log skipped tables if any
        if skipped_tables:
            logger.warning(f"Skipped {len(skipped_tables)} table(s) from extraction due to missing table_name")

        return models
