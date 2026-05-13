"""
Metadata Enrichment Service

Orchestrates the complete enrichment workflow including:
- Model discovery
- Snowflake connection management
- Metadata enrichment
- Result aggregation
"""

import glob
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from snowflake_semantic_tools.core.enrichment import MetadataEnricher, PrimaryKeyValidator, YAMLHandler
from snowflake_semantic_tools.core.parsing.parsers.data_extractors import get_sst_meta
from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeClient

# Events: User-facing output → CLI terminal + logs/sst_events.log
from snowflake_semantic_tools.shared.events import (
    EnrichmentCompleted,
    EnrichmentStarted,
    ModelEnriched,
    ModelEnrichmentSkipped,
    fire_event,
)
from snowflake_semantic_tools.shared.progress import NoOpProgressCallback, ProgressCallback

# Logger: Technical debugging → logs/sst.log (NOT shown to users)
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


@dataclass
class EnrichmentConfig:
    """Configuration for metadata enrichment."""

    target_path: Optional[str] = None  # Directory or file path (optional if model_files provided)
    model_files: Optional[List[str]] = None  # Explicit list of model file paths (from --models)
    database: Optional[str] = None  # Optional: Can be auto-detected from manifest
    schema: Optional[str] = None  # Optional: Can be auto-detected from manifest
    primary_key_candidates: Optional[Dict[str, List[List[str]]]] = None
    excluded_dirs: Optional[List[str]] = None
    dry_run: bool = False
    fail_fast: bool = False
    manifest_path: Optional[Path] = None  # Override manifest location
    require_prod_target: bool = True  # Warn if not prod target
    allow_non_prod: bool = False  # Allow non-prod manifest

    # dbt profile target (e.g., 'dev', 'prod')
    dbt_target: Optional[str] = None

    # NEW: Component flags for modular enrichment
    components: Optional[List[str]] = None  # Which components to enrich
    enrich_all: bool = False  # Shorthand for all components
    enrich_synonyms: bool = False  # Shorthand for table + column synonyms

    # NEW: Force flags to override preservation defaults
    force_synonyms: bool = False  # Overwrite existing synonyms
    force_column_types: bool = False  # Overwrite existing column types
    force_data_types: bool = False  # Overwrite existing data types
    force_all: bool = False  # Overwrite everything

    # Source enrichment flags
    include_sources: bool = False  # Also enrich dbt sources alongside models
    sources_only: bool = False  # Enrich only sources, skip models
    source_names: Optional[List[str]] = None  # Specific "source_name.table_name" selectors

    def __post_init__(self):
        """Validate that either target_path, model_files, or source flags are provided."""
        if not self.target_path and not self.model_files and not self.sources_only and not self.source_names:
            raise ValueError("Either target_path, model_files, sources_only, or source_names must be provided")


@dataclass
class EnrichmentResult:
    """Result of enrichment operation."""

    status: str  # 'complete', 'partial', 'failed'
    processed: int
    total: int
    results: List[Dict[str, Any]]
    errors: List[Dict[str, Any]]

    def print_summary(self):
        """Print human-readable summary (deprecated - use events instead)."""
        # Summary is now handled by EnrichmentCompleted event
        # Keep this method for backwards compatibility but it does nothing
        pass


class MetadataEnrichmentService:
    """
    Service for enriching dbt YAML metadata.

    Orchestrates the complete enrichment workflow including:
    - Model discovery
    - Snowflake connection management
    - Metadata enrichment
    - Result aggregation
    """

    def __init__(self, config: EnrichmentConfig):
        """
        Initialize service with configuration.

        Args:
            config: Enrichment configuration
        """
        self.config = config
        self.snowflake_client = None
        self.enricher = None
        self.manifest_parser = None  # NEW: For auto-detection

    def connect(self, session=None):
        """
        Establish Snowflake connection using dbt profiles.yml.

        Args:
            session: Optional existing Snowflake session
        """
        if session:
            self.snowflake_client = SnowflakeClient.from_session(session)
        else:
            from snowflake_semantic_tools.infrastructure.snowflake.config import SnowflakeConfig

            # Load config from dbt profiles.yml
            # Use INFORMATION_SCHEMA as default database/schema for enrichment
            # since we use fully qualified names in all queries
            config = SnowflakeConfig.from_dbt_profile(
                target=self.config.dbt_target,
                database_override="INFORMATION_SCHEMA",
                schema_override="TABLES",
            )

            logger.info(f"Using dbt profile: {config.profile_name} (target: {config.target_name})")
            self.snowflake_client = SnowflakeClient(config)

        # Show configuration to user
        import click

        # Format target info (may be from explicit config or will be from manifest)
        if self.config.database and self.config.schema:
            target_info = f"{self.config.database}.{self.config.schema}"
        else:
            target_info = "auto-detect from manifest"

        # Show profile info
        profile_info = f"{config.profile_name}.{config.target_name}" if config.profile_name else "unknown"
        click.echo(f"Configured for Snowflake: {config.account} (profile: {profile_info})")
        click.echo(f"Target database/schema: {target_info}")
        click.echo("Connecting to Snowflake...")

        # Test connection immediately to catch auth errors early
        try:
            test_query = "SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_ROLE()"
            result = self.snowflake_client.execute_query(test_query)
            if not result.empty:
                current_user = result.iloc[0, 0]
                click.echo(f"Connected successfully as: {current_user}")
        except Exception as conn_error:
            error_msg = str(conn_error)
            if "differs from the user currently logged in" in error_msg:
                click.echo("\nERROR: SSO authentication mismatch", err=True)
                click.echo("The user in your browser session doesn't match the user in profiles.yml", err=True)
                click.echo(f"Expected: {config.user}", err=True)
                click.echo("\nTry: Log out of Okta, then run this command again", err=True)
            raise

        # NEW: Try to load manifest for auto-detection
        from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

        self.manifest_parser = ManifestParser(self.config.manifest_path)
        manifest_loaded = self.manifest_parser.load()

        if manifest_loaded:
            target = self.manifest_parser.get_target_name()
            is_prod = self.manifest_parser.is_prod_target()

            click.echo(f"Found dbt manifest (target: {target})")
            logger.info(f"Loaded manifest from: {self.manifest_parser.manifest_path}")

            # Warn if not production target and user cares
            if self.config.require_prod_target and not is_prod and not self.config.allow_non_prod:
                click.echo(f"\nWARNING: Manifest compiled for '{target}' target (not prod)", err=True)
                click.echo(f"  Enrichment will use {target} database locations", err=True)
                click.echo(f"  Recommendation: Run 'dbt compile --target prod' for production locations", err=True)
                click.echo(f"  Or use --allow-non-prod flag to suppress this warning\n", err=True)

                # Give user time to see the warning
                import time

                time.sleep(1)
        else:
            # No manifest found
            if not self.config.database or not self.config.schema:
                # Fatal: Need either manifest or explicit flags
                click.echo("\nERROR: No dbt manifest.json found\n", err=True)
                click.echo("SST needs a compiled manifest to auto-detect database and schema.\n", err=True)
                click.echo("Solutions:", err=True)
                click.echo("  1. Generate manifest:", err=True)
                click.echo("     dbt compile --target prod\n", err=True)
                click.echo("  2. Or provide explicit flags:", err=True)
                click.echo(
                    f"     sst enrich {self.config.target_path} --database ANALYTICS --schema your_schema\n", err=True
                )
                click.echo("Where SST looks for manifest:", err=True)
                click.echo("  - ./target/manifest.json", err=True)
                click.echo("  - ./target_prod/manifest.json", err=True)
                click.echo("  - Specify with: --manifest /path/to/manifest.json\n", err=True)

                raise click.ClickException("No manifest found and no explicit database/schema provided")
            else:
                # Warning: using explicit flags without manifest
                click.echo("WARNING: No manifest found - using explicit database/schema from CLI flags")
                logger.info("Proceeding with explicit database/schema (no manifest)")

        # Initialize enrichment components
        # Note: We'll determine database/schema per-model in enrich() method
        yaml_handler = YAMLHandler()
        pk_validator = PrimaryKeyValidator(self.snowflake_client)

        # Use explicit config if provided, otherwise will be determined per-model
        default_database = self.config.database or "PENDING"  # Will be resolved per-model
        default_schema = self.config.schema or "PENDING"  # Will be resolved per-model

        self.enricher = MetadataEnricher(
            self.snowflake_client,
            yaml_handler,
            pk_validator,
            default_database=default_database,
            default_schema=default_schema,
        )

        # NEW: Pass all force flags to enricher
        self.enricher.force_synonyms = self.config.force_synonyms
        self.enricher.force_column_types = self.config.force_column_types
        self.enricher.force_data_types = self.config.force_data_types

        # Pass project path for table synonym deduplication
        self.enricher.project_path = self.config.target_path

        logger.info("Connected to Snowflake for metadata enrichment")
        if self.config.database and self.config.schema:
            logger.info(f"Target Database: {self.config.database}")
            logger.info(f"Target Schema: {self.config.schema}")
        else:
            logger.info("Target Database/Schema: Will be auto-detected per model from manifest")

    def _resolve_location(self, model_file: str) -> Dict[str, str]:
        """
        Resolve database and schema for a model file.

        Uses resolution hierarchy:
        1. Explicit CLI flags (--database/--schema) - Override everything
        2. YAML metadata (meta.sst) - User's explicit choice
        3. Manifest.json - Auto-detect if YAML doesn't specify
        4. Error with clear guidance

        Args:
            model_file: Path to model SQL file

        Returns:
            Dict with 'database', 'schema', and 'source' keys

        Raises:
            ValueError: If location cannot be determined
        """
        model_path = Path(model_file)
        model_name = model_path.stem

        # Priority 1: Explicit CLI flags (override everything)
        if self.config.database and self.config.schema:
            logger.debug(f"Using explicit CLI flags for {model_name}: {self.config.database}.{self.config.schema}")
            return {"database": self.config.database, "schema": self.config.schema, "source": "cli"}

        # Priority 2: YAML metadata (user's explicit choice)
        yaml_file = model_path.with_suffix(".yml")
        if yaml_file.exists():
            import yaml as yaml_lib

            try:
                with open(yaml_file) as f:
                    yaml_content = yaml_lib.safe_load(f)

                models = yaml_content.get("models", [])
                if models:
                    # Match model by name instead of assuming first model
                    matching_model = None
                    for m in models:
                        if m.get("name") == model_name:
                            matching_model = m
                            break

                    # Fall back to first model if no match found (for backward compat)
                    model = matching_model if matching_model else models[0]

                    # Use get_sst_meta utility (supports config.meta.sst and meta.sst)
                    sst = get_sst_meta(model, node_type="model", node_name=model_name)

                    database = sst.get("database")
                    schema = sst.get("schema")

                    if database and schema:
                        logger.debug(f"Using YAML metadata for {model_name}: {database}.{schema}")
                        return {"database": database.upper(), "schema": schema.upper(), "source": "yaml"}
            except Exception as e:
                logger.warning(f"Error reading YAML for {model_name}: {e}")

        # Priority 3: Manifest (auto-detect if YAML doesn't specify)
        if self.manifest_parser and self.manifest_parser.manifest:
            location = self.manifest_parser.get_location_by_path(model_path)
            if location:
                logger.debug(
                    f"Auto-detected from manifest for {model_name}: {location['database']}.{location['schema']}"
                )
                return {"database": location["database"], "schema": location["schema"], "source": "manifest"}
            else:
                # Model not in manifest - provide helpful error
                error_msg = (
                    f"\nERROR: Model '{model_name}' not found in manifest\n\n"
                    f"The model exists but isn't in target/manifest.json yet.\n\n"
                    f"Solutions:\n"
                    f"  1. Compile this model:\n"
                    f"     dbt compile --select {model_name} --target prod\n\n"
                    f"  2. Or compile all models:\n"
                    f"     dbt compile --target prod\n\n"
                    f"  3. Or add to YAML:\n"
                    f"     meta:\n"
                    f"       sst:\n"
                    f"         database: DATABASE_NAME\n"
                    f"         schema: SCHEMA_NAME\n\n"
                    f"  4. Or use explicit flags:\n"
                    f"     sst enrich {model_file} --database DB --schema SCHEMA\n\n"
                    f"Model file: {model_file}\n"
                )
                raise ValueError(error_msg)

        # Priority 4: Error - cannot determine location
        error_msg = (
            f"\nERROR: Cannot determine database/schema for '{model_name}'\n\n"
            f"Solutions:\n"
            f"  1. Generate manifest:\n"
            f"     dbt compile --target prod\n"
            f"     sst enrich {model_file}\n\n"
            f"  2. Add to model YAML:\n"
            f"     meta:\n"
            f"       sst:\n"
            f"         database: YOUR_DATABASE\n"
            f"         schema: YOUR_SCHEMA\n\n"
            f"  3. Use explicit flags:\n"
            f"     sst enrich {model_file} --database DB --schema SCHEMA\n\n"
            f"Model file: {model_file}\n"
        )
        raise ValueError(error_msg)

    def enrich(self, progress_callback: Optional[ProgressCallback] = None) -> EnrichmentResult:
        """
        Execute enrichment workflow with auto-detection support.

        Args:
            progress_callback: Optional callback for progress reporting

        Returns:
            EnrichmentResult with summary and details
        """
        if not self.snowflake_client:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        # Use no-op callback if none provided
        progress = progress_callback or NoOpProgressCallback()

        # Log enrichment configuration (technical details for debugging)
        # Note: User-facing progress shown via events below (EnrichmentStarted, ModelEnriched, etc.)
        target_desc = self.config.target_path or f"{len(self.config.model_files or [])} model(s)"
        logger.info(f"Starting metadata enrichment for {target_desc}")
        progress.stage("Enriching metadata from Snowflake")

        if self.config.database and self.config.schema:
            logger.info(f"Target Database: {self.config.database}, Schema: {self.config.schema}")
        else:
            logger.info("Target Database/Schema: Auto-detecting per model from manifest")

        if self.config.excluded_dirs:
            logger.info(f"Excluded directories: {', '.join(self.config.excluded_dirs)}")

        if self.config.dry_run:
            logger.info("DRY RUN mode: no files will be modified")

        # Discover models (skip if sources_only)
        model_files = []
        if not self.config.sources_only and not self.config.source_names:
            if self.config.model_files:
                progress.info(f"Using {len(self.config.model_files)} specified model(s)...")
            elif self.config.target_path:
                progress.info(f"Discovering models in {self.config.target_path}...")
            logger.debug("Discovering models...")
            model_files = self._discover_models()

        # Discover sources
        source_tables = []
        if self.config.include_sources or self.config.sources_only or self.config.source_names:
            progress.info("Discovering dbt sources...")
            source_tables = self._discover_sources()
            if source_tables:
                progress.info(f"Found {len(source_tables)} source table(s) to enrich", indent=1)

        total_items = len(model_files) + len(source_tables)

        if total_items == 0:
            target_desc = self.config.target_path or "specified targets"
            logger.warning(f"No models or sources found at {target_desc}")
            progress.warning(f"No models or sources found at {target_desc}")
            return EnrichmentResult(status="complete", processed=0, total=0, results=[], errors=[])

        if model_files:
            progress.info(f"Found {len(model_files)} model(s) to enrich", indent=1)

        # Fire event: User-facing operation start (shown in CLI)
        event_path = self.config.target_path or "specified targets"
        fire_event(EnrichmentStarted(path=event_path, model_count=total_items))

        # Show enrichment details
        progress.blank_line()
        parts = []
        if model_files:
            parts.append(f"{len(model_files)} model(s)")
        if source_tables:
            parts.append(f"{len(source_tables)} source(s)")
        progress.info(f"Enriching {' and '.join(parts)}...")

        # Start timing
        start_time = time.time()

        # Process models
        results = []
        errors = []
        success_count = 0
        skipped_count = 0

        for idx, model_file in enumerate(model_files, 1):
            try:
                model_start = time.time()
                model_name = Path(model_file).stem

                if self.config.dry_run:
                    fire_event(
                        ModelEnrichmentSkipped(
                            model_name=model_name, reason="dry run", current=idx, total=len(model_files)
                        )
                    )
                    results.append({"status": "dry_run", "model": model_file})
                    skipped_count += 1
                    continue

                # NEW: Resolve database/schema for this model
                try:
                    location = self._resolve_location(model_file)

                    # Technical log: Database/schema resolution details (not shown to user)
                    # User sees progress via events: ModelEnriched, ModelEnrichmentSkipped
                    logger.debug(
                        f"[{idx}/{len(model_files)}] {model_name}: {location['database']}.{location['schema']} (from {location['source']})"
                    )

                except ValueError as loc_error:
                    # Location resolution failed - skip this model with clear error
                    error_result = {"status": "error", "model": model_file, "error": str(loc_error)}
                    errors.append(error_result)
                    results.append(error_result)

                    fire_event(
                        ModelEnrichmentSkipped(
                            model_name=model_name,
                            reason=f"Cannot determine location (see logs)",
                            current=idx,
                            total=len(model_files),
                        )
                    )

                    logger.error(str(loc_error))

                    if self.config.fail_fast:
                        logger.error("Fail-fast enabled, stopping on error")
                        break

                    continue  # Skip to next model

                # Show [RUN] indicator
                progress.item_progress(idx, len(model_files), model_name, "RUN")

                # Enrich model with resolved location and components
                result = self.enricher.enrich_model(
                    model_file,
                    self.config.primary_key_candidates,
                    database=location["database"],
                    schema=location["schema"],
                    components=self.config.components,  # Pass components through
                )

                results.append(result)
                model_duration = time.time() - model_start

                if result["status"] == "success":
                    success_count += 1

                    # Event system will show the [OK in X.Xs] line (second line, green)
                    fire_event(
                        ModelEnriched(
                            model_name=model_name,
                            columns_updated=result.get("columns_processed", 0),
                            duration_seconds=model_duration,
                            current=idx,
                            total=len(model_files),
                        )
                    )
                else:
                    errors.append(result)
                    # Event system will show the [SKIP] line
                    # (Don't use progress callback to avoid duplication)

                    # Get error message and make it concise but informative
                    error_msg = result.get("error", "Unknown error")
                    # Truncate at first newline or 100 chars (not 50 - too short)
                    if "\n" in error_msg:
                        error_msg = error_msg.split("\n")[0]
                    if len(error_msg) > 100:
                        error_msg = error_msg[:97] + "..."

                    fire_event(
                        ModelEnrichmentSkipped(
                            model_name=model_name, reason=error_msg, current=idx, total=len(model_files)
                        )
                    )

                    if self.config.fail_fast:
                        logger.error(f"Fail-fast enabled, stopping on error: {result.get('error', 'Unknown error')}")
                        break

            except Exception as e:
                error_result = {"status": "error", "model": model_file, "error": f"{type(e).__name__}: {str(e)}"}
                errors.append(error_result)
                results.append(error_result)

                logger.error(f"Error processing {model_file}: {type(e).__name__}: {e}")
                logger.debug(f"Full traceback for {model_file}:", exc_info=True)

                if self.config.fail_fast:
                    logger.error("Fail-fast enabled, stopping on error")
                    break

        # Process sources
        for idx_offset, source_info in enumerate(source_tables):
            idx = len(model_files) + idx_offset + 1
            source_display = f"{source_info['source_name']}.{source_info['table_name']}"

            try:
                source_start = time.time()

                if self.config.dry_run:
                    fire_event(
                        ModelEnrichmentSkipped(
                            model_name=source_display, reason="dry run", current=idx, total=total_items
                        )
                    )
                    results.append({"status": "dry_run", "source": source_display})
                    skipped_count += 1
                    continue

                progress.item_progress(idx, total_items, source_display, "RUN")

                result = self.enricher.enrich_source(
                    source_info,
                    components=self.config.components,
                )

                results.append(result)
                source_duration = time.time() - source_start

                if result["status"] == "success":
                    success_count += 1
                    fire_event(
                        ModelEnriched(
                            model_name=source_display,
                            columns_updated=result.get("columns_processed", 0),
                            duration_seconds=source_duration,
                            current=idx,
                            total=total_items,
                        )
                    )
                else:
                    errors.append(result)
                    error_msg = result.get("error", "Unknown error")
                    if "\n" in error_msg:
                        error_msg = error_msg.split("\n")[0]
                    if len(error_msg) > 100:
                        error_msg = error_msg[:97] + "..."

                    fire_event(
                        ModelEnrichmentSkipped(
                            model_name=source_display, reason=error_msg, current=idx, total=total_items
                        )
                    )

                    if self.config.fail_fast:
                        logger.error(f"Fail-fast enabled, stopping on error: {result.get('error', 'Unknown error')}")
                        break

            except Exception as e:
                error_result = {"status": "error", "source": source_display, "error": f"{type(e).__name__}: {str(e)}"}
                errors.append(error_result)
                results.append(error_result)

                logger.error(f"Error processing source {source_display}: {type(e).__name__}: {e}")
                logger.debug(f"Full traceback for source {source_display}:", exc_info=True)

                if self.config.fail_fast:
                    logger.error("Fail-fast enabled, stopping on error")
                    break

        # Determine overall status
        if success_count == total_items:
            status = "complete"
        elif success_count > 0:
            status = "partial"
        elif skipped_count == total_items:
            status = "complete"
        else:
            status = "failed"

        result = EnrichmentResult(
            status=status, processed=success_count, total=total_items, results=results, errors=errors
        )

        # Fire event: Enrichment completed
        duration = time.time() - start_time
        fire_event(
            EnrichmentCompleted(
                total_models=total_items,
                successful=success_count,
                failed=len(errors),
                skipped=skipped_count,
                duration_seconds=duration,
            )
        )

        return result

    def _discover_models(self) -> List[str]:
        """
        Discover SQL model files to process.

        Supports multiple input formats:
        - Explicit model files list (from --models option)
        - Direct SQL file path: models/model.sql
        - Direct YAML file path: models/model.yml (finds corresponding .sql)
        - Directory: models/subdirectory/ (finds all .sql files)
        - Glob pattern: models/**/*.sql

        Exclusions support both simple names and glob patterns:
        - Simple: "_intermediate" excludes any dir with that name
        - Path: "models/amplitude/*" excludes specific path
        - Pattern: "analytics/*/staging/*" uses wildcards

        Returns:
            List of SQL model file paths
        """
        # If explicit model files provided (from --models), use them directly
        if self.config.model_files:
            logger.debug(f"Using explicit model files: {self.config.model_files}")
            return self.config.model_files

        # Otherwise, discover from target_path
        if not self.config.target_path:
            logger.warning("No target_path or model_files provided")
            return []

        target_path = self.config.target_path.rstrip("/")
        excluded_patterns = self.config.excluded_dirs or []

        model_files = []

        if target_path.endswith(".sql"):
            # Single SQL file
            if Path(target_path).exists():
                model_files.append(target_path)

        elif target_path.endswith(".yml") or target_path.endswith(".yaml"):
            # YAML file - find corresponding SQL file
            yaml_path = Path(target_path)
            if yaml_path.exists():
                # Look for SQL file with same name in same directory
                sql_path = yaml_path.with_suffix(".sql")
                if sql_path.exists():
                    model_files.append(str(sql_path))
                    logger.debug(f"Found SQL file for YAML: {sql_path}")
                else:
                    logger.warning(f"No corresponding SQL file found for {target_path}")

        elif Path(target_path).is_dir():
            # Directory - find all SQL files recursively
            import fnmatch

            for root, dirs, files in os.walk(target_path):
                # Skip excluded directories (supports both simple names and patterns)
                filtered_dirs = []
                for d in dirs:
                    dir_path = os.path.relpath(os.path.join(root, d), target_path)
                    should_exclude = False

                    for pattern in excluded_patterns:
                        # If pattern has path separators or wildcards, use glob matching
                        if "/" in pattern or "*" in pattern:
                            # Glob pattern - match against relative path
                            pattern_normalized = pattern.rstrip("/*")
                            if fnmatch.fnmatch(dir_path, pattern_normalized) or fnmatch.fnmatch(dir_path, pattern):
                                should_exclude = True
                                break
                        else:
                            # Simple directory name - check if it matches
                            if d == pattern:
                                should_exclude = True
                                break

                    if not should_exclude:
                        filtered_dirs.append(d)

                dirs[:] = filtered_dirs

                for file in files:
                    if file.endswith(".sql"):
                        file_path = os.path.join(root, file)
                        rel_path = os.path.relpath(file_path, target_path)

                        # Check if file path matches any exclusion pattern
                        should_exclude = False
                        for pattern in excluded_patterns:
                            if fnmatch.fnmatch(rel_path, pattern):
                                should_exclude = True
                                break

                        if not should_exclude:
                            model_files.append(file_path)

        else:
            # Try as glob pattern
            import fnmatch

            matching_files = glob.glob(target_path + "/**/*.sql", recursive=True)
            for file_path in matching_files:
                rel_path = os.path.relpath(file_path, Path(target_path).parent)
                should_exclude = False

                for pattern in excluded_patterns:
                    if fnmatch.fnmatch(rel_path, pattern):
                        should_exclude = True
                        break

                if not should_exclude:
                    model_files.append(file_path)

        return sorted(model_files)

    def _discover_sources(self) -> List[Dict[str, Any]]:
        """
        Discover dbt source tables to process.

        Returns a list of dicts with keys:
            source_name, table_name, database, schema, yaml_path

        Discovery strategies (in order):
        1. If specific source_names provided: resolve from manifest
        2. If manifest available: return all sources from manifest
        3. Fallback: walk YAML files in target_path for 'sources:' key

        Returns:
            List of source info dicts
        """
        source_tables = []

        if self.config.source_names:
            if not self.manifest_parser or not self.manifest_parser.source_locations:
                logger.error("SST-E004: Manifest required for --source option")
                return []

            for selector in self.config.source_names:
                parts = selector.split(".", 1)
                if len(parts) != 2:
                    logger.error(f"SST-E010: Invalid source selector '{selector}'. Use 'source_name.table_name'")
                    continue

                src_name, tbl_name = parts
                location = self.manifest_parser.get_source_location(src_name, tbl_name)
                if not location:
                    available = list(self.manifest_parser.source_locations.keys())[:10]
                    available_str = ", ".join(available) if available else "(none)"
                    logger.error(
                        f"SST-E006: Source '{selector}' not found in manifest. " f"Available sources: {available_str}"
                    )
                    continue

                source_tables.append(
                    {
                        "source_name": src_name,
                        "table_name": tbl_name,
                        "database": location["database"],
                        "schema": location["schema"],
                        "yaml_path": location.get("original_file_path", ""),
                    }
                )

            return source_tables

        if self.manifest_parser and self.manifest_parser.source_locations:
            for key, location in self.manifest_parser.source_locations.items():
                source_tables.append(
                    {
                        "source_name": location["source_name"],
                        "table_name": location["name"],
                        "database": location["database"],
                        "schema": location["schema"],
                        "yaml_path": location.get("original_file_path", ""),
                    }
                )

            logger.info(f"Found {len(source_tables)} source table(s) from manifest")
            return sorted(source_tables, key=lambda s: f"{s['source_name']}.{s['table_name']}")

        if self.config.target_path:
            yaml_handler = YAMLHandler()
            search_path = Path(self.config.target_path)

            if search_path.is_dir():
                yaml_files = []
                for ext in ["**/*.yml", "**/*.yaml"]:
                    yaml_files.extend(search_path.glob(ext))

                for yaml_file in sorted(yaml_files):
                    try:
                        content = yaml_handler.read_yaml(str(yaml_file))
                    except (ValueError, Exception) as e:
                        logger.warning(f"Skipping malformed YAML file {yaml_file}: {e}")
                        continue
                    if not content or not yaml_handler.has_sources(content):
                        continue

                    for source in content.get("sources", []):
                        if not isinstance(source, dict):
                            continue

                        src_name = source.get("name", "")
                        src_database = (source.get("database") or self.config.database or "").upper()
                        src_schema = (source.get("schema") or self.config.schema or "").upper()

                        if not src_database or not src_schema:
                            logger.warning(
                                f"Source '{src_name}' in {yaml_file} has no database/schema. "
                                "Use --database/--schema or define in YAML."
                            )
                            continue

                        for table in source.get("tables", []):
                            if not isinstance(table, dict):
                                continue
                            tbl_name = table.get("name", "")
                            if not tbl_name:
                                continue
                            source_tables.append(
                                {
                                    "source_name": src_name,
                                    "table_name": tbl_name,
                                    "database": src_database,
                                    "schema": src_schema,
                                    "yaml_path": str(yaml_file),
                                }
                            )

            logger.info(f"Found {len(source_tables)} source table(s) from YAML files")

        if not source_tables:
            logger.warning("SST-E008: No dbt source definitions found")

        return sorted(source_tables, key=lambda s: f"{s['source_name']}.{s['table_name']}")

    def close(self):
        """Close Snowflake connection."""
        if self.snowflake_client:
            # Note: SnowflakeClient doesn't have a close method currently
            # Connection is managed by connection_manager context managers
            logger.info("Enrichment service closed")
