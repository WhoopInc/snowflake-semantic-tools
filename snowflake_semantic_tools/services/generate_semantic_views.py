"""
Generate Semantic Views Service

Orchestrates the generation of Snowflake semantic views from metadata tables.

Provides both low-level (execute with full configs) and high-level (generate with view names)
interfaces for flexibility.
"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from snowflake_semantic_tools.core.generation import SemanticViewBuilder
from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeClient, SnowflakeConfig
from snowflake_semantic_tools.infrastructure.snowflake.connection_manager import ConnectionManager
from snowflake_semantic_tools.services.connection_pool import ConnectionPool
from snowflake_semantic_tools.shared.events import (
    GenerationCompleted,
    GenerationStarted,
    ViewGenerated,
    ViewGenerationFailed,
    fire_event,
)
from snowflake_semantic_tools.shared.progress import NoOpProgressCallback, ProgressCallback
from snowflake_semantic_tools.shared.utils import get_logger

if TYPE_CHECKING:
    from snowflake_semantic_tools.core.parsing.parsers.manifest_parser import ManifestParser

logger = get_logger("generate_semantic_views_service")


def _format_snowflake_error(view_name: str, error: Exception) -> str:
    """Format a Snowflake error with actionable context."""
    full_msg = str(error)
    suggestion = ""

    if "already exists" in full_msg.lower():
        suggestion = "\n  [SST-G002] Fix: Rename view, drop conflicting object, or use different schema"
    elif "does not exist or not authorized" in full_msg.lower() or "object does not exist" in full_msg.lower():
        suggestion = "\n  [SST-G004] Fix: Verify table exists or use --defer-target"
    elif "insufficient privileges" in full_msg.lower() or "access control" in full_msg.lower():
        suggestion = "\n  [SST-G005] Fix: Grant CREATE SEMANTIC VIEW on schema to your role"
    else:
        suggestion = "\n  [SST-G001] Fix: Check the Snowflake error message for details"

    return f"View '{view_name}' failed: {full_msg}{suggestion}"


class MetadataClient:
    """
    Helper for accessing semantic metadata tables.

    Provides validation and query methods for SM_* tables.
    """

    def __init__(self, snowflake_client: SnowflakeClient, database: str, schema: str):
        self.client = snowflake_client
        self.database = database
        self.schema = schema

    def get_available_views(self) -> List[Dict[str, Any]]:
        """Get list of available semantic views from metadata."""
        try:
            query = f"""
            SELECT DISTINCT NAME, TABLES, DESCRIPTION, CUSTOM_INSTRUCTIONS
            FROM {self.database}.{self.schema}.SM_SEMANTIC_VIEWS
            ORDER BY NAME
            """
            df_result = self.client.execute_query(query)

            if df_result.empty:
                return []

            return df_result.to_dict("records")
        except Exception as e:
            logger.error(f"Failed to get available views: {e}")
            return []

    def validate_metadata_access(self) -> bool:
        """Validate that metadata tables are accessible."""
        logger.info(f"Validating metadata access to {self.database}.{self.schema}")
        try:
            # Check if the schema exists
            schema_query = f"""
            SELECT COUNT(*) as schema_exists
            FROM information_schema.schemata 
            WHERE UPPER(schema_name) = UPPER('{self.schema}') 
            AND UPPER(catalog_name) = UPPER('{self.database}')
            """
            schema_result = self.client.execute_query(schema_query)

            if schema_result.empty:
                logger.error(f"Schema '{self.database}.{self.schema}' does not exist")
                return False

            # Get count from DataFrame
            first_row = schema_result.iloc[0]
            if "SCHEMA_EXISTS" in schema_result.columns:
                schema_count = first_row["SCHEMA_EXISTS"]
            elif "schema_exists" in schema_result.columns:
                schema_count = first_row["schema_exists"]
            else:
                schema_count = first_row.iloc[0]

            if int(schema_count) == 0:
                logger.error(f"Schema '{self.database}.{self.schema}' does not exist")
                return False

            # Check for SM_* tables
            tables_query = f"""
            SELECT table_name
            FROM information_schema.tables 
            WHERE UPPER(table_schema) = UPPER('{self.schema}') 
            AND UPPER(table_catalog) = UPPER('{self.database}')
            AND table_name LIKE 'SM_%'
            ORDER BY table_name
            """
            tables_result = self.client.execute_query(tables_query)

            if tables_result.empty:
                logger.error(f"No SM_* tables found in {self.database}.{self.schema}. Did you run 'sst extract' first?")
                return False

            table_names = tables_result["TABLE_NAME"].tolist() if "TABLE_NAME" in tables_result.columns else []
            logger.info(f"Found {len(table_names)} SM_* tables")

            return True

        except Exception as e:
            logger.error(f"Metadata validation failed: {type(e).__name__}: {e}")
            return False


@dataclass
class GenerateConfig:
    """Configuration for view generation."""

    # View configuration
    views_to_generate: Optional[List[Dict[str, Any]]] = None  # List of {'name': str, 'tables': List[str]}

    # Target location
    target_database: Optional[str] = None
    target_schema: Optional[str] = None

    # Source metadata location
    metadata_database: Optional[str] = None
    metadata_schema: Optional[str] = None

    # Table reference override (like dbt defer)
    # DEPRECATED: Use defer_manifest instead for proper multi-database support
    defer_database: Optional[str] = None

    # Defer manifest for looking up actual table locations (database + schema per table)
    defer_manifest: Optional["ManifestParser"] = None

    # Execution mode
    execute: bool = True  # If True, executes SQL in Snowflake. If False, returns SQL only.

    # Parallelism
    threads: int = 1  # Number of concurrent view generations
    view_timeout: int = 300  # Per-view timeout in seconds


@dataclass
class GenerateResult:
    """Result of view generation (low-level interface)."""

    success: bool
    views_generated: List[str]
    views_failed: List[str]
    errors: List[str]
    sql_statements: Optional[Dict[str, str]] = None  # view_name -> SQL

    def print_summary(self):
        """Print generation summary (deprecated - use events instead)."""
        # Summary is now handled by GenerationCompleted event
        # Keep this method for backwards compatibility but it does nothing
        pass


@dataclass
class UnifiedGenerationConfig:
    """
    High-level configuration for semantic view generation.

    Simplified interface that takes view names instead of full configs.
    The service will query metadata to get table lists.
    """

    metadata_database: str
    metadata_schema: str
    target_database: str
    target_schema: str
    views_to_generate: Optional[List[str]] = None  # List of view names (None = all)
    dry_run: bool = False
    # DEPRECATED: Use defer_manifest instead for proper multi-database support
    defer_database: Optional[str] = None
    # Defer manifest for looking up actual table locations (database + schema per table)
    defer_manifest: Optional["ManifestParser"] = None
    # Parallelism
    threads: int = 1
    view_timeout: int = 300
    from_snowflake: bool = False


class UnifiedGenerationResult:
    """
    High-level results from semantic view generation.

    Provides user-friendly summary methods.
    """

    def __init__(
        self,
        success: bool = True,
        views_created: int = 0,
        errors: Optional[List[str]] = None,
        warnings: Optional[List[str]] = None,
        sql_statements: Optional[Dict[str, str]] = None,
    ):
        self.success = success
        self.views_created = views_created
        self.errors = errors or []
        self.warnings = warnings or []
        self.sql_statements = sql_statements or {}

    def add_error(self, error: str):
        """Add an error and mark as failed."""
        self.errors.append(error)
        self.success = False

    def add_warning(self, warning: str):
        """Add a warning."""
        self.warnings.append(warning)

    def add_view_result(self, view_name: str, sql: Optional[str] = None):
        """Add a successful view generation result."""
        self.views_created += 1
        if sql:
            self.sql_statements[view_name] = sql

    def print_summary(self):
        """Print comprehensive summary of generation results."""
        print("\n" + "=" * 60)
        print("SEMANTIC VIEW GENERATION SUMMARY")
        print("=" * 60)
        print(f"Semantic Views: {self.views_created} created")

        if self.warnings:
            print(f"\nWarnings ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  WARNING: {warning}")

        if self.errors:
            print(f"\nErrors ({len(self.errors)}):")
            for error in self.errors:
                print(f"  ERROR: {error}")

        print("\n" + "=" * 60)
        status = "SUCCESS" if self.success else "FAILED"
        print(f"Status: {status}")
        print("=" * 60)


class SemanticViewGenerationService:
    """
    Service for generating Snowflake SEMANTIC VIEWs.

    This service queries metadata tables (populated by extract command)
    and generates Snowflake CREATE SEMANTIC VIEW statements.

    Supports context manager usage for guaranteed cleanup:
        with SemanticViewGenerationService(config) as service:
            result = service.generate(gen_config)
        # Snowflake connection automatically closed
    """

    def __init__(self, config: SnowflakeConfig):
        """
        Initialize the service.

        Args:
            config: Snowflake configuration
        """
        self.config = config
        self.connection_manager = ConnectionManager(config=config)

        # Create builder - pass the connection manager directly
        self.builder = SemanticViewBuilder(config=self.config, snowflake_loader=self.connection_manager)

    def close(self) -> None:
        """Close Snowflake connection. Safe to call multiple times."""
        if self.connection_manager:
            self.connection_manager.close()

    def __enter__(self) -> "SemanticViewGenerationService":
        """Support context manager usage."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Ensure cleanup on context exit."""
        self.close()
        return False  # Don't suppress exceptions

    def execute(
        self, generate_config: GenerateConfig, progress_callback: Optional[ProgressCallback] = None
    ) -> GenerateResult:
        """
        Execute the view generation workflow.

        Args:
            generate_config: Generation configuration
            progress_callback: Optional callback for progress reporting

        Returns:
            Generation result with status and details
        """
        # Use no-op callback if none provided
        progress = progress_callback or NoOpProgressCallback()

        # Set database/schema on builder from config
        self.builder.target_database = generate_config.target_database
        self.builder.target_schema = generate_config.target_schema
        self.builder.metadata_database = generate_config.metadata_database
        self.builder.metadata_schema = generate_config.metadata_schema

        start_time = time.time()
        views_generated = []
        views_failed = []
        errors = []
        sql_statements = {}

        try:
            # If no specific views provided, query available views from metadata
            if not generate_config.views_to_generate:
                logger.debug("Querying available semantic views from metadata tables...")
                views_to_generate = self._query_available_views()
                logger.debug(f"Found {len(views_to_generate) if views_to_generate else 0} views to generate")

                if not views_to_generate:
                    logger.warning("No semantic views found in metadata tables")
                    return GenerateResult(success=True, views_generated=[], views_failed=[], errors=[])
            else:
                views_to_generate = generate_config.views_to_generate

            # Fire event: Generation started
            fire_event(GenerationStarted(view_count=len(views_to_generate)))

            # Dispatch: parallel or sequential
            if generate_config.threads > 1:
                return self._execute_parallel(views_to_generate, generate_config, progress, start_time)

            # Generate each view (sequential)
            for idx, view_config in enumerate(views_to_generate, 1):
                view_name = view_config.get("name")
                table_names = view_config.get("tables", [])
                description = view_config.get("description", "")
                custom_instruction_names = view_config.get("custom_instructions", [])

                if not table_names:
                    error_msg = "No tables specified"
                    errors.append(f"No tables specified for view {view_name}")
                    views_failed.append(view_name)
                    progress.item_progress(idx, len(views_to_generate), view_name, "ERROR")
                    fire_event(
                        ViewGenerationFailed(
                            view_name=view_name, error_message=error_msg, current=idx, total=len(views_to_generate)
                        )
                    )
                    continue

                try:
                    view_start = time.time()
                    # Show [RUN] indicator
                    progress.item_progress(idx, len(views_to_generate), view_name, "RUN")

                    # Generate the semantic view
                    result = self.builder.build_semantic_view(
                        table_names=table_names,
                        view_name=view_name,
                        description=description,
                        execute=generate_config.execute,
                        defer_database=generate_config.defer_database,
                        defer_manifest=generate_config.defer_manifest,
                        custom_instruction_names=view_config.get("custom_instructions", []),
                    )

                    view_duration = time.time() - view_start

                    if result["success"]:
                        views_generated.append(view_name)
                        sql_statements[view_name] = result["sql_statement"]

                        # Event system will show the [CREATED] line (second line, green)
                        fire_event(
                            ViewGenerated(
                                view_name=view_name,
                                table_count=len(table_names),
                                duration_seconds=view_duration,
                                current=idx,
                                total=len(views_to_generate),
                                executed=generate_config.execute,
                            )
                        )
                    else:
                        views_failed.append(view_name)
                        errors.append(result["message"])

                        fire_event(
                            ViewGenerationFailed(
                                view_name=view_name,
                                error_message=result["message"][:200],
                                current=idx,
                                total=len(views_to_generate),
                            )
                        )

                except Exception as e:
                    views_failed.append(view_name)
                    full_error = str(e)
                    error_msg = _format_snowflake_error(view_name, e)
                    errors.append(error_msg)
                    logger.debug(f"View generation exception: {view_name}: {full_error}", exc_info=True)

                    fire_event(
                        ViewGenerationFailed(
                            view_name=view_name,
                            error_message=full_error[:200],
                            current=idx,
                            total=len(views_to_generate),
                        )
                    )

            # Fire event: Generation completed
            duration = time.time() - start_time
            fire_event(
                GenerationCompleted(
                    total_views=len(views_to_generate),
                    successful=len(views_generated),
                    failed=len(views_failed),
                    duration_seconds=duration,
                )
            )

            return GenerateResult(
                success=len(views_failed) == 0,
                views_generated=views_generated,
                views_failed=views_failed,
                errors=errors,
                sql_statements=sql_statements if not generate_config.execute else None,
            )

        except Exception as e:
            logger.error(f"View generation service failed: {e}")
            errors.append(str(e))

            # Fire completion event even on failure
            duration = time.time() - start_time
            fire_event(
                GenerationCompleted(
                    total_views=len(views_to_generate) if "views_to_generate" in locals() else 0,
                    successful=len(views_generated),
                    failed=len(views_failed),
                    duration_seconds=duration,
                )
            )

            return GenerateResult(
                success=False, views_generated=views_generated, views_failed=views_failed, errors=errors
            )

    # Transient Snowflake error codes that merit a retry
    _TRANSIENT_ERRORS = {"250001", "250003"}

    def _is_transient_error(self, error: Exception) -> bool:
        """Check if a Snowflake error is transient and retryable."""
        msg = str(error)
        for code in self._TRANSIENT_ERRORS:
            if code in msg:
                return True
        if "503" in msg or "Service Unavailable" in msg:
            return True
        return False

    def _execute_parallel(
        self,
        views_to_generate: List[Dict[str, Any]],
        generate_config: GenerateConfig,
        progress: "ProgressCallback",
        start_time: float,
    ) -> GenerateResult:
        """
        Execute view generation in parallel using a thread pool.

        Each thread gets its own ConnectionManager from a pool,
        builds a SemanticViewBuilder, and generates one view at a time.
        """
        threads = min(generate_config.threads, len(views_to_generate))
        view_timeout = generate_config.view_timeout
        total = len(views_to_generate)

        views_generated = []
        views_failed = []
        errors = []
        sql_statements = {}
        output_lock = threading.Lock()
        cancelled = threading.Event()

        logger.info(f"Parallel generation: {threads} threads, {total} views, timeout={view_timeout}s")

        pool = ConnectionPool(config=self.config, size=threads)
        try:
            pool.warmup()
        except RuntimeError as e:
            logger.error(f"Connection pool warmup failed: {e}")
            fire_event(
                GenerationCompleted(
                    total_views=total, successful=0, failed=total, duration_seconds=time.time() - start_time
                )
            )
            return GenerateResult(success=False, views_generated=[], views_failed=[], errors=[str(e)])

        def _generate_one(view_config: Dict[str, Any], idx: int) -> Dict[str, Any]:
            """Worker: generate a single semantic view."""
            view_name = view_config.get("name")
            table_names = view_config.get("tables", [])
            description = view_config.get("description", "")
            custom_instruction_names = view_config.get("custom_instructions", [])

            if cancelled.is_set():
                return {"view_name": view_name, "success": False, "error": "Cancelled", "duration": 0}

            if not table_names:
                with output_lock:
                    progress.item_progress(idx, total, view_name, "ERROR")
                    fire_event(
                        ViewGenerationFailed(
                            view_name=view_name, error_message="No tables specified", current=idx, total=total
                        )
                    )
                return {"view_name": view_name, "success": False, "error": "No tables specified", "duration": 0}

            cm = pool.checkout()
            try:
                builder = SemanticViewBuilder(config=self.config, snowflake_loader=cm)
                builder.target_database = generate_config.target_database
                builder.target_schema = generate_config.target_schema
                builder.metadata_database = generate_config.metadata_database
                builder.metadata_schema = generate_config.metadata_schema
                builder.store = self.builder.store

                with output_lock:
                    progress.item_progress(idx, total, view_name, "RUN")

                view_start = time.time()
                result = builder.build_semantic_view(
                    table_names=table_names,
                    view_name=view_name,
                    description=description,
                    execute=generate_config.execute,
                    defer_database=generate_config.defer_database,
                    defer_manifest=generate_config.defer_manifest,
                    custom_instruction_names=custom_instruction_names,
                )
                view_duration = time.time() - view_start

                if result["success"]:
                    with output_lock:
                        fire_event(
                            ViewGenerated(
                                view_name=view_name,
                                table_count=len(table_names),
                                duration_seconds=view_duration,
                                current=idx,
                                total=total,
                                executed=generate_config.execute,
                            )
                        )
                    return {
                        "view_name": view_name,
                        "success": True,
                        "sql": result["sql_statement"],
                        "duration": view_duration,
                    }
                else:
                    with output_lock:
                        fire_event(
                            ViewGenerationFailed(
                                view_name=view_name,
                                error_message=result["message"][:200],
                                current=idx,
                                total=total,
                            )
                        )
                    return {
                        "view_name": view_name,
                        "success": False,
                        "error": result["message"],
                        "duration": view_duration,
                    }

            except Exception as e:
                # Retry once on transient errors
                if self._is_transient_error(e):
                    logger.debug(f"Transient error on '{view_name}', retrying in 2s...")
                    time.sleep(2)
                    try:
                        view_start = time.time()
                        result = builder.build_semantic_view(
                            table_names=table_names,
                            view_name=view_name,
                            description=description,
                            execute=generate_config.execute,
                            defer_database=generate_config.defer_database,
                            defer_manifest=generate_config.defer_manifest,
                            custom_instruction_names=custom_instruction_names,
                        )
                        view_duration = time.time() - view_start
                        if result["success"]:
                            with output_lock:
                                fire_event(
                                    ViewGenerated(
                                        view_name=view_name,
                                        table_count=len(table_names),
                                        duration_seconds=view_duration,
                                        current=idx,
                                        total=total,
                                        executed=generate_config.execute,
                                    )
                                )
                            return {
                                "view_name": view_name,
                                "success": True,
                                "sql": result["sql_statement"],
                                "duration": view_duration,
                            }
                    except Exception:
                        pass  # Fall through to error handling

                error_msg = _format_snowflake_error(view_name, e)
                with output_lock:
                    fire_event(
                        ViewGenerationFailed(view_name=view_name, error_message=str(e)[:200], current=idx, total=total)
                    )
                return {"view_name": view_name, "success": False, "error": error_msg, "duration": 0}
            finally:
                pool.checkin(cm)

        try:
            with ThreadPoolExecutor(max_workers=threads) as executor:
                futures = {
                    executor.submit(_generate_one, view_config, idx): view_config
                    for idx, view_config in enumerate(views_to_generate, 1)
                }

                for future in as_completed(futures):
                    try:
                        result = future.result(timeout=view_timeout)
                    except TimeoutError:
                        view_config = futures[future]
                        view_name = view_config.get("name", "unknown")
                        error_msg = f"View '{view_name}' timed out after {view_timeout}s"
                        views_failed.append(view_name)
                        errors.append(error_msg)
                        with output_lock:
                            fire_event(
                                ViewGenerationFailed(
                                    view_name=view_name, error_message=error_msg, current=0, total=total
                                )
                            )
                        continue
                    except Exception as e:
                        view_config = futures[future]
                        view_name = view_config.get("name", "unknown")
                        error_msg = _format_snowflake_error(view_name, e)
                        views_failed.append(view_name)
                        errors.append(error_msg)
                        continue

                    if result["success"]:
                        views_generated.append(result["view_name"])
                        if result.get("sql"):
                            sql_statements[result["view_name"]] = result["sql"]
                    else:
                        views_failed.append(result["view_name"])
                        if result.get("error"):
                            errors.append(result["error"])

        except KeyboardInterrupt:
            cancelled.set()
            logger.warning("Ctrl+C received — shutting down...")
            with output_lock:
                progress.warning(
                    f"Cancelled. {len(views_generated)} of {total} completed, "
                    f"{total - len(views_generated) - len(views_failed)} aborted."
                )
        finally:
            pool.close_all()

        duration = time.time() - start_time
        fire_event(
            GenerationCompleted(
                total_views=total,
                successful=len(views_generated),
                failed=len(views_failed),
                duration_seconds=duration,
            )
        )

        return GenerateResult(
            success=len(views_failed) == 0 and not cancelled.is_set(),
            views_generated=views_generated,
            views_failed=views_failed,
            errors=errors,
            sql_statements=sql_statements if not generate_config.execute else None,
        )

    def _query_available_views(self) -> List[Dict[str, Any]]:
        """
        Query available semantic views from the sm_semantic_views table.

        Returns:
            List of view configurations
        """
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                query = f"""
                SELECT 
                    NAME,
                    TABLES,
                    DESCRIPTION,
                    CUSTOM_INSTRUCTIONS
                FROM {self.builder.metadata_database}.{self.builder.metadata_schema}.SM_SEMANTIC_VIEWS
                """

                logger.info(f"Executing query: {query}")
                cursor.execute(query)
                rows = cursor.fetchall()
                logger.info(f"Query returned {len(rows)} rows")

                views = []
                for row in rows:
                    # Parse tables (stored as JSON string)
                    tables = self._parse_tables_column(row[1])

                    custom_instruction_names = self._parse_custom_instructions(row[3])
                    views.append(
                        {
                            "name": row[0],
                            "tables": tables,
                            "description": row[2] or "",
                            "custom_instructions": custom_instruction_names,
                        }
                    )

                return views

        except Exception as e:
            import traceback

            logger.error(f"Failed to query semantic views: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []

    def _parse_tables_column(self, raw: Any) -> List[str]:
        """Parse the tables column from sm_semantic_views table."""
        if raw is None:
            return []

        # Handle different formats
        if isinstance(raw, str):
            # Try to parse as JSON
            try:
                import json

                tables = json.loads(raw)
                if isinstance(tables, list):
                    return tables
            except:
                pass

            # Try comma-separated
            if "," in raw:
                return [t.strip() for t in raw.split(",")]

            # Single table
            return [raw.strip()]

        if isinstance(raw, list):
            return raw

        return []

    def _parse_custom_instructions(self, raw: Any) -> List[str]:
        """Parse custom instructions column."""
        if raw is None:
            return []

        if isinstance(raw, str):
            try:
                import json

                instructions = json.loads(raw)
                if isinstance(instructions, list):
                    # Instruction names are already uppercase from extraction
                    return [str(i).strip() if i else "" for i in instructions if i]
            except:
                pass

            # Try comma-separated
            if "," in raw:
                return [i.strip() for i in raw.split(",") if i.strip()]

            # Single instruction
            if raw.strip():
                return [raw.strip()]

        if isinstance(raw, list):
            # Instruction names are already uppercase from extraction
            return [str(i).strip() if i else "" for i in raw if i]

        return []

    def generate_all_views(self) -> GenerateResult:
        """
        Generate all available semantic views from metadata.

        Returns:
            Generation result
        """
        config = GenerateConfig(execute=True)
        return self.execute(config)

    def get_available_views(self, database: str, schema: str) -> List[Dict[str, Any]]:
        """
        Get list of available semantic views from metadata.

        This method is useful for pre-filtering views before calling generate().

        Args:
            database: Metadata database
            schema: Metadata schema

        Returns:
            List of view dictionaries with 'NAME' and 'TABLES' keys
        """
        try:
            snowflake_client = SnowflakeClient(self.config)
            metadata_client = MetadataClient(snowflake_client, database, schema)
            return metadata_client.get_available_views()
        except Exception as e:
            logger.error(f"Failed to get available views: {e}")
            return []

    def generate(
        self, config: UnifiedGenerationConfig, progress_callback: Optional[ProgressCallback] = None
    ) -> UnifiedGenerationResult:
        """
        High-level generation interface with automatic view discovery.

        This method provides a simpler interface that:
        1. Validates metadata access
        2. Queries available views from metadata
        3. Filters by requested views (if specified)
        4. Delegates to execute() for actual generation

        Args:
            config: High-level generation configuration
            progress_callback: Optional callback for progress reporting

        Returns:
            UnifiedGenerationResult with user-friendly summary
        """
        logger.info("Starting semantic view generation (high-level interface)")

        # Use no-op callback if none provided
        progress = progress_callback or NoOpProgressCallback()

        result = UnifiedGenerationResult()

        try:
            if not config.from_snowflake:
                return self._generate_from_manifest(config, progress, result)
            return self._generate_from_snowflake(config, progress, result)
        except Exception as e:
            logger.error(f"Generation failed: {e}")
            result.add_error(f"Generation failed: {str(e)}")
            return result

    def _parse_view_configs(self, available_views: List[Dict]) -> List[Dict]:
        view_configs = []
        for raw_view in available_views:
            view = {k.upper(): v for k, v in raw_view.items()}
            tables_raw = view.get("TABLES", "[]")
            try:
                tables = json.loads(tables_raw) if isinstance(tables_raw, str) else tables_raw
            except (json.JSONDecodeError, TypeError):
                tables = tables_raw if isinstance(tables_raw, list) else []

            custom_instructions = self._parse_custom_instructions(view.get("CUSTOM_INSTRUCTIONS"))

            view_configs.append(
                {
                    "name": view.get("NAME", ""),
                    "tables": tables if isinstance(tables, list) else [],
                    "description": view.get("DESCRIPTION", ""),
                    "custom_instructions": custom_instructions,
                }
            )
        return view_configs

    def _filter_and_execute(
        self,
        config: UnifiedGenerationConfig,
        view_configs: List[Dict],
        progress: ProgressCallback,
        result: UnifiedGenerationResult,
    ) -> UnifiedGenerationResult:
        if config.views_to_generate:
            requested_views = set(config.views_to_generate)
            view_configs = [v for v in view_configs if v["name"] in requested_views]
            found_views = {v["name"] for v in view_configs}
            missing_views = requested_views - found_views
            if missing_views:
                result.add_error(f"Requested views not found: {', '.join(sorted(missing_views))}")

        if not view_configs:
            result.add_error("No views found to generate")
            return result

        low_level_config = GenerateConfig(
            views_to_generate=view_configs,
            target_database=config.target_database,
            target_schema=config.target_schema,
            metadata_database=config.metadata_database,
            metadata_schema=config.metadata_schema,
            defer_database=config.defer_database,
            defer_manifest=config.defer_manifest,
            execute=not config.dry_run,
            threads=config.threads,
            view_timeout=config.view_timeout,
        )

        low_level_result = self.execute(low_level_config, progress_callback=progress)

        for view_name in low_level_result.views_generated:
            sql = low_level_result.sql_statements.get(view_name) if low_level_result.sql_statements else None
            result.add_view_result(view_name, sql)
        for error in low_level_result.errors:
            result.add_error(error)
        result.success = low_level_result.success
        return result

    def _generate_from_manifest(
        self, config: UnifiedGenerationConfig, progress: ProgressCallback, result: UnifiedGenerationResult
    ) -> UnifiedGenerationResult:
        from snowflake_semantic_tools.core.metadata.in_memory_store import InMemoryStore

        manifest_path = Path("target") / "sst_manifest.json"
        if not manifest_path.exists():
            result.add_error(
                "SST-C007: sst_manifest.json not found. Run 'sst compile' first, "
                "or use --from-snowflake to read from SM_* tables."
            )
            return result

        manifest_mtime = manifest_path.stat().st_mtime
        from snowflake_semantic_tools.shared.utils.file_utils import find_dbt_model_files, find_semantic_model_files

        try:
            for f in find_dbt_model_files() + find_semantic_model_files():
                if f.stat().st_mtime > manifest_mtime:
                    result.add_warning(
                        f"SST-C008: sst_manifest.json is older than {f.name}. " "Run 'sst compile' to refresh."
                    )
                    break
        except (ValueError, OSError) as e:
            logger.debug(f"Staleness check skipped: {e}")

        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest_data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            result.add_error(f"SST-C007: Could not load sst_manifest.json: {e}")
            return result

        tables_data = manifest_data.get("tables", {})
        if not tables_data:
            result.add_error("SST-C007: sst_manifest.json has no tables section. Re-run 'sst compile'.")
            return result

        store = InMemoryStore(tables_data)
        self.builder.store = store

        progress.info("Discovering semantic views...")
        available_views = store.get_semantic_views()
        progress.detail(f"Found {len(available_views)} available views")

        view_configs = self._parse_view_configs(available_views)
        return self._filter_and_execute(config, view_configs, progress, result)

    def _generate_from_snowflake(
        self, config: UnifiedGenerationConfig, progress: ProgressCallback, result: UnifiedGenerationResult
    ) -> UnifiedGenerationResult:
        progress.info("Validating metadata access...")
        snowflake_client = SnowflakeClient(self.config)
        metadata_client = MetadataClient(snowflake_client, config.metadata_database, config.metadata_schema)

        if not metadata_client.validate_metadata_access():
            result.add_error(f"Cannot access semantic metadata in {config.metadata_database}.{config.metadata_schema}")
            return result

        progress.detail(f"Metadata accessible at {config.metadata_database}.{config.metadata_schema}")

        progress.info("Discovering semantic views...")
        available_views = metadata_client.get_available_views()
        progress.detail(f"Found {len(available_views)} available views")

        view_configs = self._parse_view_configs(available_views)
        return self._filter_and_execute(config, view_configs, progress, result)
