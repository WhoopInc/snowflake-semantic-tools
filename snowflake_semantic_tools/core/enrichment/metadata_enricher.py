"""
Core Metadata Enrichment Logic

Enriches dbt model YAML files with semantic metadata from Snowflake while
preserving existing manual work.

## What Gets Enriched

- Column types (dimension, fact, time_dimension)
- Data types (from Snowflake schema)
- Sample values (batched for performance)
- Primary keys (optional validation)

## Preservation Rules

**Never Overwritten:**
- Descriptions, synonyms, existing column_type/data_type

**Always Refreshed:**
- sample_values, is_enum (from current Snowflake data)

**Set If Missing:**
- column_type, data_type, synonyms, primary_key (if candidates provided)

"""

import copy
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from snowflake_semantic_tools.core.enrichment.primary_key_validator import PrimaryKeyValidator
from snowflake_semantic_tools.core.enrichment.type_mappings import determine_column_type, map_snowflake_to_sst_datatype
from snowflake_semantic_tools.core.enrichment.yaml_handler import YAMLHandler
from snowflake_semantic_tools.shared.config import get_config
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


class MetadataEnricher:
    """
    Enriches dbt model YAML files with Snowflake metadata.

    Coordinates Snowflake queries, YAML operations, and primary key validation
    to safely enrich models while preserving existing manual work.

    Safe to run repeatedly - preserves manual work, refreshes data-driven fields.
    """

    def __init__(
        self,
        snowflake_client,
        yaml_handler: YAMLHandler,
        primary_key_validator: PrimaryKeyValidator,
        default_database: str,
        default_schema: str,
    ):
        """
        Initialize enricher with dependencies.

        Args:
            snowflake_client: SnowflakeClient instance for Snowflake operations
            yaml_handler: YAMLHandler instance for YAML operations
            primary_key_validator: PrimaryKeyValidator instance for PK validation
            default_database: Default database name for models (required)
            default_schema: Default schema name for models (required)
        """
        self.snowflake_client = snowflake_client
        self.yaml_handler = yaml_handler
        self.primary_key_validator = primary_key_validator
        self.default_database = default_database.upper()
        self.default_schema = default_schema.lower()
        self.config = get_config()

        # Get configurable limits for enrichment
        self.distinct_limit = self.config.get_enrichment_distinct_limit()
        self.display_limit = self.config.get_enrichment_display_limit()

        # NEW: Always initialize Cortex synonym generator (lazy - only used if components request it)
        from snowflake_semantic_tools.core.enrichment.cortex_synonym_generator import CortexSynonymGenerator

        self.synonym_generator = CortexSynonymGenerator(
            snowflake_client=snowflake_client,
            model=self.config.get_synonym_model(),
            max_synonyms=self.config.get_synonym_max_count(),
        )

    def _execute_with_retry(self, operation, *args, max_retries=3, **kwargs):
        """
        Execute a Snowflake operation with retry logic.

        Args:
            operation: Function to execute
            *args: Positional arguments for the operation
            max_retries: Maximum number of retry attempts
            **kwargs: Keyword arguments for the operation

        Returns:
            Result of the operation

        Raises:
            Exception: If all retries fail
        """
        last_exception = None

        for attempt in range(max_retries):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                last_exception = e
                error_msg = str(e).lower()

                # Check if it's a connection-related error
                is_connection_error = any(
                    keyword in error_msg
                    for keyword in [
                        "connection",
                        "timeout",
                        "timed out",
                        "network",
                        "socket",
                        "disconnected",
                        "closed",
                        "broken pipe",
                    ]
                )

                if is_connection_error and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # Exponential backoff: 2s, 4s, 6s
                    logger.warning(
                        f"Connection error on attempt {attempt + 1}/{max_retries}: {e}. "
                        f"Retrying in {wait_time} seconds..."
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    # Not a connection error or last attempt
                    raise

        # If we get here, all retries failed
        raise last_exception

    def enrich_model(
        self,
        model_sql_path: str,
        primary_key_candidates: Optional[Dict[str, List[List[str]]]] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        components: Optional[List[str]] = None,  # NEW: Which components to enrich
    ) -> Dict[str, Any]:
        """
        Enrich a single model's metadata with data from Snowflake.

        Components can include:
        - column-types: Determine dimension/fact/time_dimension
        - data-types: Map Snowflake types to SST types
        - sample-values: Query data for sample values (SLOW)
        - detect-enums: Mark low-cardinality columns
        - primary-keys: Validate primary key candidates
        - table-synonyms: Generate table synonyms via Cortex LLM
        - column-synonyms: Generate column synonyms via Cortex LLM

        If components is None, does all standard enrichment (no synonyms)

        Args:
            model_sql_path: Path to SQL model file

            primary_key_candidates: Dictionary mapping model names to PK candidate lists.
                                   Structure: {'model': [['id'], ['id', 'date']]}
                                   If None, PK validation is skipped.

            database: Database override (uses default if not provided)
            schema: Schema override (uses default if not provided)

        Returns:
            Dict with status, model name, yaml_path, columns_processed, or error
        """
        try:
            logger.info(f"Processing model: {Path(model_sql_path).name}")

            # Extract model information
            model_info = self._extract_model_info(model_sql_path, database, schema)
            model_name = model_info["name"]
            schema_name = model_info["schema"]
            database_name = model_info["database"]

            logger.info(f"  Model Name: {model_name}")
            logger.info(f"  Database:   {database_name}")
            logger.info(f"  Schema:     {schema_name}")

            # Find/create YAML file
            yaml_path = self.yaml_handler.find_yaml_file_for_model(model_sql_path)
            existing_yaml = self.yaml_handler.read_yaml(yaml_path)

            # Get existing model metadata from 'models' section only (not semantic_models)
            # Enrichment columns ALWAYS go under 'models:', never touch 'semantic_models:'
            if existing_yaml:
                existing_model = None
                if "models" in existing_yaml:
                    for model in existing_yaml["models"]:
                        if isinstance(model, dict) and model.get("name") == model_name:
                            existing_model = model
                            break
                logger.info(
                    f"  Status:     Found existing YAML"
                    + (" (with models section)" if existing_model else " (semantic_models only)")
                )
            else:
                existing_model = None
                existing_yaml = self.yaml_handler.create_base_yaml_structure(model_name)
                logger.info(f"  Status:     Creating new YAML")

            # Determine what data we need from Snowflake (decoupled checks)
            # Schema metadata: column names, data types (for type mapping)
            needs_schema_metadata = not components or any(
                c in components for c in ["column-types", "data-types", "primary-keys"]
            )
            # Sample data: actual data queries (expensive)
            needs_sample_data = not components or any(c in components for c in ["sample-values", "detect-enums"])
            # Combined: need schema query if either is true
            needs_schema_query = needs_schema_metadata or needs_sample_data

            # Also need schema query if column-synonyms requested but no existing columns
            existing_columns = existing_model.get("columns", []) if existing_model else []
            if "column-synonyms" in components and not existing_columns:
                needs_schema_query = True

            # Get table schema from Snowflake ONLY if needed
            table_columns = None
            if needs_schema_query:
                logger.info(f"  Action:     Querying Snowflake for table schema...")
                table_columns = self._execute_with_retry(
                    self.snowflake_client.metadata_manager.get_table_schema, model_name, schema_name, database_name
                )

                # CRITICAL: If we can't get table schema, abort to avoid data loss
                if not table_columns:
                    logger.error(f"  Result:     FAILED - Could not retrieve table schema from Snowflake")
                    logger.error(f"              Skipping to prevent overwriting existing metadata")
                    return {
                        "success": False,
                        "model": model_name,
                        "error": "Failed to retrieve table schema from Snowflake",
                        "columns_processed": 0,
                    }
            else:
                logger.info(f"  Action:     Synonym-only mode (no Snowflake schema query)")
                table_columns = []  # Empty for synonym-only mode

            # Process model-level metadata (only if we have schema)
            if needs_schema_query:
                pk_candidates = primary_key_candidates.get(model_name, []) if primary_key_candidates else []
                # Use existing model from 'models' section, or create fresh base
                # NEVER use semantic_models as base - they have different structure
                base_model = existing_model or {"name": model_name}
                updated_model = self._process_model_metadata(base_model, model_info, table_columns, pk_candidates)

                # Process column-level metadata
                updated_columns = self._process_columns_metadata(
                    existing_model.get("columns", []) if existing_model else [],
                    table_columns,
                    model_name,
                    schema_name,
                    database_name,
                    components=components,  # Pass components to control behavior
                )
            else:
                # Synonym-only: use existing data from 'models' section only
                updated_model = existing_model or {"name": model_name}
                updated_columns = updated_model.get("columns", [])

            # Update model with processed columns
            updated_model["columns"] = updated_columns

            # NEW: Generate synonyms (AFTER sample values for rich context)
            if self.synonym_generator and components:
                # Table-level synonyms (with full YAML context)
                if "table-synonyms" in components:
                    # Check if force_synonyms is set in service config (passed from CLI)
                    force = getattr(self, "force_synonyms", False)
                    updated_model = self._enrich_table_synonyms(
                        updated_model, updated_columns, full_yaml=existing_yaml, force=force
                    )

                # Column-level synonyms (with full YAML context)
                if "column-synonyms" in components:
                    force = getattr(self, "force_synonyms", False)
                    updated_columns = self._enrich_column_synonyms(
                        updated_columns,
                        model_name,
                        table_description=updated_model.get("description", ""),
                        full_yaml=existing_yaml,
                        force=force,
                    )
                    updated_model["columns"] = updated_columns

            # Update YAML structure
            # CRITICAL: Enrichment columns ALWAYS go under 'models:' key, never under 'semantic_models:'
            # semantic_models uses dimensions/measures/entities, models uses columns
            if "models" in existing_yaml:
                # Update existing model in the models section
                model_found = False
                for i, model in enumerate(existing_yaml["models"]):
                    if model.get("name") == model_name:
                        existing_yaml["models"][i] = updated_model
                        model_found = True
                        break
                if not model_found:
                    # Model not in models list, add it
                    existing_yaml["models"].append(updated_model)
            else:
                # Create new 'models' section (even if semantic_models exists)
                existing_yaml["models"] = [updated_model]

            # Write updated YAML
            success = self.yaml_handler.write_yaml(existing_yaml, yaml_path)

            if success:
                logger.info(f"  Result: SUCCESS - Updated {len(updated_columns)} columns")
                logger.info(f"  YAML Path: {yaml_path}")

                return {
                    "status": "success",
                    "model": model_name,
                    "yaml_path": yaml_path,
                    "columns_processed": len(updated_columns),
                }
            else:
                logger.error(f"  Result: FAILED - Could not write YAML file")

                return {"status": "error", "model": model_name, "error": "Failed to write YAML file"}

        except KeyError as e:
            logger.error(f"  Result: ERROR - Missing required field {str(e)}")

            # Provide helpful error message for common KeyErrors
            field_name = str(e).strip("'\"")
            if field_name == "sst":
                error_msg = (
                    f"Model YAML missing 'meta.sst' section. Run 'sst enrich' to add metadata or check YAML structure."
                )
            else:
                error_msg = f"Missing required field '{field_name}' in YAML metadata. Check model structure."

            return {"status": "error", "model": model_sql_path, "error": error_msg}
        except Exception as e:
            logger.error(f"  Result: ERROR - {str(e)}")
            logger.debug(f"  Exception type: {type(e).__name__}", exc_info=True)

            return {"status": "error", "model": model_sql_path, "error": f"{type(e).__name__}: {str(e)}"}

    def _extract_model_info(
        self, model_sql_path: str, database: Optional[str] = None, schema: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Extract model name, schema, and database from SQL file path.

        Args:
            model_sql_path: Path to SQL model file
            database: Optional database override for this model
            schema: Optional schema override for this model

        Returns:
            Dict with model name, schema, and database
        """
        path = Path(model_sql_path)
        model_name = path.stem

        # Use provided overrides or fall back to defaults
        database_name = (database or self.default_database).upper()
        schema_name = (schema or self.default_schema).lower()

        return {"name": model_name, "schema": schema_name, "database": database_name}

    def _process_model_metadata(
        self,
        existing_model: Dict[str, Any],
        model_info: Dict[str, str],
        table_columns: List[Dict[str, Any]],
        primary_key_candidates: List[List[str]],
    ) -> Dict[str, Any]:
        """
        Process model-level metadata following preservation rules.

        Enriches meta.sst with database/schema/table, synonyms, and primary_key.
        Validates PK candidates if provided (preserves existing, skips if no candidates).

        Args:
            existing_model: Existing model metadata from YAML
            model_info: Dict with 'name', 'schema', 'database'
            table_columns: Column information from Snowflake
            primary_key_candidates: List of PK candidates (e.g., [['id'], ['id', 'date']])

        Returns:
            Dict with updated model metadata
        """
        # Ensure SST structure exists
        model = self.yaml_handler.ensure_sst_structure(existing_model)
        sst = model["meta"]["sst"]

        # Set required fields (using NEW v1.2+ names)
        sst["cortex_searchable"] = sst.get("cortex_searchable", False)

        # NOTE: database/schema/table are NO LONGER written during enrichment
        # With manifest auto-detection (v1.3.0+), these fields are optional and auto-detected
        # Enrichment now focuses on data-driven metadata only (types, samples, synonyms, PKs)

        # Handle synonyms (preserve existing, add empty list if missing)
        if "synonyms" not in sst or sst["synonyms"] is None:
            sst["synonyms"] = []

        # Handle primary key (preserve existing, validate candidates if provided)
        if "primary_key" not in sst or not sst["primary_key"]:
            # Only run validation if candidates are provided
            # This saves Snowflake compute when no candidates available
            if primary_key_candidates and len(primary_key_candidates) > 0:
                logger.info(f"  PK Validation: Testing {len(primary_key_candidates)} candidate(s)...")
                try:
                    detected_pk = self.primary_key_validator.detect_primary_key(
                        model_info["name"],
                        model_info["schema"],
                        model_info["database"],
                        table_columns,
                        primary_key_candidates,
                    )

                    if detected_pk:
                        if len(detected_pk) == 1:
                            sst["primary_key"] = detected_pk[0]
                        else:
                            sst["primary_key"] = detected_pk
                        logger.info(f"  PK Validated: {sst['primary_key']}")
                    else:
                        # No valid candidates found
                        sst["primary_key"] = ""
                        logger.warning(
                            f"  PK Validation: None of the {len(primary_key_candidates)} candidates were valid - left blank"
                        )

                except Exception as e:
                    logger.warning(f"  PK Validation: Failed ({str(e)}) - continuing without primary key")
                    sst["primary_key"] = ""
            else:
                # No candidates provided - skip validation to save Snowflake compute
                sst["primary_key"] = ""
                logger.info(f"  PK Status:    No candidates provided - left blank for manual specification")
        else:
            logger.info(f"  PK Status:    Preserved existing key: {sst['primary_key']}")

        # Handle unique keys (preserve existing - required for ASOF relationships)
        if "unique_keys" in sst and sst["unique_keys"]:
            logger.info(f"  UK Status:    Preserved existing keys: {sst['unique_keys']}")

        # Ensure proper model structure order: name, description, meta, config, columns
        ordered_model = self._order_model_structure(model)

        return ordered_model

    def _process_columns_metadata(
        self,
        existing_columns: List[Dict[str, Any]],
        table_columns: List[Dict[str, Any]],
        model_name: str,
        schema_name: str,
        database_name: str,
        components: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Process column-level metadata following preservation rules.

        Orchestrates column processing: batch sample fetching, individual column
        enrichment, and preservation of existing metadata.
        """
        logger.info(f"  Columns:      Processing {len(table_columns)} columns...")

        # Create case-insensitive lookup for existing columns
        existing_lookup = self._create_column_lookup(existing_columns)

        # Batch-fetch sample values for all non-PII columns (if needed)
        batch_samples = self._batch_fetch_samples(
            table_columns, existing_lookup, model_name, schema_name, database_name, components
        )

        # Process each column
        updated_columns = []
        for idx, table_col in enumerate(table_columns, 1):
            column = self._process_single_column(
                table_col,
                existing_lookup,
                batch_samples,
                model_name,
                schema_name,
                database_name,
                idx,
                len(table_columns),
                components=components,
            )
            updated_columns.append(column)

        return updated_columns

    def _create_column_lookup(self, columns: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Create case-insensitive lookup for existing columns."""
        return {col["name"].upper(): col for col in columns}

    def _batch_fetch_samples(
        self,
        table_columns: List[Dict[str, Any]],
        existing_lookup: Dict[str, Dict[str, Any]],
        model_name: str,
        schema_name: str,
        database_name: str,
        components: Optional[List[str]],
    ) -> Dict[str, List[Any]]:
        """Batch-fetch sample values for all non-PII columns."""
        # Need samples for sample-values OR detect-enums (enum detection uses sample data)
        needs_samples = not components or any(c in components for c in ["sample-values", "detect-enums"])
        if not needs_samples:
            return {}

        # Identify columns needing samples
        columns_needing_samples = []
        for table_col in table_columns:
            col_name = table_col["name"]
            existing_col = existing_lookup.get(col_name.upper())
            column_dict = copy.deepcopy(existing_col) if existing_col else {"name": col_name.lower()}

            if not self._is_pii_protected_column(column_dict):
                columns_needing_samples.append(col_name)

        if not columns_needing_samples:
            return {}

        # Batch fetch
        logger.debug(f"    Batch-fetching sample values for {len(columns_needing_samples)} columns...")
        try:
            return self.snowflake_client.metadata_manager.get_sample_values_batch(
                model_name, schema_name, columns_needing_samples, database_name, limit=self.distinct_limit
            )
        except Exception as e:
            logger.warning(f"    Batch sample values failed: {e}")
            return {}

    def _process_single_column(
        self,
        table_col: Dict[str, Any],
        existing_lookup: Dict[str, Dict[str, Any]],
        batch_samples: Dict[str, List[Any]],
        model_name: str,
        schema_name: str,
        database_name: str,
        idx: int,
        total: int,
        components: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Process metadata for a single column."""
        col_name = table_col["name"]
        logger.debug(f"    [{idx}/{total}] {col_name}")

        # Get existing or create new
        existing_col = existing_lookup.get(col_name.upper())
        if existing_col:
            column = copy.deepcopy(existing_col)
        else:
            # Create new column with description placeholder so validation passes
            column = {
                "name": col_name.lower(),
                "description": "",  # Required field - placeholder for user to fill in
            }
        column["name"] = col_name.lower()

        # Ensure SST structure
        column = self.yaml_handler.ensure_column_sst_structure(column)
        column_sst = column["meta"]["sst"]

        # Map and set data types (only if requested via components)
        snowflake_type = table_col["type"]
        self._enrich_column_types(column_sst, col_name, snowflake_type, components)

        # Handle sample values and enum detection (only if requested via components)
        needs_sample_enrichment = not components or any(c in components for c in ["sample-values", "detect-enums"])
        if needs_sample_enrichment:
            if self._is_pii_protected_column(column):
                # Only set fields if their corresponding components are requested
                if not components or "sample-values" in components:
                    column_sst["sample_values"] = []
                if not components or "detect-enums" in components:
                    column_sst["is_enum"] = False
                logger.debug(f"      - PII protected (no samples)")
            else:
                self._enrich_sample_values(
                    column_sst, col_name, batch_samples, model_name, schema_name, database_name, components
                )

        # Ensure proper key ordering
        return self.yaml_handler._order_column_sst_keys(column)

    def _enrich_column_types(
        self,
        column_sst: Dict[str, Any],
        col_name: str,
        snowflake_type: str,
        components: Optional[List[str]] = None,
    ):
        """Enrich column with data_type and column_type (preserves existing).

        Only enriches if the corresponding component is requested:
        - data-types: Sets data_type from Snowflake type mapping
        - column-types: Sets column_type (dimension/fact/time_dimension)
        """
        sst_data_type = map_snowflake_to_sst_datatype(snowflake_type)

        # Only enrich data_type if requested (or no components = all)
        enrich_data_types = not components or "data-types" in components
        if enrich_data_types:
            if "data_type" not in column_sst or not column_sst["data_type"]:
                column_sst["data_type"] = sst_data_type
                logger.debug(f"      - Set data_type: {sst_data_type}")
            else:
                logger.debug(f"      - Preserved data_type: {column_sst['data_type']}")

        # Only enrich column_type if requested (or no components = all)
        enrich_column_types = not components or "column-types" in components
        if enrich_column_types:
            if "column_type" not in column_sst or not column_sst["column_type"]:
                column_type = determine_column_type(col_name, snowflake_type)
                column_sst["column_type"] = column_type
                logger.debug(f"      - Set column_type: {column_type}")
            else:
                logger.debug(f"      - Preserved column_type: {column_sst['column_type']}")

        if "synonyms" not in column_sst or column_sst["synonyms"] is None:
            column_sst["synonyms"] = []

    def _enrich_sample_values(
        self,
        column_sst: Dict[str, Any],
        col_name: str,
        batch_samples: Dict[str, List[Any]],
        model_name: str,
        schema_name: str,
        database_name: str,
        components: Optional[List[str]] = None,
    ):
        """Fetch and apply sample values with enum detection.

        Only enriches if the corresponding component is requested:
        - sample-values: Sets sample_values from Snowflake data
        - detect-enums: Sets is_enum based on cardinality analysis
        """
        # Get samples (from batch or individual query)
        col_upper = col_name.upper()
        if col_upper in batch_samples:
            sample_values_raw = batch_samples[col_upper]
            logger.debug(f"      - Using batch-fetched samples")
        else:
            try:
                sample_values_raw = self._execute_with_retry(
                    self.snowflake_client.metadata_manager.get_sample_values,
                    model_name,
                    schema_name,
                    col_name,
                    database_name,
                    limit=self.distinct_limit,
                )
                logger.debug(f"      - Fetched samples individually")
            except Exception as e:
                logger.warning(f"      - Could not get samples: {e}")
                sample_values_raw = []

        # Filter nulls/empties
        sample_values = [
            v
            for v in sample_values_raw
            if v and str(v).strip() and str(v).strip().lower() not in ["null", "none", "<null>"]
        ]

        # Determine enum status and apply samples
        final_samples, is_enum = self._determine_enum_status(sample_values, column_sst.get("column_type", ""))

        # Only set sample_values if requested (or no components = all)
        enrich_sample_values = not components or "sample-values" in components
        if enrich_sample_values:
            column_sst["sample_values"] = final_samples

        # Only set is_enum if requested (or no components = all)
        enrich_detect_enums = not components or "detect-enums" in components
        if enrich_detect_enums:
            column_sst["is_enum"] = is_enum

    def _determine_enum_status(self, sample_values: List[Any], column_type: str) -> tuple[List[Any], bool]:
        """
        Determine if column is enum and which samples to keep.

        Rules:
        1. time_dimension: Never enum (dates grow)
        2. fact: Never enum (numeric measures)
        3. dimension: Enum if < distinct_limit values

        Returns:
            (samples_to_keep, is_enum)
        """
        # RULE 1: time_dimension never enum
        if column_type == "time_dimension":
            return sample_values[: self.display_limit], False

        # RULE 2: fact never enum
        if column_type == "fact":
            return sample_values[: self.display_limit], False

        # RULE 3: dimension enum detection
        if len(sample_values) == self.distinct_limit:
            # Hit limit - more values likely exist, not enum
            return sample_values[: self.display_limit], False
        else:
            # Got all values - it's an enum
            return sample_values, len(sample_values) > 0

    def _order_model_structure(self, model: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure proper model structure order: name, description, meta, config, columns

        Args:
            model: Model metadata dictionary

        Returns:
            Dict with properly ordered structure
        """
        ordered_model = {}

        # Desired order for model keys
        key_order = ["name", "description", "meta", "config", "columns"]

        # Add keys in the desired order
        for key in key_order:
            if key in model:
                ordered_model[key] = model[key]

        # Add any remaining keys that weren't in our order
        for key, value in model.items():
            if key not in ordered_model:
                ordered_model[key] = value

        return ordered_model

    def _serialize_yaml_for_llm(self, full_yaml: Optional[Dict[str, Any]]) -> Optional[str]:
        """
        Serialize YAML dict to string for LLM context.

        Converts Python dict to clean YAML string that LLMs can understand.
        Used for providing full context to Cortex synonym generation.
        """
        if not full_yaml:
            return None

        # Convert to JSON and back to clean Python types (removes non-serializable objects)
        clean_yaml = json.loads(json.dumps(full_yaml, default=str))

        # Serialize to YAML
        return yaml.dump(clean_yaml, default_flow_style=False, sort_keys=False)

    def _enrich_table_synonyms(
        self,
        model_data: Dict[str, Any],
        column_data: List[Dict[str, Any]],
        full_yaml: Optional[Dict[str, Any]] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        Enrich table-level synonyms using Cortex with full YAML context.

        Args:
            model_data: Model metadata dict
            column_data: List of column dicts
            full_yaml: Complete YAML dict for LLM context
            force: Force regeneration

        Returns:
            Updated model_data with synonyms
        """
        meta_sst = model_data.get("meta", {}).get("sst", {})
        existing_synonyms = meta_sst.get("synonyms", [])

        if existing_synonyms and not force:
            logger.debug(f"Table {model_data['name']} already has synonyms, skipping")
            return model_data

        # Serialize YAML for LLM context
        yaml_context = self._serialize_yaml_for_llm(full_yaml)

        # Generate synonyms
        synonyms = self.synonym_generator.generate_table_synonyms(
            table_name=model_data["name"],
            description=model_data.get("description", ""),
            column_info=column_data,
            existing_synonyms=existing_synonyms,
            full_yaml_context=yaml_context,
            force=force,
        )

        # Update model data
        if "meta" not in model_data:
            model_data["meta"] = {}
        if "sst" not in model_data["meta"]:
            model_data["meta"]["sst"] = {}

        model_data["meta"]["sst"]["synonyms"] = synonyms
        logger.info(f"  Generated {len(synonyms)} table synonyms")

        return model_data

    def _enrich_column_synonyms(
        self,
        columns: List[Dict[str, Any]],
        table_name: str,
        table_description: Optional[str] = None,
        full_yaml: Optional[Dict[str, Any]] = None,
        force: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Enrich column-level synonyms using Cortex with full context.

        Batch-generates synonyms for all columns in one Cortex call for performance.
        """
        # Serialize YAML for LLM context
        yaml_context = self._serialize_yaml_for_llm(full_yaml)

        # Filter columns needing synonyms
        columns_needing_synonyms = [
            col for col in columns if force or not col.get("meta", {}).get("sst", {}).get("synonyms")
        ]

        if not columns_needing_synonyms:
            logger.debug(f"All columns in {table_name} already have synonyms")
            return columns

        # Batch generate
        logger.info(f"  Batch generating synonyms for {len(columns_needing_synonyms)} columns...")
        batch_synonyms = self.synonym_generator.generate_column_synonyms_batch(
            table_name=table_name,
            columns=columns_needing_synonyms,
            table_description=table_description,
            full_yaml_context=yaml_context,
            force=force,
        )

        # Apply results
        columns_with_synonyms = 0
        for col in columns:
            col_name = col["name"]

            if col_name in batch_synonyms and batch_synonyms[col_name]:
                if "meta" not in col:
                    col["meta"] = {}
                if "sst" not in col["meta"]:
                    col["meta"]["sst"] = {}

                col["meta"]["sst"]["synonyms"] = batch_synonyms[col_name]
                columns_with_synonyms += 1

        if columns_with_synonyms > 0:
            logger.info(f"  Enriched {columns_with_synonyms} columns with synonyms (batch mode)")

        return columns

    def _is_pii_protected_column(self, column: Dict[str, Any]) -> bool:
        """
        Check if a column is PII protected and should not have sample values.

        Args:
            column: Column metadata dictionary

        Returns:
            bool: True if column is PII protected (direct_identifier)
        """
        # Check for PII tags in meta
        if "meta" in column and "pii_tags" in column["meta"]:
            pii_tags = column["meta"]["pii_tags"]

            # Only protect direct_identifier privacy category
            if pii_tags.get("privacy_category") == "direct_identifier":
                return True

        return False
