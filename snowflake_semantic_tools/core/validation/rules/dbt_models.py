"""
dbt Model Validator

Ensures dbt models meet requirements for semantic model generation.

Validates the physical layer foundation that semantic models build upon,
checking that dbt models have the necessary metadata for Cortex Analyst
to understand the data structure and generate accurate queries.

Validation includes required fields, data type consistency, and best
practices that improve the quality of generated semantic models.
"""

from typing import Any, Dict, List, Optional, Set, Tuple

from snowflake_semantic_tools.core.models import ValidationResult
from snowflake_semantic_tools.core.models.validation import ValidationSeverity
from snowflake_semantic_tools.core.parsing.parsers.data_extractors import get_sst_meta
from snowflake_semantic_tools.shared.utils import get_logger
from snowflake_semantic_tools.shared.utils.character_sanitizer import CharacterSanitizer

logger = get_logger("dbt_model_validator")


class DbtModelValidator:
    """
    Validates dbt models for semantic layer compatibility.

    Enforces Requirements:
    - **Primary Keys**: Required for relationship definitions
    - **Column Metadata**: column_type (dimension/fact/time_dimension) for categorization
    - **Data Types**: Valid Snowflake types for SQL generation

    Best Practice Checks:
    - Descriptions for business context
    - Sample values for better AI understanding
    - Synonyms for natural language mapping
    - Logical consistency (primary keys exist as columns)

    Database and Schema Resolution:
    - Database and schema are read exclusively from dbt's manifest.json
    - This ensures environment-correct values and avoids sync issues
    - No validation is performed on these fields (dbt is source of truth)

    These validations ensure dbt models provide sufficient metadata
    for high-quality semantic model generation.
    """

    # Valid values for column_type
    VALID_COLUMN_TYPES = {"dimension", "time_dimension", "fact"}

    KNOWN_TABLE_SST_KEYS = {
        "primary_key", "unique_keys", "synonyms", "constraints", "tags",
        "table", "database", "schema", "exclude",
    }

    KNOWN_COLUMN_SST_KEYS = {
        "column_type", "data_type", "synonyms", "sample_values", "is_enum",
        "visibility", "tags", "exclude",
    }

    REMOVED_SST_KEYS = {
        "cortex_searchable": "Cortex Search Service support was removed in v0.3.0",
    }

    # Valid Snowflake data types (common ones)
    VALID_DATA_TYPES = {
        # Numeric types
        "number",
        "decimal",
        "numeric",
        "int",
        "integer",
        "bigint",
        "smallint",
        "tinyint",
        "byteint",
        "float",
        "float4",
        "float8",
        "double",
        "double precision",
        "real",
        # String types
        "varchar",
        "char",
        "character",
        "string",
        "text",
        # Date/time types
        "date",
        "datetime",
        "time",
        "timestamp",
        "timestamp_ltz",
        "timestamp_ntz",
        "timestamp_tz",
        # Boolean type
        "boolean",
        "bool",
        # Semi-structured types
        "variant",
        "object",
        "array",
        # Binary type
        "binary",
        "varbinary",
    }

    def validate(self, dbt_data: Dict[str, Any]) -> ValidationResult:
        """
        Validate dbt model definitions.

        Args:
            dbt_data: Parsed dbt model data

        Returns:
            ValidationResult with all issues found
        """
        result = ValidationResult()

        # Get tables data - try different keys
        tables = dbt_data.get("sm_tables", [])
        if not tables:
            tables_data = dbt_data.get("tables", {})
            if isinstance(tables_data, dict) and "items" in tables_data:
                tables = tables_data.get("items", [])
            else:
                tables = tables_data if isinstance(tables_data, list) else []

        # Get dimensions, facts, and time_dimensions data
        dimensions_data = dbt_data.get("sm_dimensions", {})
        dimensions = self._extract_items(dimensions_data)

        facts_data = dbt_data.get("sm_facts", {})
        facts = self._extract_items(facts_data)

        time_dimensions_data = dbt_data.get("sm_time_dimensions", {})
        time_dimensions = self._extract_items(time_dimensions_data)

        # Get all models for comprehensive checking
        models = dbt_data.get("models", [])

        # Log what we're validating
        if tables:
            logger.debug(f"Validating {len(tables)} dbt models with metadata")

        # Track skipped tables
        skipped_tables = []

        # Validate each table/model
        for table in tables:
            table_name = table.get("table_name", "unknown")

            # Check if table has critical missing metadata
            should_skip, missing_fields = self._should_skip_table(table, table_name)
            if should_skip:
                skipped_tables.append((table_name, missing_fields, table.get("source_file")))
                continue

            self._validate_table(table, result, dimensions, facts, time_dimensions)

        # Check for models that should be included but aren't
        self._check_missing_models(models, tables, result)

        # Report each skipped table as an individual warning
        if skipped_tables:
            for table_name, missing_fields, source_file in sorted(skipped_tables):
                missing_fields_str = ", ".join(missing_fields)
                result.add_warning(
                    f"Model '{table_name}' will be excluded from extraction due to missing metadata ({missing_fields_str})",
                    file_path=source_file,
                    rule_id="SST-V010",
                    suggestion="Add primary_key to config.meta.sst",
                    entity_name=table_name,
                    context={"table": table_name, "reason": "missing_metadata", "missing_fields": missing_fields},
                )
            logger.warning(f"Skipped validation for {len(skipped_tables)} tables with missing metadata")

        # Log final summary
        validated_count = len(
            [
                i
                for i in result.issues
                if i.severity == ValidationSeverity.INFO and "passed all validation checks" in i.message
            ]
        )
        if validated_count > 0:
            logger.debug(f"Successfully validated {validated_count} models without issues")
        if skipped_tables:
            logger.debug(f"Skipped {len(skipped_tables)} models with missing metadata")

        return result

    def _extract_items(self, data: Any) -> List[Dict[str, Any]]:
        """Extract items from various data structures."""
        if isinstance(data, dict) and "items" in data:
            return data.get("items", [])
        elif isinstance(data, list):
            return data
        return []

    def _should_skip_table(self, table: Dict[str, Any], table_name: str) -> tuple[bool, List[str]]:
        """
        Check if a table should be skipped due to missing critical metadata.

        Args:
            table: Table dictionary
            table_name: Name of the table

        Returns:
            Tuple of (should_skip, missing_fields)
        """
        # Check for critical missing fields
        # Note: database and schema are now optional (can come from manifest.json)
        # Only primary_key is truly critical
        critical_fields = ["primary_key"]
        missing_fields = []

        for field in critical_fields:
            value = table.get(field)
            if not value or (isinstance(value, list) and len(value) == 0):
                missing_fields.append(field)

        # If any critical field is missing, skip the table
        if missing_fields:
            logger.debug(f"Table '{table_name}' missing critical metadata: {', '.join(missing_fields)}")
            return True, missing_fields

        return False, []

    def _validate_table(
        self,
        table: Dict[str, Any],
        result: ValidationResult,
        dimensions: List[Dict[str, Any]],
        facts: List[Dict[str, Any]],
        time_dimensions: List[Dict[str, Any]],
    ):
        """Validate a single table/model."""
        table_name = table.get("table_name", "unknown")
        initial_error_count = result.error_count
        initial_warning_count = result.warning_count

        # Check required table-level fields
        self._check_required_table_fields(table, table_name, result)

        self._check_unrecognized_sst_keys(table, table_name, result)

        # Check naming conventions
        self._check_naming_conventions(table, table_name, result)

        # Check primary key validity
        self._check_primary_key(table, table_name, dimensions, facts, time_dimensions, result)

        # Check table synonym content (apostrophes cause SQL errors)
        self._check_table_synonym_content(table, table_name, result)

        # Check for best practices
        self._check_table_best_practices(table, table_name, result)

        # Validate constraints (DISTINCT RANGE)
        self._check_constraints(table, table_name, dimensions, facts, time_dimensions, result)

        # Validate tags
        if table.get("tags"):
            from snowflake_semantic_tools.core.validation.rules.semantic_models import SemanticModelValidator

            validator = SemanticModelValidator()
            validator._validate_tags(table["tags"], f"Table '{table_name}'", table.get("source_file"), result)

        # Validate columns
        all_columns = dimensions + facts + time_dimensions
        table_columns = [c for c in all_columns if c.get("table_name", "").upper() == table_name.upper()]

        for column in table_columns:
            self._validate_column(column, table_name, result)

        # Log if this table passed all validations
        if result.error_count == initial_error_count and result.warning_count == initial_warning_count:
            logger.debug(f"Table '{table_name}' passed all validation checks")
            # Add to result as SUCCESS for tables that passed all checks
            result.add_success(f"Table '{table_name}' passed all validation checks")

    def _check_required_table_fields(self, table: Dict[str, Any], table_name: str, result: ValidationResult):
        """
        Check that required table-level fields are present and non-empty.

        Note: Database and schema are read exclusively from manifest.json.
        Only primary_key and description are required in YAML.
        """
        source_file = table.get("source_file")

        # Check description (always required in YAML)
        description = table.get("description")
        if not description or (isinstance(description, str) and not description.strip()):
            result.add_warning(
                f"Model '{table_name}' is missing description (recommended for Cortex Analyst)",
                file_path=source_file,
                rule_id="SST-V012",
                suggestion="Add a description to the model definition",
                entity_name=table_name,
                context={"table": table_name, "field": "description", "level": "table"},
            )

        # Check primary_key (always required in YAML)
        primary_key = table.get("primary_key")
        if not primary_key:
            result.add_error(
                f"Model '{table_name}' is missing required field: meta.sst.primary_key",
                file_path=source_file,
                rule_id="SST-V010",
                suggestion="Add primary_key: column_name to config.meta.sst",
                entity_name=table_name,
                context={"table": table_name, "field": "meta.sst.primary_key", "level": "table"},
            )
        elif primary_key == []:
            result.add_error(
                f"Model '{table_name}' has empty primary key list",
                file_path=source_file,
                rule_id="SST-V010",
                suggestion="primary_key must contain at least one column",
                entity_name=table_name,
                context={"table": table_name, "field": "meta.sst.primary_key", "level": "table"},
            )

    def _check_unrecognized_sst_keys(self, table: Dict[str, Any], table_name: str, result: ValidationResult):
        """Warn about unrecognized keys in the sst meta block."""
        raw_keys = table.get("_raw_sst_keys", [])
        source_file = table.get("source_file")
        for key in raw_keys:
            if key in self.REMOVED_SST_KEYS:
                result.add_warning(
                    f"Model '{table_name}' uses removed key '{key}' in config.meta.sst. "
                    f"{self.REMOVED_SST_KEYS[key]}. This key is ignored.",
                    file_path=source_file,
                    context={"table": table_name, "key": key, "level": "table"},
                )
            elif key not in self.KNOWN_TABLE_SST_KEYS:
                result.add_warning(
                    f"Model '{table_name}' has unrecognized key '{key}' in config.meta.sst. "
                    f"This key will be ignored. Known keys: {', '.join(sorted(self.KNOWN_TABLE_SST_KEYS))}",
                    file_path=source_file,
                    context={"table": table_name, "key": key, "level": "table"},
                )

    def _check_primary_key(
        self,
        table: Dict[str, Any],
        table_name: str,
        dimensions: List[Dict[str, Any]],
        facts: List[Dict[str, Any]],
        time_dimensions: List[Dict[str, Any]],
        result: ValidationResult,
    ):
        """Check that primary key columns actually exist."""
        source_file = table.get("source_file")
        primary_keys = table.get("primary_key", [])

        # Validate that primary_key is a list
        if primary_keys and not isinstance(primary_keys, list):
            # Handle single string primary key
            if isinstance(primary_keys, str):
                # Handle comma-separated string
                if "," in primary_keys:
                    primary_keys = [key.strip() for key in primary_keys.split(",")]
                else:
                    primary_keys = [primary_keys]
                result.add_warning(
                    f"Model '{table_name}' has primary_key as string instead of list",
                    file_path=source_file,
                    rule_id="SST-V005",
                    suggestion="Use a YAML list:\\n  primary_key:\\n    - column_name",
                    entity_name=table_name,
                    context={"table": table_name, "field": "primary_key", "level": "table"},
                )
            else:
                result.add_error(
                    f"Model '{table_name}' has primary_key as {type(primary_keys).__name__} instead of list",
                    file_path=source_file,
                    rule_id="SST-V005",
                    suggestion="primary_key must be a list of column names",
                    entity_name=table_name,
                    context={
                        "table": table_name,
                        "field": "primary_key",
                        "type": type(primary_keys).__name__,
                        "level": "table",
                    },
                )
                return

        if not primary_keys:
            return  # Already reported as missing required field

        # Get all column names for this table (normalized to uppercase for comparison)
        all_columns = dimensions + facts + time_dimensions
        table_columns = [
            c.get("name", "").upper() for c in all_columns if c.get("table_name", "").upper() == table_name.upper()
        ]

        # Check each primary key exists (case-insensitive comparison)
        for pk in primary_keys:
            # Normalize primary key for comparison
            pk_normalized = pk.upper().strip()

            if pk_normalized not in table_columns:
                result.add_error(
                    f"Model '{table_name}' has primary key '{pk}' that doesn't exist as a column",
                    file_path=source_file,
                    rule_id="SST-V011",
                    suggestion="Check column name spelling — column must exist in the model",
                    entity_name=table_name,
                    context={"table": table_name, "primary_key": pk, "level": "table"},
                )

        unique_keys = table.get("unique_keys", [])
        if unique_keys and isinstance(unique_keys, list):
            pk_set = {p.upper().strip() for p in primary_keys}
            uk_set = {u.upper().strip() for u in unique_keys if isinstance(u, str)}
            overlap = pk_set & uk_set
            if overlap:
                result.add_error(
                    f"Table '{table_name}' has columns in both primary_key and unique_keys: "
                    f"{sorted(overlap)}. Snowflake rejects duplicate primary/unique key declarations.",
                    file_path=source_file,
                    rule_id="SST-V048",
                    suggestion="Remove overlapping columns from unique_keys — primary_key is already unique",
                    entity_name=table_name,
                    context={"table": table_name, "overlap": sorted(overlap)},
                )

    def _check_table_synonym_content(self, table: Dict[str, Any], table_name: str, result: ValidationResult):
        """Check that table synonyms don't contain characters that break SQL generation."""
        source_file = table.get("source_file")
        synonyms = table.get("synonyms")
        if synonyms and isinstance(synonyms, list):
            # Use the same sanitization logic as generation (DRY principle)
            sanitized_synonyms = CharacterSanitizer.sanitize_synonym_list(synonyms)

            # Only warn if sanitization would change anything (but don't modify the data)
            if sanitized_synonyms != synonyms:
                # Find first problematic synonym to show as example
                example = None
                for orig, cleaned in zip(synonyms, sanitized_synonyms):
                    if orig != cleaned:
                        example = f"'{orig}' → '{cleaned}'"
                        break

                example_text = f" (e.g., {example})" if example else ""
                result.add_warning(
                    f"Table '{table_name}' has synonyms with problematic characters. "
                    f"These will be automatically sanitized during generation{example_text}.",
                    file_path=source_file,
                    rule_id="SST-V014",
                    suggestion="Run: sst format --sanitize",
                    entity_name=table_name,
                    context={"table": table_name, "level": "table"},
                )

    def _check_table_best_practices(self, table: Dict[str, Any], table_name: str, result: ValidationResult):
        """Check for best practices at the table level."""
        source_file = table.get("source_file")
        # Description is required, checked in _check_required_table_fields

        # Validate synonyms is a list if present
        synonyms = table.get("synonyms")
        if synonyms is not None and not isinstance(synonyms, list):
            result.add_error(
                f"Model '{table_name}' has synonyms as {type(synonyms).__name__} instead of list",
                file_path=source_file,
                rule_id="SST-V013",
                suggestion="synonyms must be a YAML list",
                entity_name=table_name,
                context={"table": table_name, "field": "synonyms", "type": type(synonyms).__name__, "level": "table"},
            )

        # Check for synonyms
        if not synonyms:
            result.add_warning(
                f"Model '{table_name}' has no synonyms defined (helpful for natural language queries)",
                file_path=source_file,
                rule_id="SST-V013",
                suggestion="Add synonyms for better natural language queries",
                entity_name=table_name,
                context={"table": table_name, "best_practice": "synonyms", "level": "table"},
            )

    def _check_naming_conventions(self, table: Dict[str, Any], table_name: str, result: ValidationResult):
        """Check naming conventions for table name."""
        source_file = table.get("source_file")
        # Get model name if available
        model_name = table.get("model_name", "")

        # Check: Table name should match model name (case-insensitive)
        if model_name and table_name.upper() != model_name.upper():
            result.add_error(
                f"Table name '{table_name}' doesn't match model name '{model_name}'. They should be the same.",
                file_path=source_file,
                rule_id="SST-V004",
                suggestion="Table name and dbt model name must match",
                entity_name=table_name,
                context={"table": table_name, "model_name": model_name, "level": "table"},
            )

    def _check_constraints(
        self,
        table: Dict[str, Any],
        table_name: str,
        dimensions: List[Dict[str, Any]],
        facts: List[Dict[str, Any]],
        time_dimensions: List[Dict[str, Any]],
        result: ValidationResult,
    ):
        constraints = table.get("constraints", [])
        if not constraints:
            return
        source_file = table.get("source_file")
        all_columns = dimensions + facts + time_dimensions
        table_col_names = {
            c.get("name", "").upper() for c in all_columns if c.get("table_name", "").upper() == table_name.upper()
        }
        for constraint in constraints:
            if not isinstance(constraint, dict):
                continue
            if constraint.get("type") == "distinct_range":
                start_col = constraint.get("start_column", "").upper()
                end_col = constraint.get("end_column", "").upper()
                c_name = constraint.get("name", "unnamed")
                if not start_col:
                    result.add_error(
                        f"Table '{table_name}' constraint '{c_name}' missing required 'start_column'",
                        file_path=source_file,
                        rule_id="SST-V015",
                        suggestion="Add start_column to the constraint definition",
                        entity_name=table_name,
                        context={"table": table_name, "constraint": c_name},
                    )
                elif table_col_names and start_col not in table_col_names:
                    result.add_error(
                        f"Table '{table_name}' constraint '{c_name}' start_column '{start_col}' not found in table columns",
                        file_path=source_file,
                        rule_id="SST-V015",
                        suggestion="Check column name spelling — column must exist in the model",
                        entity_name=table_name,
                        context={"table": table_name, "constraint": c_name, "column": start_col},
                    )
                if not end_col:
                    result.add_error(
                        f"Table '{table_name}' constraint '{c_name}' missing required 'end_column'",
                        file_path=source_file,
                        rule_id="SST-V015",
                        suggestion="Add end_column to the constraint definition",
                        entity_name=table_name,
                        context={"table": table_name, "constraint": c_name},
                    )
                elif table_col_names and end_col not in table_col_names:
                    result.add_error(
                        f"Table '{table_name}' constraint '{c_name}' end_column '{end_col}' not found in table columns",
                        file_path=source_file,
                        rule_id="SST-V015",
                        suggestion="Check column name spelling — column must exist in the model",
                        entity_name=table_name,
                        context={"table": table_name, "constraint": c_name, "column": end_col},
                    )
                if start_col and end_col and start_col == end_col:
                    result.add_error(
                        f"Table '{table_name}' constraint '{c_name}' start_column and end_column cannot be the same",
                        file_path=source_file,
                        rule_id="SST-V015",
                        suggestion="start_column and end_column must be different columns",
                        entity_name=table_name,
                        context={"table": table_name, "constraint": c_name},
                    )

    def _validate_column(self, column: Dict[str, Any], table_name: str, result: ValidationResult):
        """Validate a single column."""
        column_name = column.get("name", "unknown")
        source_file = column.get("source_file")

        if column.get("exclude"):
            if column.get("column_type"):
                result.add_warning(
                    f"Column '{column_name}' in table '{table_name}' has both 'exclude: true' and "
                    f"'column_type: {column.get('column_type')}'. The column will be excluded from "
                    f"the semantic view — column_type is ignored.",
                    file_path=source_file,
                    rule_id="SST-V047",
                    suggestion="Remove column_type or remove exclude: true",
                    entity_name=column_name,
                    context={"table": table_name, "column": column_name},
                )
            return

        # Check required column fields
        self._check_required_column_fields(column, table_name, column_name, source_file, result)

        # Check valid values
        self._check_column_valid_values(column, table_name, column_name, source_file, result)

        # Check logical consistency
        self._check_column_consistency(column, table_name, column_name, source_file, result)

        # Check synonym content (apostrophes cause SQL errors)
        self._check_synonym_content(column, table_name, column_name, source_file, result)

        # Check for data type conflicts between dbt and sst
        self._check_data_type_mismatch(column, table_name, column_name, source_file, result)

        # Check best practices
        self._check_column_best_practices(column, table_name, column_name, source_file, result)

    def _check_required_column_fields(
        self,
        column: Dict[str, Any],
        table_name: str,
        column_name: str,
        source_file: Optional[str],
        result: ValidationResult,
    ):
        """Check required column fields."""
        # column_type is REQUIRED and must be valid
        column_type = column.get("column_type")
        if not column_type:
            result.add_error(
                f"Column '{column_name}' in table '{table_name}' is missing required field: meta.sst.column_type",
                file_path=source_file,
                rule_id="SST-V021",
                suggestion="Add column_type: dimension or fact or time_dimension",
                entity_name=column_name,
                context={"table": table_name, "column": column_name, "field": "column_type", "level": "column"},
            )
        elif column_type not in self.VALID_COLUMN_TYPES:
            result.add_error(
                f"Column '{column_name}' in table '{table_name}' has invalid column_type: '{column_type}'. Must be one of: {', '.join(sorted(self.VALID_COLUMN_TYPES))}",
                file_path=source_file,
                rule_id="SST-V007",
                suggestion="Must be one of: dimension, fact, time_dimension",
                entity_name=column_name,
                context={
                    "table": table_name,
                    "column": column_name,
                    "field": "column_type",
                    "column_type": column_type,
                    "value": column_type,
                    "level": "column",
                },
            )

        # data_type is required for all column types
        # Can be specified in native dbt contracts (column.data_type) or SST metadata (config.meta.sst.data_type)
        if not column.get("data_type"):
            result.add_error(
                f"Column '{column_name}' in table '{table_name}' is missing required field: data_type. "
                f"Specify either as native dbt contract (column.data_type) or SST metadata (config.meta.sst.data_type)",
                file_path=source_file,
                rule_id="SST-V022",
                suggestion="Add data_type (e.g., TEXT, NUMBER, TIMESTAMP_NTZ)",
                entity_name=column_name,
                context={"table": table_name, "column": column_name, "field": "data_type", "level": "column"},
            )

        # Description is REQUIRED (not just technically)
        if not column.get("description"):
            result.add_warning(
                f"Column '{column_name}' in model '{table_name}' is missing description (recommended for Cortex Analyst)",
                file_path=source_file,
                rule_id="SST-V020",
                suggestion="Add a description to the column",
                entity_name=column_name,
                context={"table": table_name, "column": column_name, "field": "description", "level": "column"},
            )

        # Visibility validation: only facts and metrics can be private
        visibility = column.get("visibility")
        if visibility:
            if visibility.lower() not in ("public", "private"):
                result.add_error(
                    f"Column '{column_name}' in table '{table_name}' has invalid visibility: '{visibility}'. Must be 'public' or 'private'",
                    file_path=source_file,
                    rule_id="SST-V035",
                    suggestion="visibility must be 'public' or 'private'",
                    entity_name=column_name,
                    context={"table": table_name, "column": column_name, "field": "visibility", "level": "column"},
                )
            elif visibility.lower() == "private" and column_type in ("dimension", "time_dimension"):
                result.add_error(
                    f"Column '{column_name}' in table '{table_name}' cannot be PRIVATE: only facts and metrics support visibility control (Snowflake restriction)",
                    file_path=source_file,
                    rule_id="SST-V035",
                    suggestion="Only facts support PRIVATE visibility (Snowflake restriction)",
                    entity_name=column_name,
                    context={"table": table_name, "column": column_name, "field": "visibility", "level": "column"},
                )

    def _determine_column_type(self, column: Dict[str, Any]) -> str:
        """Get the column type from metadata (no longer defaults or infers)."""
        # Column type must be explicitly set now - no defaults
        # Validation of this field happens in _check_required_column_fields
        return column.get("column_type", "")

    def _check_column_valid_values(
        self,
        column: Dict[str, Any],
        table_name: str,
        column_name: str,
        source_file: Optional[str],
        result: ValidationResult,
    ):
        """Check that column fields have valid values."""
        # Check data_type is valid - must be recognized Snowflake type
        data_type = column.get("data_type", "").lower()
        # Strip precision/scale for validation (e.g., "number(38,0)" -> "number")
        base_data_type = data_type.split("(")[0] if "(" in data_type else data_type
        if base_data_type and base_data_type not in self.VALID_DATA_TYPES:
            result.add_warning(
                f"Column '{column_name}' in model '{table_name}' has unrecognized data_type: '{data_type}'. "
                f"May not be a valid Snowflake data type.",
                file_path=source_file,
                rule_id="SST-V008",
                suggestion=f"Valid types: TEXT, NUMBER, FLOAT, BOOLEAN, DATE, TIMESTAMP_NTZ, TIMESTAMP_LTZ, VARIANT. Run: DESCRIBE TABLE <table> to check actual column types",
                entity_name=column_name,
                context={"table": table_name, "column": column_name, "data_type": data_type, "level": "column"},
            )

        # Note: column_type validation is handled in _check_required_column_fields

    def _check_column_consistency(
        self,
        column: Dict[str, Any],
        table_name: str,
        column_name: str,
        source_file: Optional[str],
        result: ValidationResult,
    ):
        """Check logical consistency of column configuration."""
        column_type = self._determine_column_type(column)
        data_type = column.get("data_type", "").lower()
        # Strip precision/scale for consistency checks (e.g., "number(38,0)" -> "number")
        base_data_type = data_type.split("(")[0] if "(" in data_type else data_type

        # Facts should have numeric data types
        if column_type == "fact":
            numeric_types = {
                "number",
                "decimal",
                "numeric",
                "int",
                "integer",
                "bigint",
                "smallint",
                "float",
                "double",
                "real",
            }
            if base_data_type and base_data_type not in numeric_types:
                result.add_error(
                    f"Fact column '{column_name}' in table '{table_name}' has non-numeric data_type: '{data_type}'",
                    file_path=source_file,
                    rule_id="SST-V023",
                    suggestion="Fact columns require numeric data types (NUMBER, INT, FLOAT, etc.)",
                    entity_name=column_name,
                    context={
                        "table": table_name,
                        "column": column_name,
                        "column_type": "fact",
                        "data_type": data_type,
                        "level": "column",
                    },
                )

        # Time dimensions should have date/time data types
        if column_type == "time_dimension":
            time_types = {"date", "datetime", "time", "timestamp", "timestamp_ltz", "timestamp_ntz", "timestamp_tz"}
            if base_data_type and base_data_type not in time_types:
                result.add_error(
                    f"Time dimension '{column_name}' in table '{table_name}' has non-temporal data_type: '{data_type}'",
                    file_path=source_file,
                    rule_id="SST-V024",
                    suggestion="Time dimensions require temporal types (DATE, TIMESTAMP, etc.)",
                    entity_name=column_name,
                    context={
                        "table": table_name,
                        "column": column_name,
                        "column_type": "time_dimension",
                        "data_type": data_type,
                        "level": "column",
                    },
                )

        # Validate list field types
        synonyms = column.get("synonyms")
        if synonyms is not None and not isinstance(synonyms, list):
            result.add_error(
                f"Column '{column_name}' in table '{table_name}' has synonyms as {type(synonyms).__name__} instead of list",
                file_path=source_file,
                rule_id="SST-V013",
                suggestion="synonyms must be a list",
                entity_name=column_name,
                context={
                    "table": table_name,
                    "column": column_name,
                    "field": "synonyms",
                    "type": type(synonyms).__name__,
                    "level": "column",
                },
            )

        sample_values = column.get("sample_values")
        if sample_values is not None and not isinstance(sample_values, list):
            result.add_error(
                f"Column '{column_name}' in table '{table_name}' has sample_values as {type(sample_values).__name__} instead of list",
                file_path=source_file,
                rule_id="SST-V005",
                suggestion="sample_values must be a list",
                entity_name=column_name,
                context={
                    "table": table_name,
                    "column": column_name,
                    "field": "sample_values",
                    "type": type(sample_values).__name__,
                    "level": "column",
                },
            )

        # Check PII protection - CRITICAL SECURITY CHECK
        # Direct identifier PII columns must NEVER have sample values
        privacy_category = column.get("privacy_category")
        sample_values_list = sample_values if isinstance(sample_values, list) else []

        if privacy_category == "direct_identifier" and sample_values_list:
            result.add_error(
                f"Column '{column_name}' in table '{table_name}' has privacy_category='direct_identifier' "
                f"but contains sample_values. PII columns must not expose sample data",
                file_path=source_file,
                rule_id="SST-V025",
                suggestion="Remove sample_values from PII columns (security requirement)",
                entity_name=column_name,
                context={
                    "table": table_name,
                    "column": column_name,
                    "privacy_category": privacy_category,
                    "level": "column",
                    "security": "PII_EXPOSURE",
                },
            )

        # Check for Jinja-breaking characters in sample values
        # These characters cause dbt compilation errors and must be sanitized
        if sample_values_list:
            jinja_patterns = ["{{{", "}}}", "{{", "}}", "{%", "%}", "{#", "#}"]
            problematic_values = []
            for sample_val in sample_values_list:
                val_str = str(sample_val)
                if any(pattern in val_str for pattern in jinja_patterns):
                    problematic_values.append(val_str[:50] + "..." if len(val_str) > 50 else val_str)

            if problematic_values:
                result.add_error(
                    f"Column '{column_name}' in table '{table_name}' contains sample_values with Jinja template "
                    f"characters that will break dbt compilation. Run 'sst enrich' to sanitize these values",
                    file_path=source_file,
                    rule_id="SST-V005",
                    suggestion="Remove {{ }} Jinja characters from sample values",
                    entity_name=column_name,
                    context={
                        "table": table_name,
                        "column": column_name,
                        "level": "column",
                        "validation": "JINJA_CHARACTERS_IN_SAMPLES",
                        "problematic_count": len(problematic_values),
                        "examples": problematic_values[:3],
                    },
                )

        # Check is_enum consistency
        is_enum = column.get("is_enum", False)

        # Fact and time_dimension columns should NEVER be enums
        # Facts are numeric measures, not categories
        # Time dimensions grow continuously, not discrete categories
        if is_enum and column_type in ["fact", "time_dimension"]:
            result.add_error(
                f"Column '{column_name}' in table '{table_name}' has is_enum=true but column_type='{column_type}'. "
                f"Fact and time_dimension columns should never be enums",
                file_path=source_file,
                rule_id="SST-V025",
                suggestion="is_enum is only valid for dimension columns",
                entity_name=column_name,
                context={
                    "table": table_name,
                    "column": column_name,
                    "column_type": column_type,
                    "level": "column",
                    "validation": "ENUM_TYPE_MISMATCH",
                },
            )

        if is_enum and not sample_values:
            result.add_warning(
                f"Column '{column_name}' in table '{table_name}' has is_enum=true but no sample_values",
                file_path=source_file,
                rule_id="SST-V025",
                suggestion="Add sample_values with the allowed enum values",
                entity_name=column_name,
                context={"table": table_name, "column": column_name, "level": "column"},
            )

    def _check_synonym_content(
        self,
        column: Dict[str, Any],
        table_name: str,
        column_name: str,
        source_file: Optional[str],
        result: ValidationResult,
    ):
        """Check that synonyms don't contain characters that break SQL generation."""
        synonyms = column.get("synonyms")
        if synonyms and isinstance(synonyms, list):
            # Use the same sanitization logic as generation (DRY principle)
            sanitized_synonyms = CharacterSanitizer.sanitize_synonym_list(synonyms)

            # Only warn if sanitization would change anything (but don't modify the data)
            if sanitized_synonyms != synonyms:
                # Find first problematic synonym to show as actual example
                example = None
                for orig, cleaned in zip(synonyms, sanitized_synonyms):
                    if orig != cleaned:
                        example = f"'{orig}' → '{cleaned}'"
                        break

                example_text = f" (e.g., {example})" if example else ""
                result.add_warning(
                    f"Column '{column_name}' in table '{table_name}' has synonyms with problematic characters. "
                    f"These will be automatically sanitized during generation{example_text}.",
                    file_path=source_file,
                    rule_id="SST-V014",
                    suggestion="Run: sst format --sanitize",
                    entity_name=column_name,
                    context={"table": table_name, "column": column_name, "level": "column"},
                )

    def _check_data_type_mismatch(
        self,
        column: Dict[str, Any],
        table_name: str,
        column_name: str,
        source_file: Optional[str],
        result: ValidationResult,
    ):
        """Check for conflicting data types between dbt and sst metadata."""
        native_data_type = column.get("_native_data_type")
        sst_data_type = column.get("_sst_data_type")

        # Only warn if both locations have conflicting values
        if native_data_type and sst_data_type:
            native_normalized = native_data_type.lower().strip()
            sst_normalized = sst_data_type.lower().strip()
            if native_normalized != sst_normalized:
                result.add_warning(
                    f"Column '{column_name}' in table '{table_name}' has conflicting data types: "
                    f"dbt data_type='{native_data_type}' vs sst data_type='{sst_data_type}'. "
                    f"Using dbt value '{native_data_type}'. "
                    f"Consider removing duplicate from config.meta.sst.data_type.",
                    file_path=source_file,
                    rule_id="SST-V008",
                    suggestion="Resolve mismatch between dbt contract data_type and sst data_type",
                    entity_name=column_name,
                    context={
                        "table": table_name,
                        "column": column_name,
                        "native_data_type": native_data_type,
                        "sst_data_type": sst_data_type,
                        "level": "column",
                    },
                )

    def _check_column_best_practices(
        self,
        column: Dict[str, Any],
        table_name: str,
        column_name: str,
        source_file: Optional[str],
        result: ValidationResult,
    ):
        """Check column best practices."""
        # Sample values are helpful for AI/BI tools
        if not column.get("sample_values"):
            # Only suggest for dimensions, not facts
            column_type = self._determine_column_type(column)
            if column_type == "dimension":
                result.add_info(
                    f"Consider adding sample_values for dimension '{column_name}' in table '{table_name}'",
                    file_path=source_file,
                    rule_id="SST-V025",
                    suggestion="Run: sst enrich to auto-populate sample values",
                    entity_name=column_name,
                    context={
                        "table": table_name,
                        "column": column_name,
                        "best_practice": "sample_values",
                        "level": "column",
                    },
                )

        # Synonyms can be helpful
        if not column.get("synonyms"):
            # Only for important columns (skip for very common ones like id, created_at, etc.)
            common_columns = {"id", "created_at", "updated_at", "deleted_at"}
            if column_name.lower() not in common_columns:
                result.add_info(
                    f"Consider adding synonyms for column '{column_name}' in table '{table_name}'",
                    file_path=source_file,
                    rule_id="SST-V013",
                    suggestion="Add synonyms for better AI discoverability",
                    entity_name=column_name,
                    context={
                        "table": table_name,
                        "column": column_name,
                        "best_practice": "synonyms",
                        "level": "column",
                    },
                )

    def _check_missing_models(
        self, all_models: List[Dict[str, Any]], included_tables: List[Dict[str, Any]], result: ValidationResult
    ):
        """Check for models that should be included but aren't in the extraction."""
        included_names = {t.get("table_name", "").upper() for t in included_tables}

        for model in all_models:
            model_name = model.get("name", "unknown")
            source_file = model.get("source_file")

            if model_name.upper() not in included_names:
                sst_meta = get_sst_meta(model, node_type="model", node_name=model_name, emit_warning=False)

                if not sst_meta:
                    result.add_info(
                        f"Model '{model_name}' has no meta.sst configuration (not included in semantic layer)",
                        file_path=source_file,
                        rule_id="SST-V010",
                        suggestion="Add config.meta.sst block to include model in semantic layer",
                        entity_name=model_name,
                        context={"model": model_name, "reason": "no_sst_meta"},
                    )
                else:
                    result.add_error(
                        f"Model '{model_name}' has SST config but wasn't extracted",
                        file_path=source_file,
                        rule_id="SST-V002",
                        suggestion="Model with SST config not found in extracted data",
                        entity_name=model_name,
                        context={"model": model_name, "reason": "extraction_failure"},
                    )
