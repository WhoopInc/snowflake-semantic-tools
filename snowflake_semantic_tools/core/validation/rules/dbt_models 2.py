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
                skipped_tables.append((table_name, missing_fields))
                continue

            self._validate_table(table, result, dimensions, facts, time_dimensions)

        # Check for models that should be included but aren't
        self._check_missing_models(models, tables, result)

        # Report each skipped table as an individual warning
        if skipped_tables:
            for table_name, missing_fields in sorted(skipped_tables):
                missing_fields_str = ", ".join(missing_fields)
                result.add_warning(
                    f"Table '{table_name}' skipped due to missing critical metadata ({missing_fields_str})",
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

        # Check naming conventions
        self._check_naming_conventions(table, table_name, result)

        # Check primary key validity
        self._check_primary_key(table, table_name, dimensions, facts, time_dimensions, result)

        # Check table synonym content (apostrophes cause SQL errors)
        self._check_table_synonym_content(table, table_name, result)

        # Check for best practices
        self._check_table_best_practices(table, table_name, result)

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
        # Check description (always required in YAML)
        description = table.get("description")
        if not description or (isinstance(description, str) and not description.strip()):
            result.add_error(
                f"Table '{table_name}' is missing required field: description at the table-level",
                context={"table": table_name, "field": "description", "level": "table"},
            )

        # Check primary_key (always required in YAML)
        primary_key = table.get("primary_key")
        if not primary_key:
            result.add_error(
                f"Table '{table_name}' is missing required field: meta.sst.primary_key at the table-level",
                context={"table": table_name, "field": "meta.sst.primary_key", "level": "table"},
            )
        elif primary_key == []:
            result.add_error(
                f"Table '{table_name}' has empty primary key list at the table-level",
                context={"table": table_name, "field": "meta.sst.primary_key", "level": "table"},
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
                    f"Table '{table_name}' has primary_key as string instead of list at the table-level",
                    context={"table": table_name, "field": "primary_key", "level": "table"},
                )
            else:
                result.add_error(
                    f"Table '{table_name}' has primary_key as {type(primary_keys).__name__} instead of list at the table-level",
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
                    f"Table '{table_name}' has primary key '{pk}' that doesn't exist as a column at the table-level",
                    context={"table": table_name, "primary_key": pk, "level": "table"},
                )

    def _check_table_synonym_content(self, table: Dict[str, Any], table_name: str, result: ValidationResult):
        """Check that table synonyms don't contain characters that break SQL generation."""
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
                    context={"table": table_name, "level": "table"},
                )

    def _check_table_best_practices(self, table: Dict[str, Any], table_name: str, result: ValidationResult):
        """Check for best practices at the table level."""
        # Description is required, checked in _check_required_table_fields

        # Validate synonyms is a list if present
        synonyms = table.get("synonyms")
        if synonyms is not None and not isinstance(synonyms, list):
            result.add_error(
                f"Table '{table_name}' has synonyms as {type(synonyms).__name__} instead of list at the table-level",
                context={"table": table_name, "field": "synonyms", "type": type(synonyms).__name__, "level": "table"},
            )

        # Check for synonyms
        if not synonyms:
            result.add_warning(
                f"Table '{table_name}' has no synonyms defined at the table-level (helpful for natural language queries)",
                context={"table": table_name, "best_practice": "synonyms", "level": "table"},
            )

    def _check_naming_conventions(self, table: Dict[str, Any], table_name: str, result: ValidationResult):
        """Check naming conventions for table name."""
        # Get model name if available
        model_name = table.get("model_name", "")

        # Check: Table name should match model name (case-insensitive)
        if model_name and table_name.upper() != model_name.upper():
            result.add_error(
                f"Table name '{table_name}' doesn't match model name '{model_name}'. They should be the same.",
                context={"table": table_name, "model_name": model_name, "level": "table"},
            )

    def _validate_column(self, column: Dict[str, Any], table_name: str, result: ValidationResult):
        """Validate a single column."""
        column_name = column.get("name", "unknown")

        # Check required column fields
        self._check_required_column_fields(column, table_name, column_name, result)

        # Check valid values
        self._check_column_valid_values(column, table_name, column_name, result)

        # Check logical consistency
        self._check_column_consistency(column, table_name, column_name, result)

        # Check synonym content (apostrophes cause SQL errors)
        self._check_synonym_content(column, table_name, column_name, result)

        # Check best practices
        self._check_column_best_practices(column, table_name, column_name, result)

    def _check_required_column_fields(
        self, column: Dict[str, Any], table_name: str, column_name: str, result: ValidationResult
    ):
        """Check required column fields."""
        # column_type is REQUIRED and must be valid
        column_type = column.get("column_type")
        if not column_type:
            result.add_error(
                f"Column '{column_name}' in table '{table_name}' is missing required field: meta.sst.column_type at the column-level",
                context={"table": table_name, "column": column_name, "field": "column_type", "level": "column"},
            )
        elif column_type not in self.VALID_COLUMN_TYPES:
            result.add_error(
                f"Column '{column_name}' in table '{table_name}' has invalid column_type: '{column_type}'. Must be one of: {', '.join(sorted(self.VALID_COLUMN_TYPES))}",
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
        if not column.get("data_type"):
            result.add_error(
                f"Column '{column_name}' in table '{table_name}' is missing required field: meta.sst.data_type at the column-level",
                context={"table": table_name, "column": column_name, "field": "data_type", "level": "column"},
            )

        # Description is REQUIRED (not just technically)
        if not column.get("description"):
            result.add_error(
                f"Column '{column_name}' in table '{table_name}' is missing required field: description at the column-level",
                context={"table": table_name, "column": column_name, "field": "description", "level": "column"},
            )

    def _determine_column_type(self, column: Dict[str, Any]) -> str:
        """Get the column type from metadata (no longer defaults or infers)."""
        # Column type must be explicitly set now - no defaults
        # Validation of this field happens in _check_required_column_fields
        return column.get("column_type", "")

    def _check_column_valid_values(
        self, column: Dict[str, Any], table_name: str, column_name: str, result: ValidationResult
    ):
        """Check that column fields have valid values."""
        # Check data_type is valid - must be recognized Snowflake type
        data_type = column.get("data_type", "").lower()
        if data_type and data_type not in self.VALID_DATA_TYPES:
            result.add_error(
                f"Column '{column_name}' in table '{table_name}' has unrecognized data_type: '{data_type}' at the column-level. "
                f"Must be a valid Snowflake data type.",
                context={"table": table_name, "column": column_name, "data_type": data_type, "level": "column"},
            )

        # Note: column_type validation is handled in _check_required_column_fields

    def _check_column_consistency(
        self, column: Dict[str, Any], table_name: str, column_name: str, result: ValidationResult
    ):
        """Check logical consistency of column configuration."""
        column_type = self._determine_column_type(column)
        data_type = column.get("data_type", "").lower()

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
            if data_type and data_type not in numeric_types:
                result.add_error(
                    f"Fact column '{column_name}' in table '{table_name}' has non-numeric data_type: '{data_type}' at the column-level",
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
            if data_type and data_type not in time_types:
                result.add_error(
                    f"Time dimension '{column_name}' in table '{table_name}' has non-temporal data_type: '{data_type}' at the column-level",
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
                f"Column '{column_name}' in table '{table_name}' has synonyms as {type(synonyms).__name__} instead of list at the column-level",
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
                f"Column '{column_name}' in table '{table_name}' has sample_values as {type(sample_values).__name__} instead of list at the column-level",
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
                f"but contains sample_values. PII columns must not expose sample data at the column-level",
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
                    f"characters that will break dbt compilation. Run 'sst enrich' to sanitize these values at the column-level",
                    context={
                        "table": table_name,
                        "column": column_name,
                        "level": "column",
                        "validation": "JINJA_CHARACTERS_IN_SAMPLES",
                        "problematic_count": len(problematic_values),
                        "examples": problematic_values[:3],  # Show up to 3 examples
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
                f"Fact and time_dimension columns should never be enums at the column-level",
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
                f"Column '{column_name}' in table '{table_name}' has is_enum=true but no sample_values at the column-level",
                context={"table": table_name, "column": column_name, "level": "column"},
            )

    def _check_synonym_content(
        self, column: Dict[str, Any], table_name: str, column_name: str, result: ValidationResult
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
                    context={"table": table_name, "column": column_name, "level": "column"},
                )

    def _check_column_best_practices(
        self, column: Dict[str, Any], table_name: str, column_name: str, result: ValidationResult
    ):
        """Check column best practices."""
        # Sample values are helpful for AI/BI tools
        if not column.get("sample_values"):
            # Only suggest for dimensions, not facts
            column_type = self._determine_column_type(column)
            if column_type == "dimension":
                result.add_info(
                    f"Consider adding sample_values for dimension '{column_name}' in table '{table_name}' at the column-level",
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
                    f"Consider adding synonyms for column '{column_name}' in table '{table_name}' at the column-level",
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
        # This would catch models that have cortex_searchable=false or are missing metadata

        included_names = {t.get("table_name", "").upper() for t in included_tables}

        for model in all_models:
            model_name = model.get("name", "unknown")

            # Process all models - no hardcoded exclusions
            model_path = model.get("path", "")

            # Check if this model is in the extracted tables
            if model_name.upper() not in included_names:
                # Check why it's not included
                meta = model.get("meta", {})
                sst_meta = meta.get("sst", {})

                if not sst_meta:
                    result.add_info(
                        f"Model '{model_name}' has no meta.sst configuration (not included in semantic layer)",
                        context={"model": model_name, "reason": "no_sst_meta"},
                    )
                else:
                    # Check cortex_searchable
                    cortex_searchable = sst_meta.get("cortex_searchable", False)

                    if not cortex_searchable:
                        # This is intentional, just log as info
                        result.add_info(
                            f"Model '{model_name}' has cortex_searchable=false (intentionally excluded)",
                            context={"model": model_name, "reason": "excluded"},
                        )
                    else:
                        # This shouldn't happen, but if it does, it's an error
                        result.add_error(
                            f"Model '{model_name}' has cortex_searchable=true but wasn't extracted",
                            context={"model": model_name, "reason": "extraction_failure"},
                        )
