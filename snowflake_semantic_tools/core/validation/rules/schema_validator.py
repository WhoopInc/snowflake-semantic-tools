"""
Schema Validator

Validates YAML column definitions against actual Snowflake table schemas.

This validator connects to Snowflake to verify that columns referenced in
dbt model YAML files actually exist in the physical tables. This catches
schema drift and typos that would otherwise cause runtime errors.

Requires Snowflake credentials and is only run when --verify-schema flag is used.
"""

from typing import Any, Dict, List, Optional, Set

from snowflake_semantic_tools.core.models import ValidationResult
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("schema_validator")


class SchemaValidator:
    """
    Validates YAML columns against actual Snowflake schema.

    Connects to Snowflake to retrieve actual table schemas and compares
    them against the columns defined in dbt model YAML files.

    Features:
    - Detects columns in YAML that don't exist in Snowflake
    - Provides "Did you mean?" suggestions for typos
    - Reports Snowflake columns missing from YAML (optional)
    - Handles permission errors gracefully
    """

    def __init__(self, metadata_manager):
        """
        Initialize the schema validator.

        Args:
            metadata_manager: MetadataManager instance for Snowflake queries
        """
        self.metadata_manager = metadata_manager

    def validate(
        self,
        dbt_catalog: Dict[str, Any],
        check_missing_in_yaml: bool = False,
    ) -> ValidationResult:
        """
        Compare YAML columns against actual Snowflake schema.

        Args:
            dbt_catalog: Catalog of dbt models with columns from YAML
            check_missing_in_yaml: If True, also report Snowflake columns not in YAML

        Returns:
            ValidationResult with schema validation issues
        """
        result = ValidationResult()

        tables_checked = 0
        tables_skipped = 0

        for table_name, table_info in dbt_catalog.items():
            database = table_info.get("database")
            schema = table_info.get("schema")

            # Skip tables without location info
            if not database or not schema:
                tables_skipped += 1
                logger.debug(f"Skipping {table_name}: missing database/schema")
                continue

            # Get actual Snowflake columns
            try:
                sf_columns = self.metadata_manager.get_table_schema(table_name, schema, database)
                sf_column_names = {c["name"].upper() for c in sf_columns}
                tables_checked += 1
            except Exception as e:
                error_msg = str(e).lower()
                if "does not exist" in error_msg:
                    result.add_error(
                        f"Table '{table_name}' does not exist in Snowflake at "
                        f"{database}.{schema}.{table_name.upper()}",
                        context={
                            "table": table_name,
                            "database": database,
                            "schema": schema,
                            "issue": "table_not_found",
                        },
                    )
                elif "not authorized" in error_msg:
                    result.add_warning(
                        f"Cannot verify schema for '{table_name}': permission denied. "
                        f"Check that your role has access to {database}.{schema}",
                        context={
                            "table": table_name,
                            "database": database,
                            "schema": schema,
                            "issue": "permission_denied",
                        },
                    )
                else:
                    result.add_warning(
                        f"Could not verify schema for '{table_name}': {e}",
                        context={
                            "table": table_name,
                            "issue": "connection_error",
                        },
                    )
                continue

            # Get YAML columns
            yaml_columns = table_info.get("columns", {})
            yaml_column_names = {col.upper() for col in yaml_columns.keys()}

            # Find columns in YAML that don't exist in Snowflake
            missing_in_snowflake = yaml_column_names - sf_column_names
            for col_name in sorted(missing_in_snowflake):
                suggestions = self._find_similar_columns(col_name, sf_column_names)
                error_msg = f"Column '{col_name}' in table '{table_name}' does not exist in Snowflake"
                if suggestions:
                    error_msg += f". Did you mean: {', '.join(suggestions)}?"
                result.add_error(
                    error_msg,
                    context={
                        "table": table_name,
                        "column": col_name,
                        "suggestions": suggestions,
                        "available_columns": sorted(list(sf_column_names))[:20],
                    },
                )

            # Optionally report Snowflake columns not in YAML
            if check_missing_in_yaml:
                missing_in_yaml = sf_column_names - yaml_column_names
                if missing_in_yaml:
                    result.add_info(
                        (
                            f"Table '{table_name}' has {len(missing_in_yaml)} columns in Snowflake "
                            f"not defined in YAML: {', '.join(sorted(list(missing_in_yaml))[:5])}..."
                            if len(missing_in_yaml) > 5
                            else f"Table '{table_name}' has columns in Snowflake not defined in YAML: "
                            f"{', '.join(sorted(missing_in_yaml))}"
                        ),
                        context={
                            "table": table_name,
                            "missing_columns": sorted(list(missing_in_yaml)),
                        },
                    )

        # Log summary
        logger.info(
            f"Schema verification complete: checked {tables_checked} tables, "
            f"skipped {tables_skipped} (missing location info)"
        )

        return result

    def _find_similar_columns(self, column_name: str, columns: Set[str]) -> List[str]:
        """
        Find similar column names for "Did you mean?" suggestions.

        Args:
            column_name: The unknown column name
            columns: Set of known column names

        Returns:
            List of up to 3 similar column names
        """
        if not column_name or not columns:
            return []

        col_lower = column_name.lower()
        candidates = []

        for known_col in columns:
            known_lower = known_col.lower()
            score = 0

            # Prefix matching
            min_prefix_len = min(3, len(col_lower), len(known_lower))
            if min_prefix_len > 0 and col_lower[:min_prefix_len] == known_lower[:min_prefix_len]:
                score += 3

            # Substring matching
            if col_lower in known_lower or known_lower in col_lower:
                score += 2

            # Character overlap
            common_chars = set(col_lower) & set(known_lower)
            all_chars = set(col_lower) | set(known_lower)
            if all_chars and len(common_chars) / len(all_chars) > 0.5:
                score += 1

            # Suffix matching (catches _id, _at, _date patterns)
            min_suffix_len = min(3, len(col_lower), len(known_lower))
            if min_suffix_len > 0 and col_lower[-min_suffix_len:] == known_lower[-min_suffix_len:]:
                score += 1

            if score > 0:
                candidates.append((known_col, score))

        candidates.sort(key=lambda x: x[1], reverse=True)
        return [name for name, _ in candidates[:3]]
