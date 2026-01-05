"""
Reference Validator

Ensures all semantic model references point to valid physical objects.

Critical for preventing runtime errors in Cortex Analyst by validating
that every table and column referenced in metrics, relationships, and
filters actually exists in the underlying dbt catalog.
"""

import re
from typing import Any, Dict, List, Optional, Set

from snowflake_semantic_tools.core.models import ValidationResult


class ReferenceValidator:
    """
    Validates physical object references in semantic models.

    Cross-references semantic model definitions against the dbt catalog
    to ensure all referenced objects exist:

    **Table Validation**:
    - Metrics reference existing tables
    - Relationships join valid tables
    - Filters apply to real tables

    **Column Validation**:
    - Metric expressions use valid columns
    - Relationship join keys exist
    - Filter conditions reference real columns

    This validation prevents SQL generation errors and ensures
    Cortex Analyst can generate executable queries.
    """

    def validate(self, semantic_data: Dict[str, Any], dbt_catalog: Dict[str, Any]) -> ValidationResult:
        """
        Validate references in semantic data against dbt catalog.

        Args:
            semantic_data: Parsed semantic model data
            dbt_catalog: Catalog of dbt models and columns

        Returns:
            ValidationResult with reference issues
        """
        result = ValidationResult()

        # Validate metrics
        metrics_data = semantic_data.get("metrics", {})
        if metrics_data:
            self._validate_metric_references(metrics_data, dbt_catalog, result)

        # Validate relationships
        relationships_data = semantic_data.get("relationships", {})
        if relationships_data:
            self._validate_relationship_references(relationships_data, dbt_catalog, result)

        # Validate filters
        filters_data = semantic_data.get("filters", {})
        if filters_data:
            self._validate_filter_references(filters_data, dbt_catalog, result)

        # Validate custom instructions
        instructions_data = semantic_data.get("custom_instructions", {})
        if instructions_data:
            self._validate_instruction_references(instructions_data, dbt_catalog, result)

        # Validate verified queries
        queries_data = semantic_data.get("verified_queries", {})
        if queries_data:
            self._validate_query_references(queries_data, dbt_catalog, result)

        # Validate semantic views
        views_data = semantic_data.get("semantic_views", {})
        instructions_data = semantic_data.get("custom_instructions", {})
        metrics_data = semantic_data.get("metrics", {})
        relationships_data = semantic_data.get("relationships", {})
        if views_data:
            self._validate_semantic_view_references(
                views_data, instructions_data, metrics_data, relationships_data, dbt_catalog, result
            )

        return result

    def _validate_metric_references(self, metrics_data: Dict, dbt_catalog: Dict, result: ValidationResult):
        """Validate table and column references in metrics."""
        items = metrics_data.get("items", [])

        for metric in items:
            if isinstance(metric, dict):
                name = metric.get("name", "")
                tables = metric.get("tables", [])
                expr = metric.get("expr", "")
                source_file = metric.get("source_file")

                # Defensive check: warn if tables field is missing
                if "tables" not in metric:
                    result.add_warning(
                        f"Metric '{name}' is missing 'tables' field - validation may be incomplete",
                        file_path=source_file,
                        context={"metric": name},
                    )

                # Validate table references
                for table in tables:
                    if isinstance(table, str):
                        table_lower = table.lower()
                        if table_lower not in dbt_catalog:
                            # Check if it's a CTE or subquery
                            if not self._is_cte_or_subquery(table):
                                # Find similar tables for suggestions
                                suggestions = self._find_similar_tables(table, dbt_catalog)
                                error_msg = f"Metric '{name}' references unknown table '{table}'"
                                if suggestions:
                                    error_msg += f". Did you mean: {', '.join(suggestions)}?"
                                else:
                                    error_msg += ". Check that the model has `config.meta.sst` configuration."
                                result.add_error(
                                    error_msg,
                                    file_path=source_file,
                                    context={
                                        "metric": name,
                                        "table": table,
                                        "suggestions": suggestions,
                                        "available_tables": list(dbt_catalog.keys())[:10],
                                    },
                                )
                        elif table_lower in dbt_catalog:
                            # Check if this table was skipped due to missing metadata
                            table_info = dbt_catalog[table_lower]
                            if isinstance(table_info, dict):
                                # Check for missing critical metadata
                                missing_fields = []
                                if not table_info.get("database"):
                                    missing_fields.append("database")
                                if not table_info.get("schema"):
                                    missing_fields.append("schema")

                                if missing_fields:
                                    result.add_error(
                                        f"Metric '{name}' references table '{table}' which is missing critical metadata ({'/'.join(missing_fields)}) and won't be available in the semantic model",
                                        file_path=source_file,
                                        context={"metric": name, "table": table, "missing_metadata": missing_fields},
                                    )

                # Validate column references in expression
                if expr:
                    self._validate_column_references_in_expr(
                        name, expr, tables, dbt_catalog, result, "Metric", source_file
                    )

    def _validate_relationship_references(self, relationships_data: Dict, dbt_catalog: Dict, result: ValidationResult):
        """Validate table and column references in relationships."""
        items = relationships_data.get("items", [])
        relationship_columns = relationships_data.get("relationship_columns", [])

        # Group relationship columns by relationship name
        columns_by_relationship = {}
        for col in relationship_columns:
            if isinstance(col, dict):
                rel_name = col.get("relationship_name", "")
                if rel_name not in columns_by_relationship:
                    columns_by_relationship[rel_name] = []
                columns_by_relationship[rel_name].append(col)

        for rel in items:
            if isinstance(rel, dict):
                name = rel.get("relationship_name", "") or rel.get("name", "")
                left_table = rel.get("left_table_name", "") or rel.get("left_table", "")
                right_table = rel.get("right_table_name", "") or rel.get("right_table", "")
                source_file = rel.get("source_file")

                # Get columns for this relationship
                columns = columns_by_relationship.get(name, [])

                # Validate for duplicate columns in foreign keys
                if columns:
                    left_columns = []
                    right_columns = []
                    for col_mapping in columns:
                        if isinstance(col_mapping, dict):
                            left_col = col_mapping.get("left_column", "")
                            right_col = col_mapping.get("right_column", "")
                            # Extract column name if in TABLE.COLUMN format
                            if "." in left_col:
                                _, left_col = left_col.rsplit(".", 1)
                            if "." in right_col:
                                _, right_col = right_col.rsplit(".", 1)
                            left_columns.append(left_col.lower())
                            right_columns.append(right_col.lower())

                    # Check for duplicates in left_columns
                    left_duplicates = [col for col in set(left_columns) if left_columns.count(col) > 1]
                    if left_duplicates:
                        result.add_error(
                            f"Relationship '{name}' has duplicate columns in foreign key (left side): {left_duplicates}. "
                            f"Each column can only appear once in a relationship join condition.",
                            file_path=source_file,
                            context={
                                "relationship": name,
                                "duplicate_columns": left_duplicates,
                                "side": "left",
                                "issue": "duplicate_foreign_key_columns",
                            },
                        )

                    # Check for duplicates in right_columns
                    right_duplicates = [col for col in set(right_columns) if right_columns.count(col) > 1]
                    if right_duplicates:
                        result.add_error(
                            f"Relationship '{name}' has duplicate columns in foreign key (right side): {right_duplicates}. "
                            f"Each column can only appear once in a relationship join condition.",
                            file_path=source_file,
                            context={
                                "relationship": name,
                                "duplicate_columns": right_duplicates,
                                "side": "right",
                                "issue": "duplicate_foreign_key_columns",
                            },
                        )

                # Convert table names to lowercase for catalog lookup
                left_table_lower = left_table.lower()
                right_table_lower = right_table.lower()

                # Validate left table
                if left_table and left_table_lower not in dbt_catalog:
                    suggestions = self._find_similar_tables(left_table, dbt_catalog)
                    error_msg = f"Relationship '{name}' references unknown left table '{left_table}'"
                    if suggestions:
                        error_msg += f". Did you mean: {', '.join(suggestions)}?"
                    else:
                        error_msg += ". Check that the model has `config.meta.sst` configuration."
                    result.add_error(
                        error_msg,
                        file_path=source_file,
                        context={"relationship": name, "table": left_table, "suggestions": suggestions},
                    )
                elif left_table_lower in dbt_catalog:
                    # Check if this table was skipped due to missing metadata
                    table_info = dbt_catalog[left_table_lower]
                    if isinstance(table_info, dict):
                        missing_fields = []
                        if not table_info.get("database"):
                            missing_fields.append("database")
                        if not table_info.get("schema"):
                            missing_fields.append("schema")
                        if missing_fields:
                            result.add_error(
                                f"Relationship '{name}' references left table '{left_table}' which is missing critical metadata ({'/'.join(missing_fields)}) and won't be available in the semantic model",
                                file_path=source_file,
                                context={"relationship": name, "table": left_table, "missing_metadata": missing_fields},
                            )

                # Validate right table
                if right_table and right_table_lower not in dbt_catalog:
                    suggestions = self._find_similar_tables(right_table, dbt_catalog)
                    error_msg = f"Relationship '{name}' references unknown right table '{right_table}'"
                    if suggestions:
                        error_msg += f". Did you mean: {', '.join(suggestions)}?"
                    else:
                        error_msg += ". Check that the model has `config.meta.sst` configuration."
                    result.add_error(
                        error_msg,
                        file_path=source_file,
                        context={"relationship": name, "table": right_table, "suggestions": suggestions},
                    )
                elif right_table_lower in dbt_catalog:
                    # Check if this table was skipped due to missing metadata
                    table_info = dbt_catalog[right_table_lower]
                    if isinstance(table_info, dict):
                        missing_fields = []
                        if not table_info.get("database"):
                            missing_fields.append("database")
                        if not table_info.get("schema"):
                            missing_fields.append("schema")
                        if missing_fields:
                            result.add_error(
                                f"Relationship '{name}' references right table '{right_table}' which is missing critical metadata ({'/'.join(missing_fields)}) and won't be available in the semantic model",
                                file_path=source_file,
                                context={
                                    "relationship": name,
                                    "table": right_table,
                                    "missing_metadata": missing_fields,
                                },
                            )

                # CRITICAL: Validate that relationship references the primary key of the right table
                # Must be done AFTER collecting all columns for this relationship
                if right_table_lower in dbt_catalog and columns:
                    right_table_info = dbt_catalog[right_table_lower]
                    primary_key = right_table_info.get("primary_key")

                    if primary_key:
                        # Handle both single column and composite primary keys
                        if isinstance(primary_key, str):
                            pk_columns = [primary_key.lower()]
                        elif isinstance(primary_key, list):
                            pk_columns = [col.lower() for col in primary_key]
                        else:
                            pk_columns = []

                        if pk_columns:
                            # Collect all right_columns used in this relationship
                            right_columns_used = []
                            for col_mapping in columns:
                                if isinstance(col_mapping, dict):
                                    right_col = col_mapping.get("right_column", "")
                                    # Extract column name if in TABLE.COLUMN format
                                    if "." in right_col:
                                        _, right_col = right_col.rsplit(".", 1)
                                    right_columns_used.append(right_col.lower())

                            # For composite keys: ALL pk columns must be used
                            # For single keys: the right_column must BE the pk column
                            if len(pk_columns) > 1:
                                # Composite key - check if all pk columns are referenced
                                missing_pk_cols = [col for col in pk_columns if col not in right_columns_used]
                                if missing_pk_cols:
                                    result.add_error(
                                        f"Relationship '{name}' does not reference the complete primary key of right table '{right_table}'. "
                                        f"The primary key is composite: [{', '.join(pk_columns)}], but relationship only references: [{', '.join(right_columns_used)}]. "
                                        f"Missing: [{', '.join(missing_pk_cols)}]. "
                                        f"Snowflake documentation states relationships MUST reference PRIMARY KEY or UNIQUE columns. "
                                        f"To fix: (1) add the missing columns to complete the primary key reference, (2) reverse the relationship direction if '{right_table}' has a UNIQUE constraint on [{', '.join(right_columns_used)}], or (3) update the primary_key in the YAML if [{', '.join(right_columns_used)}] is the actual composite primary key.",
                                        file_path=source_file,
                                        context={
                                            "relationship": name,
                                            "right_table": right_table,
                                            "primary_key": pk_columns,
                                            "columns_used": right_columns_used,
                                            "missing_columns": missing_pk_cols,
                                            "issue": "incomplete_composite_key",
                                        },
                                    )
                            else:
                                # Single column primary key - must match exactly
                                if pk_columns[0] not in right_columns_used:
                                    result.add_error(
                                        f"Relationship '{name}' references column(s) [{', '.join(right_columns_used)}] in right table '{right_table}', "
                                        f"but the primary key is '{pk_columns[0]}'. "
                                        f"Snowflake documentation states relationships MUST reference PRIMARY KEY or UNIQUE columns. "
                                        f"If '{right_columns_used[0] if right_columns_used else 'N/A'}' has a UNIQUE constraint, this is valid. "
                                        f"Otherwise, consider reversing the relationship direction.",
                                        file_path=source_file,
                                        context={
                                            "relationship": name,
                                            "right_table": right_table,
                                            "right_columns": right_columns_used,
                                            "primary_key": pk_columns[0],
                                            "issue": "not_primary_key",
                                        },
                                    )
                    else:
                        # Primary key information is missing from the table metadata
                        result.add_error(
                            f"Relationship '{name}' references table '{right_table}' which has no primary key metadata. "
                            f"This usually means the table was not properly extracted or enriched. "
                            f"Run 'sst enrich' on the table's YAML file to populate primary key information, "
                            f"or check that the table has proper meta.sst configuration.",
                            file_path=source_file,
                            context={
                                "relationship": name,
                                "right_table": right_table,
                                "issue": "missing_primary_key_metadata",
                            },
                        )
                elif right_table_lower not in dbt_catalog:
                    # Table is completely missing from the catalog
                    result.add_error(
                        f"Relationship '{name}' references table '{right_table}' that was not extracted. "
                        f"This usually means the table's metadata is missing or incomplete. "
                        f"Check that the table has proper meta.sst configuration or run 'sst enrich' to populate metadata.",
                        file_path=source_file,
                        context={"relationship": name, "right_table": right_table, "issue": "missing_table_dependency"},
                    )

                # Validate join columns
                for col_mapping in columns:
                    if isinstance(col_mapping, dict):
                        left_col = col_mapping.get("left_column", "")
                        right_col = col_mapping.get("right_column", "")

                        # Check for SQL transformations in column references
                        # This detects any SQL beyond just the column reference itself
                        def has_sql_transformation(col_ref: str) -> tuple[bool, str]:
                            """Check if column reference contains SQL transformations."""
                            import re

                            # Skip if empty or just whitespace
                            if not col_ref or not col_ref.strip():
                                return False, ""

                            # Extract the part after the last dot (if TABLE.COLUMN format)
                            col_part = col_ref.split(".")[-1] if "." in col_ref else col_ref

                            # Patterns that indicate SQL transformations
                            patterns = [
                                (r"::", "type casting (::)"),
                                (r"\bCAST\s*\(", "CAST function"),
                                (r"\bCONVERT\s*\(", "CONVERT function"),
                                (r"\bTO_DATE\s*\(", "TO_DATE function"),
                                (r"\bTO_TIMESTAMP\s*\(", "TO_TIMESTAMP function"),
                                (r"\bTO_CHAR\s*\(", "TO_CHAR function"),
                                (r"\bTO_NUMBER\s*\(", "TO_NUMBER function"),
                                (r"\bDATE\s*\(", "DATE function"),
                                (r"\bTRIM\s*\(", "TRIM function"),
                                (r"\bUPPER\s*\(", "UPPER function"),
                                (r"\bLOWER\s*\(", "LOWER function"),
                                (r"\bSUBSTRING\s*\(", "SUBSTRING function"),
                                (r"\bCOALESCE\s*\(", "COALESCE function"),
                                (r"\bNVL\s*\(", "NVL function"),
                                (r"\bIFNULL\s*\(", "IFNULL function"),
                                (r"\bCASE\s+WHEN", "CASE statement"),
                                (r"[\+\-\*\/]", "arithmetic operation"),
                                (r"\|\|", "string concatenation"),
                            ]

                            for pattern, description in patterns:
                                if re.search(pattern, col_part, re.IGNORECASE):
                                    return True, description

                            return False, ""

                        # Check left column
                        has_transform, transform_type = has_sql_transformation(left_col)
                        if has_transform:
                            result.add_error(
                                f"Relationship '{name}' contains SQL transformation ({transform_type}) in column reference '{left_col}'. "
                                f"Transformations cannot be performed within column references. "
                                f"Use template syntax {{ column('table_name', 'column_name') }} for the base column only.",
                                file_path=source_file,
                                context={
                                    "relationship": name,
                                    "column": left_col,
                                    "issue": "sql_transformation",
                                    "transform_type": transform_type,
                                },
                            )
                            continue

                        # Check right column
                        has_transform, transform_type = has_sql_transformation(right_col)
                        if has_transform:
                            result.add_error(
                                f"Relationship '{name}' contains SQL transformation ({transform_type}) in column reference '{right_col}'. "
                                f"Transformations cannot be performed within column references. "
                                f"Use template syntax {{ column('table_name', 'column_name') }} for the base column only.",
                                file_path=source_file,
                                context={
                                    "relationship": name,
                                    "column": right_col,
                                    "issue": "sql_transformation",
                                    "transform_type": transform_type,
                                },
                            )
                            continue

                        # Extract column names if in TABLE.COLUMN format
                        if "." in left_col:
                            _, left_col = left_col.rsplit(".", 1)
                        if "." in right_col:
                            _, right_col = right_col.rsplit(".", 1)

                        # Validate columns exist
                        if left_table_lower in dbt_catalog:
                            if left_col.lower() not in dbt_catalog[left_table_lower].get("columns", {}):
                                result.add_error(
                                    f"Relationship '{name}' references unknown column "
                                    f"'{left_col}' in table '{left_table}'",
                                    file_path=source_file,
                                    context={"relationship": name, "column": left_col, "table": left_table},
                                )

                        if right_table_lower in dbt_catalog:
                            if right_col.lower() not in dbt_catalog[right_table_lower].get("columns", {}):
                                result.add_error(
                                    f"Relationship '{name}' references unknown column "
                                    f"'{right_col}' in table '{right_table}'",
                                    file_path=source_file,
                                    context={"relationship": name, "column": right_col, "table": right_table},
                                )

    def _validate_filter_references(self, filters_data: Dict, dbt_catalog: Dict, result: ValidationResult):
        """Validate table references in filters."""
        items = filters_data.get("items", [])

        for filter_item in items:
            if isinstance(filter_item, dict):
                name = filter_item.get("name", "")
                table = filter_item.get("table_name", "").lower()
                expr = filter_item.get("expression", "")
                source_file = filter_item.get("source_file")

                # Validate table
                if table and table not in dbt_catalog:
                    suggestions = self._find_similar_tables(table, dbt_catalog)
                    error_msg = f"Filter '{name}' references unknown table '{table}'"
                    if suggestions:
                        error_msg += f". Did you mean: {', '.join(suggestions)}?"
                    else:
                        error_msg += ". Check that the model has `config.meta.sst` configuration."
                    result.add_error(
                        error_msg,
                        file_path=source_file,
                        context={"filter": name, "table": table, "suggestions": suggestions},
                    )

                # Validate column references in expression
                if expr and table:
                    self._validate_column_references_in_expr(
                        name, expr, [table], dbt_catalog, result, "Filter", source_file
                    )

    def _validate_instruction_references(self, instructions_data: Dict, dbt_catalog: Dict, result: ValidationResult):
        """Validate custom instructions exist and have unique names."""
        items = instructions_data.get("items", [])

        # Check for duplicate instruction names
        seen_names = set()
        for instruction in items:
            if isinstance(instruction, dict):
                name = instruction.get("name", "")
                if name:
                    if name in seen_names:
                        result.add_error(f"Duplicate custom instruction name: '{name}'", context={"instruction": name})
                    seen_names.add(name)

        # No table validation needed since custom instructions no longer have tables

    def _validate_query_references(self, queries_data: Dict, dbt_catalog: Dict, result: ValidationResult):
        """Validate table references in verified queries."""
        items = queries_data.get("items", [])

        for query in items:
            if isinstance(query, dict):
                name = query.get("name", "")
                tables = query.get("tables", [])

                # Validate table references
                for table in tables:
                    if isinstance(table, str):
                        table_lower = table.lower()
                        if table_lower not in dbt_catalog:
                            result.add_warning(
                                f"Verified query '{name}' references table '{table}' " f"not found in dbt models",
                                context={"query": name, "table": table},
                            )

    def _validate_column_references_in_expr(
        self,
        entity_name: str,
        expression: str,
        tables: List[str],
        dbt_catalog: Dict,
        result: ValidationResult,
        entity_type: str = "Entity",
        source_file: Optional[str] = None,
    ):
        """Validate column references in an expression."""
        # Find column references (TABLE.COLUMN pattern)
        column_refs = re.findall(r"(\w+)\.(\w+)", expression)

        for table_ref, column_ref in column_refs:
            table_lower = table_ref.lower()
            column_lower = column_ref.lower()

            # Check if table is in the entity's table list
            table_match = None
            for table in tables:
                if isinstance(table, str) and table.lower() == table_lower:
                    table_match = table.lower()
                    break

            if table_match and table_match in dbt_catalog:
                # Validate column exists
                columns = dbt_catalog[table_match].get("columns", {})
                if column_lower not in columns:
                    available = list(columns.keys())[:5]  # Show first 5 columns
                    result.add_error(
                        f"{entity_type} '{entity_name}' references unknown column "
                        f"'{column_ref}' in table '{table_ref}'",
                        file_path=source_file,
                        context={
                            "entity": entity_name,
                            "column": column_ref,
                            "table": table_ref,
                            "available_columns": available,
                        },
                    )

    def _validate_semantic_view_references(
        self,
        views_data: Dict,
        instructions_data: Dict,
        metrics_data: Dict,
        relationships_data: Dict,
        dbt_catalog: Dict,
        result: ValidationResult,
    ):
        """
        Validate references in semantic views.

        Checks:
        - Table references exist in dbt catalog
        - Custom instruction references are valid
        - Cross-table metrics have relationships between the tables
        """
        import json

        view_items = views_data.get("items", [])
        metric_items = metrics_data.get("items", [])
        relationship_items = relationships_data.get("items", [])

        # Build set of available custom instruction names
        available_instructions = set()
        instruction_items = instructions_data.get("items", [])
        for instruction in instruction_items:
            if isinstance(instruction, dict):
                name = instruction.get("name", "")
                if name:
                    available_instructions.add(name.upper())

        # Build relationship graph (table pairs that have relationships)
        relationship_graph = set()
        for rel in relationship_items:
            if isinstance(rel, dict):
                left_table = (rel.get("left_table_name") or rel.get("left_table", "")).lower()
                right_table = (rel.get("right_table_name") or rel.get("right_table", "")).lower()
                if left_table and right_table:
                    # Add both directions since relationships are bidirectional
                    relationship_graph.add((left_table, right_table))
                    relationship_graph.add((right_table, left_table))

        for view in view_items:
            if isinstance(view, dict):
                view_name = view.get("name", "")

                # Validate table references
                tables_json = view.get("tables", "[]")
                try:
                    tables = json.loads(tables_json) if isinstance(tables_json, str) else tables_json
                except:
                    tables = []

                tables_lower = [t.lower() for t in tables if isinstance(t, str)]

                for table in tables:
                    if isinstance(table, str):
                        table_lower = table.lower()
                        if table_lower not in dbt_catalog:
                            if not self._is_cte_or_subquery(table):
                                suggestions = self._find_similar_tables(table, dbt_catalog)
                                error_msg = (
                                    f"Semantic view '{view_name}' references table '{table}' that was not extracted"
                                )
                                if suggestions:
                                    error_msg += f". Did you mean: {', '.join(suggestions)}?"
                                else:
                                    error_msg += (
                                        ". Check that the table has `config.meta.sst` configuration "
                                        "or run 'sst enrich' to populate metadata."
                                    )
                                result.add_error(
                                    error_msg,
                                    context={
                                        "view": view_name,
                                        "table": table,
                                        "type": "MISSING_TABLE_DEPENDENCY",
                                        "suggestions": suggestions,
                                    },
                                )

                # CRITICAL: Validate cross-table metrics have relationships
                # Snowflake requires: "A metric cannot refer to another fact from an unrelated entity"
                if len(tables_lower) > 1:
                    # Check each metric to see if it references multiple tables
                    for metric in metric_items:
                        if isinstance(metric, dict):
                            metric_name = metric.get("name", "")
                            metric_tables_raw = metric.get("table_name", "")

                            # Parse metric's table list
                            try:
                                if isinstance(metric_tables_raw, str) and metric_tables_raw.startswith("["):
                                    metric_tables = json.loads(metric_tables_raw)
                                elif isinstance(metric_tables_raw, list):
                                    metric_tables = metric_tables_raw
                                else:
                                    metric_tables = [metric_tables_raw] if metric_tables_raw else []
                            except:
                                metric_tables = []

                            metric_tables_lower = [t.lower() for t in metric_tables if isinstance(t, str)]

                            # Check if this metric references multiple tables in this view
                            metric_tables_in_view = [t for t in metric_tables_lower if t in tables_lower]

                            if len(metric_tables_in_view) > 1:
                                # This metric spans multiple tables in this view
                                # Check if there's a relationship between ALL pairs of tables
                                missing_relationships = []
                                for i, table1 in enumerate(metric_tables_in_view):
                                    for table2 in metric_tables_in_view[i + 1 :]:
                                        if (table1, table2) not in relationship_graph:
                                            missing_relationships.append((table1, table2))

                                if missing_relationships:
                                    missing_pairs = [f"({t1}, {t2})" for t1, t2 in missing_relationships]
                                    result.add_error(
                                        f"Semantic view '{view_name}' includes metric '{metric_name}' which references "
                                        f"multiple tables {metric_tables_in_view}, but there is no relationship defined between: {', '.join(missing_pairs)}. "
                                        f"Snowflake semantic views require relationships between tables when metrics span multiple entities. "
                                        f"Either: (1) add a relationship between these tables, (2) remove this metric from the view, or (3) remove this semantic view.",
                                        context={
                                            "view": view_name,
                                            "metric": metric_name,
                                            "tables": metric_tables_in_view,
                                            "missing_relationships": missing_relationships,
                                            "issue": "cross_entity_metric_without_relationship",
                                        },
                                    )

                # Validate custom instruction references
                instructions_json = view.get("custom_instructions", "[]")
                try:
                    custom_instructions = (
                        json.loads(instructions_json) if isinstance(instructions_json, str) else instructions_json
                    )
                except:
                    custom_instructions = []

                if custom_instructions:
                    for instruction_ref in custom_instructions:
                        if isinstance(instruction_ref, str):
                            instruction_upper = instruction_ref.upper()
                            if instruction_upper not in available_instructions:
                                result.add_error(
                                    f"Semantic view '{view_name}' references unknown custom instruction '{instruction_ref}'",
                                    context={
                                        "view": view_name,
                                        "instruction": instruction_ref,
                                        "available": list(available_instructions),
                                    },
                                )

    def _is_cte_or_subquery(self, table_name: str) -> bool:
        """
        Check if a table name looks like a CTE or subquery alias.

        These don't need to exist in the dbt catalog.
        """
        # Common CTE patterns
        cte_patterns = ["cte_", "with_", "temp_", "tmp_", "_cte", "_with", "_temp", "_tmp"]

        table_lower = table_name.lower()
        return any(pattern in table_lower for pattern in cte_patterns)

    def _find_similar_tables(self, table_name: str, catalog: Dict[str, Any]) -> List[str]:
        """
        Find similar table names for "Did you mean?" suggestions.

        Uses multiple matching strategies without external dependencies:
        1. Prefix matching (same first 3+ characters)
        2. Substring matching (one name contains the other)
        3. Edit distance approximation (simple character overlap)

        Args:
            table_name: The unknown table name
            catalog: Dictionary of known tables

        Returns:
            List of up to 3 similar table names, sorted by relevance
        """
        if not table_name or not catalog:
            return []

        table_lower = table_name.lower()
        candidates = []

        for known_table in catalog.keys():
            known_lower = known_table.lower()
            score = 0

            # Strategy 1: Prefix matching (most likely for typos)
            min_prefix_len = min(3, len(table_lower), len(known_lower))
            if min_prefix_len > 0 and table_lower[:min_prefix_len] == known_lower[:min_prefix_len]:
                score += 3

            # Strategy 2: Substring matching
            if table_lower in known_lower or known_lower in table_lower:
                score += 2

            # Strategy 3: Character overlap (simple edit distance approximation)
            common_chars = set(table_lower) & set(known_lower)
            all_chars = set(table_lower) | set(known_lower)
            if all_chars:
                overlap_ratio = len(common_chars) / len(all_chars)
                if overlap_ratio > 0.5:
                    score += 1

            # Strategy 4: Same suffix (catches pluralization, etc.)
            min_suffix_len = min(3, len(table_lower), len(known_lower))
            if min_suffix_len > 0 and table_lower[-min_suffix_len:] == known_lower[-min_suffix_len:]:
                score += 1

            if score > 0:
                candidates.append((known_table, score))

        # Sort by score (descending) and return top 3
        candidates.sort(key=lambda x: x[1], reverse=True)
        return [name for name, _ in candidates[:3]]

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

            if score > 0:
                candidates.append((known_col, score))

        candidates.sort(key=lambda x: x[1], reverse=True)
        return [name for name, _ in candidates[:3]]
