"""
Semantic Model Validator

Validates the structure, required fields, and data types of all semantic model types:
- Metrics
- Relationships
- Filters
- Custom Instructions
- Verified Queries
- Semantic Views

Ensures semantic models have proper structure before they're used in generation,
preventing runtime errors and ensuring high-quality semantic layer definitions.
"""

from typing import Any, Dict, List, Optional, Set

from snowflake_semantic_tools.core.models import ValidationResult
from snowflake_semantic_tools.core.models.validation import ValidationSeverity
from snowflake_semantic_tools.core.parsing.join_condition_parser import JoinConditionParser, JoinType
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("semantic_model_validator")


class SemanticModelValidator:
    """
    Validates semantic model structure and required fields.

    Enforces Requirements:
    - **Required Fields**: All semantic models must have name, description, etc.
    - **Field Types**: Lists must be lists, strings must be strings, etc.
    - **Enum Values**: join_type, relationship_type must be valid values
    - **Logical Consistency**: Tables must be lists, expressions must be strings

    Best Practice Checks:
    - Descriptions for all models
    - Synonyms for better AI understanding
    - Proper naming conventions

    These validations ensure semantic models are well-formed before
    generation and prevent SQL generation errors.
    """

    # Valid aggregation types for metrics
    VALID_AGGREGATIONS = {
        "sum",
        "avg",
        "count",
        "count_distinct",
        "min",
        "max",
        "median",
        "percentile",
        "stddev",
        "variance",
    }

    def validate(self, semantic_data: Dict[str, Any]) -> ValidationResult:
        """
        Validate all semantic models in the parsed data.

        Args:
            semantic_data: Parsed semantic model data

        Returns:
            ValidationResult with all validation issues
        """
        result = ValidationResult()

        # Validate each semantic model type
        if "metrics" in semantic_data:
            self._validate_metrics(semantic_data["metrics"], result)

        if "relationships" in semantic_data:
            self._validate_relationships(semantic_data["relationships"], result)

        if "filters" in semantic_data:
            self._validate_filters(semantic_data["filters"], result)

        if "custom_instructions" in semantic_data:
            self._validate_custom_instructions(semantic_data["custom_instructions"], result)

        if "verified_queries" in semantic_data:
            self._validate_verified_queries(semantic_data["verified_queries"], result)

        if "semantic_views" in semantic_data:
            self._validate_semantic_views(semantic_data["semantic_views"], result)

        return result

    def _validate_metrics(self, metrics_data: Dict, result: ValidationResult):
        """Validate metric definitions."""
        items = metrics_data.get("items", [])

        for metric in items:
            metric_name = metric.get("name", "<unnamed>")

            # Required fields
            self._check_required_field(metric, "name", metric_name, "metric", result)
            self._check_required_field(metric, "expr", metric_name, "metric", result)
            self._check_required_field(metric, "tables", metric_name, "metric", result)

            # Validate metric name format
            self._validate_metric_name(metric_name, result)

            # Field types
            if "tables" in metric:
                if not isinstance(metric["tables"], list):
                    result.add_error(
                        f"Metric '{metric_name}' field 'tables' must be a list, got {type(metric['tables']).__name__}",
                        context={"metric": metric_name, "field": "tables", "type": "metric"},
                    )
                elif len(metric["tables"]) == 0:
                    result.add_error(
                        f"Metric '{metric_name}' field 'tables' cannot be empty",
                        context={"metric": metric_name, "field": "tables", "type": "metric"},
                    )

            if "expr" in metric:
                if not isinstance(metric["expr"], str):
                    result.add_error(
                        f"Metric '{metric_name}' field 'expr' must be a string, got {type(metric['expr']).__name__}",
                        context={"metric": metric_name, "field": "expr", "type": "metric"},
                    )
                elif not metric["expr"].strip():
                    result.add_error(
                        f"Metric '{metric_name}' field 'expr' cannot be empty",
                        context={"metric": metric_name, "field": "expr", "type": "metric"},
                    )
                else:
                    # Validate SQL syntax in expression
                    self._validate_sql_expression(metric_name, metric["expr"], "metric", result)

            # Validate default_aggregation if present
            if "default_aggregation" in metric:
                agg = metric["default_aggregation"]
                if agg and agg.lower() not in self.VALID_AGGREGATIONS:
                    result.add_warning(
                        f"Metric '{metric_name}' has unrecognized default_aggregation: '{agg}'. "
                        f"Valid values: {', '.join(sorted(self.VALID_AGGREGATIONS))}",
                        context={"metric": metric_name, "field": "default_aggregation", "value": agg, "type": "metric"},
                    )

            # Validate synonyms if present
            if "synonyms" in metric:
                if not isinstance(metric["synonyms"], list):
                    result.add_error(
                        f"Metric '{metric_name}' field 'synonyms' must be a list, got {type(metric['synonyms']).__name__}",
                        context={"metric": metric_name, "field": "synonyms", "type": "metric"},
                    )

            # Best practices
            if not metric.get("description"):
                result.add_warning(
                    f"Metric '{metric_name}' is missing description (recommended for AI understanding)",
                    context={"metric": metric_name, "field": "description", "type": "metric"},
                )

            if not metric.get("synonyms") or len(metric.get("synonyms", [])) == 0:
                result.add_info(
                    f"Consider adding synonyms to metric '{metric_name}' for better natural language queries",
                    context={"metric": metric_name, "field": "synonyms", "type": "metric"},
                )

    def _validate_relationships(self, relationships_data: Dict, result: ValidationResult):
        """Validate relationship definitions."""
        items = relationships_data.get("items", [])

        for relationship in items:
            # Handle both raw YAML format and parsed format
            # Raw YAML: name, left_table, right_table
            # Parsed format: relationship_name, left_table_name, right_table_name
            rel_name = relationship.get("name") or relationship.get("relationship_name", "<unnamed>")

            # Determine which format we're validating
            is_parsed_format = "relationship_name" in relationship

            # Required fields - adjust based on format
            if is_parsed_format:
                # Parsed format from storage
                required_fields = [
                    ("relationship_name", "name"),
                    ("left_table_name", "left_table"),
                    ("right_table_name", "right_table"),
                ]

                for actual_field, display_field in required_fields:
                    if not relationship.get(actual_field):
                        result.add_error(
                            f"Relationship '{rel_name}' is missing required field: {display_field}",
                            context={"relationship": rel_name, "field": display_field, "type": "relationship"},
                        )
            else:
                # Raw YAML format
                required_fields = ["name", "left_table", "right_table", "relationship_conditions"]

                # Check required fields
                for field in required_fields:
                    self._check_required_field(relationship, field, rel_name, "relationship", result)

            # Validate relationship_conditions structure
            if "relationship_conditions" in relationship:
                rel_conditions = relationship["relationship_conditions"]

                if not isinstance(rel_conditions, list):
                    result.add_error(
                        f"Relationship '{rel_name}' field 'relationship_conditions' must be a list",
                        context={"relationship": rel_name, "field": "relationship_conditions", "type": "relationship"},
                    )
                elif len(rel_conditions) == 0:
                    result.add_error(
                        f"Relationship '{rel_name}' field 'relationship_conditions' cannot be empty",
                        context={"relationship": rel_name, "field": "relationship_conditions", "type": "relationship"},
                    )
                else:
                    # Validate each join condition
                    for i, condition in enumerate(rel_conditions):
                        if not isinstance(condition, str):
                            result.add_error(
                                f"Relationship '{rel_name}' relationship_conditions[{i}] must be a string",
                                context={
                                    "relationship": rel_name,
                                    "field": f"relationship_conditions[{i}]",
                                    "type": "relationship",
                                },
                            )
                            continue

                        # Check if condition uses templates (before resolution)
                        if "{{" in condition or "}}" in condition:
                            # Warn if templates are not quoted properly in YAML
                            # This is a soft check since the YAML parser would have already failed if truly broken
                            # But we can warn about potential issues
                            pass

                        # Parse and validate the join condition
                        is_valid, error_msg = JoinConditionParser.validate_condition(condition)
                        if not is_valid:
                            result.add_error(
                                f"Relationship '{rel_name}' has invalid join condition: {error_msg}",
                                context={"relationship": rel_name, "condition": condition, "type": "relationship"},
                            )
                        else:
                            # Parse to get more details for validation
                            parsed = JoinConditionParser.parse(condition)

                            # Validate references to tables match relationship tables
                            left_table = relationship.get("left_table", "")
                            right_table = relationship.get("right_table", "")

                            # Extract table names from templates if present
                            left_table_name = self._extract_table_name(left_table)
                            right_table_name = self._extract_table_name(right_table)

                            # Validate table references match
                            if parsed.left_table and left_table_name and parsed.left_table != left_table_name:
                                result.add_error(
                                    f"Relationship '{rel_name}' condition references table '{parsed.left_table}' but left_table is '{left_table_name}'",
                                    context={"relationship": rel_name, "condition": condition, "type": "relationship"},
                                )

                            if parsed.right_table and right_table_name and parsed.right_table != right_table_name:
                                result.add_error(
                                    f"Relationship '{rel_name}' condition references table '{parsed.right_table}' but right_table is '{right_table_name}'",
                                    context={"relationship": rel_name, "condition": condition, "type": "relationship"},
                                )

                            # Warn about ASOF conditions (best practice)
                            if parsed.condition_type == JoinType.ASOF and i == 0:
                                result.add_warning(
                                    f"Relationship '{rel_name}' has ASOF condition as first condition - consider putting equality condition first",
                                    context={"relationship": rel_name, "condition": condition, "type": "relationship"},
                                )

            # Note: description is not part of the Snowflake spec, so we don't warn about it

    def _extract_table_name(self, table_template: str) -> str:
        """Extract table name from {{ table('name') }} template."""
        import re

        match = re.search(r"{{\s*table\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*}}", table_template)
        if match:
            return match.group(1)
        return table_template  # Return as-is if not a template

    def _validate_filters(self, filters_data: Dict, result: ValidationResult):
        """Validate filter definitions."""
        items = filters_data.get("items", [])

        for filter_def in items:
            filter_name = filter_def.get("name", "<unnamed>")

            # Required fields
            self._check_required_field(filter_def, "name", filter_name, "filter", result)
            self._check_required_field(filter_def, "expr", filter_name, "filter", result)

            # Field types
            if "expr" in filter_def:
                if not isinstance(filter_def["expr"], str):
                    result.add_error(
                        f"Filter '{filter_name}' field 'expr' must be a string, got {type(filter_def['expr']).__name__}",
                        context={"filter": filter_name, "field": "expr", "type": "filter"},
                    )
                elif not filter_def["expr"].strip():
                    result.add_error(
                        f"Filter '{filter_name}' field 'expr' cannot be empty",
                        context={"filter": filter_name, "field": "expr", "type": "filter"},
                    )

            # Validate synonyms if present
            if "synonyms" in filter_def:
                if not isinstance(filter_def["synonyms"], list):
                    result.add_error(
                        f"Filter '{filter_name}' field 'synonyms' must be a list, got {type(filter_def['synonyms']).__name__}",
                        context={"filter": filter_name, "field": "synonyms", "type": "filter"},
                    )

            # Best practices
            if not filter_def.get("description"):
                result.add_warning(
                    f"Filter '{filter_name}' is missing description (recommended for AI understanding)",
                    context={"filter": filter_name, "field": "description", "type": "filter"},
                )

            if not filter_def.get("synonyms") or len(filter_def.get("synonyms", [])) == 0:
                result.add_info(
                    f"Consider adding synonyms to filter '{filter_name}' for better natural language queries",
                    context={"filter": filter_name, "field": "synonyms", "type": "filter"},
                )

    def _validate_custom_instructions(self, instructions_data: Dict, result: ValidationResult):
        """Validate custom instruction definitions."""
        items = instructions_data.get("items", [])

        for instruction in items:
            instruction_name = instruction.get("name", "<unnamed>")

            # Required fields
            self._check_required_field(instruction, "name", instruction_name, "custom_instruction", result)
            self._check_required_field(instruction, "instruction", instruction_name, "custom_instruction", result)

            # Field types
            if "instruction" in instruction:
                if not isinstance(instruction["instruction"], str):
                    result.add_error(
                        f"Custom instruction '{instruction_name}' field 'instruction' must be a string, got {type(instruction['instruction']).__name__}",
                        context={
                            "custom_instruction": instruction_name,
                            "field": "instruction",
                            "type": "custom_instruction",
                        },
                    )
                elif not instruction["instruction"].strip():
                    result.add_error(
                        f"Custom instruction '{instruction_name}' field 'instruction' cannot be empty",
                        context={
                            "custom_instruction": instruction_name,
                            "field": "instruction",
                            "type": "custom_instruction",
                        },
                    )
                elif len(instruction["instruction"]) < 10:
                    result.add_warning(
                        f"Custom instruction '{instruction_name}' has very short instruction text (< 10 chars). "
                        f"Consider adding more detail.",
                        context={
                            "custom_instruction": instruction_name,
                            "field": "instruction",
                            "type": "custom_instruction",
                        },
                    )

            # Note: description is not required by Snowflake spec, so we don't warn about it

    def _validate_verified_queries(self, queries_data: Dict, result: ValidationResult):
        """Validate verified query definitions."""
        items = queries_data.get("items", [])

        for query in items:
            query_name = query.get("name", "<unnamed>")

            # Required fields
            self._check_required_field(query, "name", query_name, "verified_query", result)
            self._check_required_field(query, "question", query_name, "verified_query", result)
            self._check_required_field(query, "sql", query_name, "verified_query", result)

            # Field types
            if "question" in query:
                if not isinstance(query["question"], str):
                    result.add_error(
                        f"Verified query '{query_name}' field 'question' must be a string, got {type(query['question']).__name__}",
                        context={"verified_query": query_name, "field": "question", "type": "verified_query"},
                    )
                elif not query["question"].strip():
                    result.add_error(
                        f"Verified query '{query_name}' field 'question' cannot be empty",
                        context={"verified_query": query_name, "field": "question", "type": "verified_query"},
                    )

            if "sql" in query:
                if not isinstance(query["sql"], str):
                    result.add_error(
                        f"Verified query '{query_name}' field 'sql' must be a string, got {type(query['sql']).__name__}",
                        context={"verified_query": query_name, "field": "sql", "type": "verified_query"},
                    )
                elif not query["sql"].strip():
                    result.add_error(
                        f"Verified query '{query_name}' field 'sql' cannot be empty",
                        context={"verified_query": query_name, "field": "sql", "type": "verified_query"},
                    )
                else:
                    # Basic SQL validation - check for SELECT
                    sql = query["sql"].strip().upper()
                    if not sql.startswith("SELECT") and not sql.startswith("WITH"):
                        result.add_warning(
                            f"Verified query '{query_name}' SQL should start with SELECT or WITH",
                            context={"verified_query": query_name, "field": "sql", "type": "verified_query"},
                        )

            # Validate verified_at if present
            # Note: We accept YYYY-MM-DD format in YAML for readability
            # The data loader automatically converts it to Unix timestamp for Snowflake
            if "verified_at" in query:
                verified_at = query["verified_at"]
                if not isinstance(verified_at, str):
                    result.add_error(
                        f"Verified query '{query_name}' field 'verified_at' must be a string in YYYY-MM-DD format (e.g., '2024-01-15')",
                        context={"verified_query": query_name, "field": "verified_at", "type": "verified_query"},
                    )
                else:
                    # Validate YYYY-MM-DD format
                    import re

                    if not re.match(r"^\d{4}-\d{2}-\d{2}$", verified_at):
                        result.add_error(
                            f"Verified query '{query_name}' field 'verified_at' must be in YYYY-MM-DD format (e.g., '2024-01-15'), got '{verified_at}'",
                            context={
                                "verified_query": query_name,
                                "field": "verified_at",
                                "value": verified_at,
                                "type": "verified_query",
                            },
                        )

            # Note: description is not required by Snowflake spec, so we don't warn about it

            if not query.get("verified_at"):
                result.add_info(
                    f"Consider adding 'verified_at' date to verified query '{query_name}' for tracking",
                    context={"verified_query": query_name, "field": "verified_at", "type": "verified_query"},
                )

    def _validate_semantic_views(self, views_data: Dict, result: ValidationResult):
        """Validate semantic view definitions."""
        import json

        items = views_data.get("items", [])

        for view in items:
            view_name = view.get("name", "<unnamed>")

            # Required fields
            self._check_required_field(view, "name", view_name, "semantic_view", result)
            self._check_required_field(view, "tables", view_name, "semantic_view", result)

            # Field types - handle both list (raw YAML) and JSON string (parsed for storage)
            if "tables" in view:
                tables_raw = view["tables"]

                # If it's a JSON string (from parser), deserialize it
                if isinstance(tables_raw, str):
                    try:
                        tables = json.loads(tables_raw)
                    except (json.JSONDecodeError, TypeError):
                        result.add_error(
                            f"Semantic view '{view_name}' field 'tables' is an invalid JSON string",
                            context={"semantic_view": view_name, "field": "tables", "type": "semantic_view"},
                        )
                        continue
                else:
                    tables = tables_raw

                # Now validate the actual list
                if not isinstance(tables, list):
                    result.add_error(
                        f"Semantic view '{view_name}' field 'tables' must be a list, got {type(tables).__name__}",
                        context={"semantic_view": view_name, "field": "tables", "type": "semantic_view"},
                    )
                elif len(tables) == 0:
                    result.add_error(
                        f"Semantic view '{view_name}' field 'tables' cannot be empty",
                        context={"semantic_view": view_name, "field": "tables", "type": "semantic_view"},
                    )

            # Validate optional list fields
            for list_field in ["dimensions", "measures", "time_dimensions", "filters"]:
                if list_field in view:
                    if not isinstance(view[list_field], list):
                        result.add_error(
                            f"Semantic view '{view_name}' field '{list_field}' must be a list, got {type(view[list_field]).__name__}",
                            context={"semantic_view": view_name, "field": list_field, "type": "semantic_view"},
                        )

            # Best practices - description IS in the SST schema for semantic views
            if not view.get("description"):
                result.add_warning(
                    f"Semantic view '{view_name}' is missing description (recommended for documentation)",
                    context={"semantic_view": view_name, "field": "description", "type": "semantic_view"},
                )

            # Note: We do NOT warn about missing dimensions/measures/time_dimensions
            # This is BY DESIGN - sst generate automatically pulls these from the
            # referenced tables' metadata. Semantic views are intentionally minimal
            # in YAML and get enriched during generation.

    def _check_required_field(self, obj: Dict, field_name: str, obj_name: str, obj_type: str, result: ValidationResult):
        """Check if a required field exists and is not empty."""
        if field_name not in obj:
            result.add_error(
                f"{obj_type.replace('_', ' ').title()} '{obj_name}' is missing required field: {field_name}",
                context={obj_type: obj_name, "field": field_name, "type": obj_type},
            )
        elif obj[field_name] is None:
            result.add_error(
                f"{obj_type.replace('_', ' ').title()} '{obj_name}' field '{field_name}' cannot be null",
                context={obj_type: obj_name, "field": field_name, "type": obj_type},
            )
        elif isinstance(obj[field_name], str) and not obj[field_name].strip():
            result.add_error(
                f"{obj_type.replace('_', ' ').title()} '{obj_name}' field '{field_name}' cannot be empty",
                context={obj_type: obj_name, "field": field_name, "type": obj_type},
            )

    def _validate_metric_name(self, metric_name: str, result: ValidationResult):
        """Validate metric name format and syntax."""
        if not metric_name or metric_name == "<unnamed>":
            return

        # Check for spaces in metric names
        if " " in metric_name:
            result.add_error(
                f"Metric name '{metric_name}' contains spaces. Metric names must use underscores or camelCase.",
                context={"metric": metric_name, "issue": "spaces_in_name", "type": "metric"},
            )

        # Check for invalid characters
        import re

        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", metric_name):
            result.add_error(
                f"Metric name '{metric_name}' contains invalid characters. Only letters, numbers, and underscores are allowed, and it must start with a letter or underscore.",
                context={"metric": metric_name, "issue": "invalid_characters", "type": "metric"},
            )

    def _validate_sql_expression(self, entity_name: str, expression: str, entity_type: str, result: ValidationResult):
        """Validate SQL syntax in expressions."""
        import re

        # Check for invalid column reference syntax (like start_date::DATE)
        # This pattern looks for column references with :: syntax that should use template syntax instead
        invalid_column_refs = re.findall(r"(\w+)::(\w+)", expression)
        for column, type_cast in invalid_column_refs:
            result.add_error(
                f"{entity_type.title()} '{entity_name}' contains invalid column reference syntax '{column}::{type_cast}'. "
                f"Use template syntax {{ column('table_name', '{column}') }} instead.",
                context={
                    "entity": entity_name,
                    "issue": "invalid_column_syntax",
                    "syntax": f"{column}::{type_cast}",
                    "type": entity_type,
                },
            )

        # Check for common SQL syntax issues
        # Look for TOKEN RATE patterns that might cause compilation errors
        if re.search(r"\bTOKEN\s+RATE\b", expression, re.IGNORECASE):
            result.add_error(
                f"{entity_type.title()} '{entity_name}' contains 'TOKEN RATE' which may cause SQL compilation errors. "
                f"Check for proper spacing and syntax in metric expressions.",
                context={"entity": entity_name, "issue": "token_rate_syntax", "type": entity_type},
            )

        # Check for other common SQL syntax issues
        if re.search(r"\bRATE\s*\(", expression, re.IGNORECASE):
            result.add_error(
                f"{entity_type.title()} '{entity_name}' contains 'RATE(' which may cause SQL compilation errors. "
                f"Check for proper function syntax and spacing.",
                context={"entity": entity_name, "issue": "rate_function_syntax", "type": entity_type},
            )

        # Check for missing commas in function calls (very specific pattern)
        # Look for patterns like "FUNCTION ARG1 ARG2(" which should be "FUNCTION(ARG1, ARG2)"
        # But avoid matching template syntax, CASE statements, and other valid SQL patterns
        if re.search(
            r"\b(SUM|COUNT|AVG|MIN|MAX|PERCENTILE_CONT|PERCENTILE_DISC)\s+\w+\s+\w+\s*\(", expression, re.IGNORECASE
        ):
            result.add_error(
                f"{entity_type.title()} '{entity_name}' may have missing commas in function calls. "
                f"Check for proper comma separation between function arguments.",
                context={"entity": entity_name, "issue": "missing_commas", "type": entity_type},
            )

    def _validate_column_reference_syntax(
        self, relationship_name: str, column_ref: str, field_name: str, result: ValidationResult
    ):
        """Validate column reference syntax in relationships."""
        import re

        # Check for invalid column reference syntax (like start_date::DATE)
        if "::" in column_ref:
            result.add_error(
                f"Relationship '{relationship_name}' {field_name} contains invalid column reference syntax '{column_ref}'. "
                f"Use template syntax {{ column('table_name', 'column_name') }} instead.",
                context={
                    "relationship": relationship_name,
                    "field": field_name,
                    "issue": "invalid_column_syntax",
                    "syntax": column_ref,
                    "type": "relationship",
                },
            )

        # Check for other SQL syntax issues in column references
        if re.search(r"\bTOKEN\s+RATE\b", column_ref, re.IGNORECASE):
            result.add_error(
                f"Relationship '{relationship_name}' {field_name} contains 'TOKEN RATE' which may cause SQL compilation errors. "
                f"Check for proper spacing and syntax in column references.",
                context={
                    "relationship": relationship_name,
                    "field": field_name,
                    "issue": "token_rate_syntax",
                    "type": "relationship",
                },
            )
