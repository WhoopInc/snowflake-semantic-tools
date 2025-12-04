"""
Validator to detect quoted template expressions in semantic models.

Prevents patterns like "{{ column(...) }}" that cause Snowflake SQL errors
after template resolution by creating invalid quoted identifiers.
"""

import re
from typing import Any, Dict, List, Tuple

from snowflake_semantic_tools.core.models.validation import ValidationResult
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("validation.quoted_templates")


class QuotedTemplateValidator:
    """
    Detects template expressions wrapped in double quotes.

    Patterns checked:
    - "{{ table(...) }}"
    - "{{ column(...) }}"
    - "{{ metric(...) }}"
    - "{{ custom_instructions(...) }}"

    These patterns cause Snowflake errors because quotes remain after
    template resolution, creating invalid quoted identifiers like
    "TABLE.COLUMN" which Snowflake treats as a single identifier.
    """

    # Regex patterns for detection
    QUOTED_TABLE_PATTERN = r'"{{[\s]*table\([^}]+\)[\s]*}}"'
    QUOTED_COLUMN_PATTERN = r'"{{[\s]*column\([^}]+\)[\s]*}}"'
    QUOTED_METRIC_PATTERN = r'"{{[\s]*metric\([^}]+\)[\s]*}}"'
    QUOTED_INSTRUCTIONS_PATTERN = r'"{{[\s]*custom_instructions\([^}]+\)[\s]*}}"'

    def __init__(self):
        """Initialize the validator."""
        self.patterns = [
            (self.QUOTED_TABLE_PATTERN, "table"),
            (self.QUOTED_COLUMN_PATTERN, "column"),
            (self.QUOTED_METRIC_PATTERN, "metric"),
            (self.QUOTED_INSTRUCTIONS_PATTERN, "custom_instructions"),
        ]

    def validate(
        self,
        metrics_data: Dict,
        relationships_data: Dict,
        semantic_views_data: Dict,
        filters_data: Dict,
        result: ValidationResult,
    ) -> None:
        """
        Validate all semantic model files for quoted template expressions.

        Args:
            metrics_data: Parsed metrics data
            relationships_data: Parsed relationships data
            semantic_views_data: Parsed semantic views data
            filters_data: Parsed filters data
            result: ValidationResult to append errors to
        """
        logger.info("Validating for quoted template expressions...")

        error_count_before = result.error_count

        # Check metrics
        if "snowflake_metrics" in metrics_data:
            for metric in metrics_data["snowflake_metrics"]:
                self._validate_metric(metric, result)

        # Check relationships
        if "snowflake_relationships" in relationships_data:
            for rel in relationships_data["snowflake_relationships"]:
                self._validate_relationship(rel, result)

        # Check filters
        if "snowflake_filters" in filters_data:
            for filt in filters_data["snowflake_filters"]:
                self._validate_filter(filt, result)

        # Check semantic views (for any custom SQL)
        if "semantic_views" in semantic_views_data:
            for view in semantic_views_data["semantic_views"]:
                self._validate_semantic_view(view, result)

        errors_found = result.error_count - error_count_before
        if errors_found > 0:
            logger.warning(f"Found {errors_found} quoted template expression(s)")
        else:
            logger.info("No quoted template expressions found")

    def _validate_metric(self, metric: Dict[str, Any], result: ValidationResult) -> None:
        """Validate a metric for quoted templates."""
        name = metric.get("name", "unknown")

        # Check tables field
        tables = metric.get("tables", [])
        if isinstance(tables, list):
            for idx, table in enumerate(tables):
                if isinstance(table, str):
                    violations = self._find_quoted_templates(table)
                    for pattern_type, match, line_num in violations:
                        unquoted = match.strip('"')
                        result.add_error(
                            f"Quoted template expression in metric '{name}' tables field\n"
                            f"  Found: {match}\n"
                            f"  Fix: Change to {unquoted}\n"
                            f"  Issue: Template expressions must not be wrapped in double quotes.\n"
                            f"  Details: After template resolution, this creates an invalid Snowflake\n"
                            f'           quoted identifier. Snowflake treats "TABLE.COLUMN" as a single\n'
                            f"           identifier, not TABLE.COLUMN (two separate identifiers).",
                            context={
                                "metric": name,
                                "type": "QUOTED_TEMPLATE_EXPRESSION",
                                "location": "tables",
                                "found": match,
                                "fix": unquoted,
                            },
                        )

        # Check expression field
        expr = metric.get("expr", "")
        if not expr:
            return

        # Check expression for quoted templates
        violations = self._find_quoted_templates(expr)

        for pattern_type, match, line_num in violations:
            # Extract the template content (without quotes)
            unquoted = match.strip('"')

            result.add_error(
                f"Quoted template expression in metric '{name}'\n"
                f"  Found: {match}\n"
                f"  Fix: Change to {unquoted}\n"
                f"  Issue: Template expressions must not be wrapped in double quotes.\n"
                f"  Details: After template resolution, quoted identifiers cause Snowflake errors.\n"
                f'           Snowflake treats "TABLE.COLUMN" as a single identifier, not TABLE.COLUMN.',
                context={
                    "metric": name,
                    "type": "QUOTED_TEMPLATE_EXPRESSION",
                    "pattern_type": pattern_type,
                    "found": match,
                    "fix": unquoted,
                    "line": line_num,
                },
            )

    def _validate_relationship(self, rel: Dict[str, Any], result: ValidationResult) -> None:
        """Validate a relationship for quoted templates."""
        name = rel.get("name", "unknown")

        # Check left_table and right_table fields
        for field in ["left_table", "right_table"]:
            value = rel.get(field, "")
            if value and isinstance(value, str):
                violations = self._find_quoted_templates(value)
                for pattern_type, match, line_num in violations:
                    unquoted = match.strip('"')
                    result.add_error(
                        f"Quoted template in relationship '{name}' {field} field\n"
                        f"  Found: {match}\n"
                        f"  Fix: Change to {unquoted}",
                        context={
                            "relationship": name,
                            "type": "QUOTED_TEMPLATE_EXPRESSION",
                            "location": field,
                            "found": match,
                            "fix": unquoted,
                        },
                    )

        # Check relationship_conditions (new format)
        conditions = rel.get("relationship_conditions", [])
        if isinstance(conditions, list):
            for idx, condition in enumerate(conditions):
                if isinstance(condition, str):
                    violations = self._find_quoted_templates(condition)
                    for pattern_type, match, line_num in violations:
                        unquoted = match.strip('"')
                        result.add_error(
                            f"Quoted template in relationship '{name}' condition {idx+1}\n"
                            f"  Found: {match}\n"
                            f"  Fix: Change to {unquoted}\n"
                            f"  Note: Conditions must use single quotes around templates in YAML:\n"
                            f'        \'{{{{ column("table", "col") }}}} = ...\'',
                            context={
                                "relationship": name,
                                "type": "QUOTED_TEMPLATE_EXPRESSION",
                                "location": f"relationship_conditions[{idx}]",
                                "found": match,
                                "fix": unquoted,
                            },
                        )

        # Check legacy relationship_columns format
        columns = rel.get("relationship_columns", [])
        for col in columns:
            for field in ["left_column", "right_column"]:
                value = col.get(field, "")
                if value:
                    violations = self._find_quoted_templates(str(value))
                    for pattern_type, match, line_num in violations:
                        unquoted = match.strip('"')
                        result.add_error(
                            f"Quoted template in relationship '{name}' {field}\n"
                            f"  Found: {match}\n"
                            f"  Fix: Change to {unquoted}",
                            context={
                                "relationship": name,
                                "type": "QUOTED_TEMPLATE_EXPRESSION",
                                "location": field,
                                "found": match,
                                "fix": unquoted,
                            },
                        )

    def _validate_filter(self, filt: Dict[str, Any], result: ValidationResult) -> None:
        """Validate a filter for quoted templates."""
        name = filt.get("name", "unknown")
        expr = filt.get("expr", "")

        if not expr:
            return

        violations = self._find_quoted_templates(expr)
        for pattern_type, match, line_num in violations:
            unquoted = match.strip('"')
            result.add_error(
                f"Quoted template in filter '{name}'\n" f"  Found: {match}\n" f"  Fix: Change to {unquoted}",
                context={"filter": name, "type": "QUOTED_TEMPLATE_EXPRESSION", "found": match, "fix": unquoted},
            )

    def _validate_semantic_view(self, view: Dict[str, Any], result: ValidationResult) -> None:
        """Validate semantic view for quoted templates (if custom SQL exists)."""
        name = view.get("name", "unknown")

        # Check tables list
        tables = view.get("tables", [])
        for idx, table in enumerate(tables):
            if isinstance(table, str):
                violations = self._find_quoted_templates(table)
                for pattern_type, match, line_num in violations:
                    unquoted = match.strip('"')
                    result.add_error(
                        f"Quoted template in semantic view '{name}' tables field\n"
                        f"  Found: {match}\n"
                        f"  Fix: Change to {unquoted}",
                        context={
                            "semantic_view": name,
                            "type": "QUOTED_TEMPLATE_EXPRESSION",
                            "location": f"tables[{idx}]",
                            "found": match,
                            "fix": unquoted,
                        },
                    )

    def _find_quoted_templates(self, text: str) -> List[Tuple[str, str, int]]:
        """
        Find all quoted template expressions in text.

        Args:
            text: Text to search

        Returns:
            List of (pattern_type, matched_text, line_number) tuples
        """
        violations = []

        lines = text.split("\n")
        for line_num, line in enumerate(lines, start=1):
            for pattern, pattern_type in self.patterns:
                matches = re.finditer(pattern, line, re.IGNORECASE)
                for match in matches:
                    violations.append((pattern_type, match.group(0), line_num))

        return violations
