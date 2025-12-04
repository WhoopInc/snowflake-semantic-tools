"""
Template Resolution Validator

Verifies complete expansion of all template references.

Critical first-pass validation that ensures all {{ }} template syntax
has been properly resolved before further validation. Unresolved templates
would cause incorrect validation results and runtime failures in Cortex Analyst.
"""

import re
from typing import Any, Dict, List, Set

from snowflake_semantic_tools.shared import get_logger

logger = get_logger("validation.template_resolution")


class TemplateResolutionValidator:
    """
    Ensures all template syntax has been properly expanded.

    Scans for unresolved placeholder patterns that indicate failed
    template resolution:

    - **TEMP_COLUMN**: Unresolved {{ column() }} references
    - **TEMP_TABLE**: Unresolved {{ table() }} references
    - **TEMP_METRIC**: Unresolved {{ metric() }} references
    - **TEMP_INSTRUCTION**: Unresolved {{ custom_instructions() }} references

    These placeholders are temporary markers used during parsing that
    must be replaced with actual values before semantic model generation.
    Their presence indicates incomplete template resolution that would
    cause SQL generation failures.
    """

    UNRESOLVED_PATTERNS = ["TEMP_COLUMN", "TEMP_TABLE", "TEMP_METRIC", "TEMP_INSTRUCTION"]

    def __init__(self):
        """Initialize the template resolution validator."""
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def validate(self, parse_result: Dict[str, Any]) -> tuple[List[str], List[str]]:
        """
        Check for unresolved template placeholders.

        Args:
            parse_result: Parsed semantic models data

        Returns:
            Tuple of (errors, warnings)
        """
        self.errors = []
        self.warnings = []

        # Check metrics
        if "metrics" in parse_result and parse_result["metrics"]:
            self._check_metrics(parse_result["metrics"])

        # Check relationships
        if "relationships" in parse_result and parse_result["relationships"]:
            self._check_relationships(parse_result["relationships"])

        # Check filters
        if "filters" in parse_result and parse_result["filters"]:
            self._check_filters(parse_result["filters"])

        # Check verified queries
        if "verified_queries" in parse_result and parse_result["verified_queries"]:
            self._check_verified_queries(parse_result["verified_queries"])

        return self.errors, self.warnings

    def _check_metrics(self, metrics_data: Any) -> None:
        """Check metrics for unresolved templates."""
        # Handle different data structures
        if isinstance(metrics_data, dict):
            # Check both 'items' and direct metrics list
            metrics = metrics_data.get("items", [])
            if not metrics and "snowflake_metrics" in metrics_data:
                metrics = metrics_data["snowflake_metrics"]
        elif isinstance(metrics_data, list):
            metrics = metrics_data
        else:
            # Skip if not the expected type
            return

        for metric in metrics:
            if not isinstance(metric, dict):
                continue

            metric_name = metric.get("name", "unknown")
            expr = metric.get("expr", metric.get("expression", ""))

            for pattern in self.UNRESOLVED_PATTERNS:
                if pattern in str(expr):
                    self.errors.append(
                        f"CRITICAL: Metric '{metric_name}' contains unresolved template placeholder '{pattern}'. "
                        f"This indicates a parsing bug - the template {{ column('...') }} or similar could not be resolved. "
                        f"This metric MUST NOT be extracted to Snowflake."
                    )

    def _check_relationships(self, relationships_data: Any) -> None:
        """Check relationships for unresolved templates."""
        # Handle different data structures
        if isinstance(relationships_data, dict):
            relationships = relationships_data.get("items", [])
        elif isinstance(relationships_data, list):
            relationships = relationships_data
        else:
            return

        for rel in relationships:
            if not isinstance(rel, dict):
                continue

            rel_name = rel.get("name", "unknown")

            # Check table references
            for field in ["left_table", "right_table"]:
                value = rel.get(field, "")
                for pattern in self.UNRESOLVED_PATTERNS:
                    if pattern in str(value):
                        self.errors.append(
                            f"Relationship '{rel_name}' field '{field}' contains unresolved template '{pattern}'"
                        )

    def _check_filters(self, filters_data: Any) -> None:
        """Check filters for unresolved templates."""
        # Handle different data structures
        if isinstance(filters_data, dict):
            filters = filters_data.get("items", [])
        elif isinstance(filters_data, list):
            filters = filters_data
        else:
            return

        for filter_item in filters:
            if not isinstance(filter_item, dict):
                continue

            filter_name = filter_item.get("name", "unknown")
            expr = filter_item.get("expr", "")

            for pattern in self.UNRESOLVED_PATTERNS:
                if pattern in str(expr):
                    self.errors.append(f"Filter '{filter_name}' contains unresolved template '{pattern}'")

    def _check_verified_queries(self, queries_data: Any) -> None:
        """Check verified queries for unresolved templates."""
        # Handle different data structures
        if isinstance(queries_data, dict):
            queries = queries_data.get("items", [])
        elif isinstance(queries_data, list):
            queries = queries_data
        else:
            return

        for query in queries:
            if not isinstance(query, dict):
                continue

            query_name = query.get("name", "unknown")
            sql = query.get("query", query.get("sql", ""))

            for pattern in self.UNRESOLVED_PATTERNS:
                if pattern in str(sql):
                    self.errors.append(f"Verified query '{query_name}' contains unresolved template '{pattern}'")
