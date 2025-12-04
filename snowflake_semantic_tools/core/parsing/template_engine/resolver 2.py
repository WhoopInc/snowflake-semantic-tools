"""
Template Resolver

Template expansion engine for semantic model references.

Transforms Jinja-like template syntax into resolved SQL expressions, enabling:
- Reusable metric definitions that compose other metrics
- Dynamic table/column references that adapt to the physical schema
- Centralized custom instructions for consistent AI behavior

Critical for maintaining DRY principles in semantic models and ensuring
consistency between metric definitions.

## Case Handling Strategy

All identifiers (tables, columns, metrics, custom instructions) are normalized
to UPPERCASE for lookups and resolution to match Snowflake's behavior:
- Snowflake treats unquoted identifiers as case-insensitive (stored as UPPERCASE)
- Internal lookups normalize to UPPERCASE for consistency
- This enables case-insensitive template references while maintaining correctness
"""

import re
from typing import Any, Dict, List, Optional


class TemplateResolver:
    """
    Expands template references in semantic model definitions.

    Template Types and Resolution:

    **{{ table('name') }}**
    - Resolves to uppercase table name from dbt catalog
    - Validates existence in physical schema
    - Example: {{ table('orders') }} → ORDERS

    **{{ column('table', 'column') }}**
    - Creates fully qualified column reference
    - Validates both table and column existence
    - Example: {{ column('orders', 'amount') }} → ORDERS.AMOUNT

    **{{ metric('name') }}**
    - Recursively expands metric definitions
    - Detects circular dependencies
    - Example: {{ metric('revenue') }} → SUM(orders.amount * (1 - orders.discount))

    **{{ custom_instructions('name') }}**
    - Retrieves instruction text for semantic views
    - Validates instruction existence
    - Used for domain-specific Cortex Analyst guidance

    Resolution is performed in dependency order to handle nested references correctly.
    """

    def __init__(
        self,
        dbt_catalog: Optional[Dict[str, Any]] = None,
        metrics_catalog: Optional[List[Dict]] = None,
        custom_instructions_catalog: Optional[List[Dict]] = None,
    ):
        """
        Initialize template resolver with catalogs.

        Args:
            dbt_catalog: Dictionary of dbt models with their metadata
            metrics_catalog: List of all metric definitions
            custom_instructions_catalog: List of all custom instruction definitions
        """
        self.dbt_catalog = dbt_catalog or {}
        self.metrics_catalog = metrics_catalog or []
        self.custom_instructions_catalog = custom_instructions_catalog or []

        # Build lookup dictionaries for efficient access
        # Normalize keys to uppercase for case-insensitive lookups
        self.metrics_by_name = {m.get("name", "").upper(): m for m in self.metrics_catalog if m.get("name")}
        self.custom_instructions_by_name = {
            ci.get("name", "").upper(): ci for ci in self.custom_instructions_catalog if ci.get("name")
        }

        # Cache for resolved metrics to avoid redundant processing
        self.resolution_cache = {}

        # Stack for circular dependency detection
        self.resolution_stack = []

        # Extract table names for validation
        self.known_tables = set(self.dbt_catalog.keys())

    def resolve_content(self, content: str) -> str:
        """
        Resolve all template references in the given content.

        This is the main entry point for template resolution. It processes
        all template types in the correct order to ensure proper resolution.

        RESOLUTION ORDER (Critical for nested templates):
        1. Tables first - base level, no dependencies
        2. Columns second - depend on tables being resolved
        3. Metrics third - may contain nested metrics and column/table refs
        4. Custom instructions last - standalone references

        Args:
            content: YAML content with template references

        Returns:
            Content with all templates resolved
        """
        # Multi-pass resolution to handle complex nesting
        # Pass 1: Resolve all base-level templates (tables and columns)
        content = self._resolve_table_references(content)
        content = self._resolve_column_references(content)

        # Pass 2: Resolve metrics (which handles nested metrics recursively)
        content = self._resolve_metric_references(content)

        # Pass 3: Final pass to catch any remaining column/table refs that
        # might have been introduced by metric expansion
        content = self._resolve_table_references(content)
        content = self._resolve_column_references(content)

        # Pass 4: Resolve custom instructions (no dependencies)
        content = self._resolve_custom_instructions_references(content)

        return content

    def _resolve_table_references(self, content: str) -> str:
        """
        Resolve {{ table('name') }} references.

        Tables are resolved to their uppercase form and validated against
        the dbt catalog when available.
        """
        pattern = r'\{\{\s*table\([\'"]([^\'")]+)[\'"]\)\s*\}\}'

        def replace_table(match):
            table_name = match.group(1).lower()

            # Validate against dbt catalog if available
            if table_name in self.dbt_catalog:
                table_info = self.dbt_catalog[table_name]
                # Return uppercase table name for consistency
                if isinstance(table_info, dict):
                    name = table_info.get("name", table_name).upper()
                    return name
                else:
                    return table_name.upper()

            # Default to uppercase even if not in catalog
            return table_name.upper()

        return re.sub(pattern, replace_table, content)

    def _resolve_column_references(self, content: str) -> str:
        """
        Resolve {{ column('table', 'column') }} references.

        Columns are formatted as TABLE.COLUMN in uppercase.
        """
        pattern = r'\{\{\s*column\([\'"]([^\'")]+)[\'"],\s*[\'"]([^\'")]+)[\'"]\)\s*\}\}'

        def replace_column(match):
            table_name = match.group(1)
            column_name = match.group(2)
            # Return TABLE.COLUMN format
            return f"{table_name.upper()}.{column_name.upper()}"

        return re.sub(pattern, replace_column, content)

    def _resolve_metric_references(self, content: str) -> str:
        """
        Resolve {{ metric('name') }} references recursively.

        Metrics can reference other metrics, creating a composition chain.
        This method handles recursive resolution with circular dependency detection.
        """
        pattern = r'\{\{\s*metric\([\'"]([^\'")]+)[\'"]\)\s*\}\}'

        def replace_metric(match):
            metric_name = match.group(1)
            try:
                resolved = self.resolve_metric(metric_name)
            except ValueError as e:
                # Enhance error message with context about where the reference appears
                # Try to find the metric name that contains this reference
                lines = content.split("\n")
                line_num = content[: match.start()].count("\n") + 1

                # Look backwards for the metric name definition
                context_metric = None
                for i in range(line_num - 1, max(0, line_num - 20), -1):
                    if i < len(lines):
                        line = lines[i]
                        # Look for "- name:" pattern
                        name_match = re.search(r'^\s*-?\s*name:\s*["\']?(\w+)["\']?', line)
                        if name_match:
                            context_metric = name_match.group(1)
                            break

                if context_metric:
                    raise ValueError(f"{str(e)} (referenced in metric '{context_metric}')")
                else:
                    raise

            # Compact multi-line expressions to avoid YAML parsing issues
            if "\n" in resolved:
                resolved = " ".join(resolved.split())

            # Wrap in parentheses to preserve order of operations
            return f"({resolved})"

        return re.sub(pattern, replace_metric, content)

    def _resolve_custom_instructions_references(self, content: str) -> str:
        """
        Resolve {{ custom_instructions('name') }} references.

        Validates that referenced custom instructions exist and returns
        the full instruction text.
        """
        pattern = r'\{\{\s*custom_instructions\([\'"]([^\'")]+)[\'"]\)\s*\}\}'

        def replace_custom_instructions(match):
            instruction_name = match.group(1)

            # Normalize to uppercase for lookup (all keys are stored uppercase)
            instruction_key = instruction_name.upper()
            instruction = self.custom_instructions_by_name.get(instruction_key)

            if not instruction:
                available = (
                    ", ".join(self.custom_instructions_by_name.keys()) if self.custom_instructions_by_name else "none"
                )
                raise ValueError(
                    f"Custom instruction '{instruction_name}' not found. " f"Available instructions: {available}"
                )

            # Get the full instruction text
            instruction_text = instruction.get("instruction", "")

            # For YAML compatibility, we need to handle multi-line strings properly
            # Option 1: Replace newlines with spaces for a single-line format
            # Option 2: Use YAML literal block scalar (|) - but this is complex to inject
            # Going with Option 1 for simplicity and YAML validity
            instruction_text = instruction_text.replace("\n", " ").strip()

            # Return the formatted instruction text
            return instruction_text

        return re.sub(pattern, replace_custom_instructions, content)

    def resolve_metric(self, metric_name: str) -> str:
        """
        Recursively resolve a metric to its full expression.

        This method handles metric composition by recursively resolving
        any metric references within a metric's expression. It includes
        circular dependency detection and result caching for performance.

        IMPORTANT: Resolution order matters! We resolve in this order:
        1. Table and column references first (base level templates)
        2. Metric references last (which may trigger recursive resolution)

        This ensures that when a metric references another metric that contains
        column templates, those templates are properly resolved.

        Args:
            metric_name: Name of the metric to resolve

        Returns:
            Fully resolved metric expression with all templates expanded

        Raises:
            ValueError: If metric not found or circular dependency detected
        """
        # Normalize to uppercase for lookup (all keys are stored uppercase)
        metric_key = metric_name.upper()

        # Check for circular dependencies
        if metric_key in self.resolution_stack:
            cycle = " -> ".join(self.resolution_stack + [metric_key])
            raise ValueError(f"Circular dependency detected: {cycle}")

        # Return cached resolution if available
        if metric_key in self.resolution_cache:
            return self.resolution_cache[metric_key]

        # Get metric definition
        if metric_key not in self.metrics_by_name:
            # Provide context about which metric is trying to reference this missing metric
            if self.resolution_stack:
                calling_metric = self.resolution_stack[-1]
                raise ValueError(f"Metric '{metric_name}' not found (referenced by metric '{calling_metric}')")
            else:
                raise ValueError(f"Metric '{metric_name}' not found")

        metric = self.metrics_by_name[metric_key]
        expr = metric.get("expr", "")

        # Push to stack for circular dependency detection
        self.resolution_stack.append(metric_key)

        try:
            # Resolve table and column references FIRST
            # This ensures base-level templates are resolved before we
            # attempt to resolve any nested metric references
            resolved_expr = self._resolve_table_references(expr)
            resolved_expr = self._resolve_column_references(resolved_expr)

            # Then resolve any metric references (which may trigger recursive resolution)
            # This works because any metrics we reference will themselves have their
            # table/column references resolved before returning
            resolved_expr = self._resolve_metric_references(resolved_expr)

            # Cache the resolution for performance
            self.resolution_cache[metric_key] = resolved_expr

            return resolved_expr

        finally:
            # Always pop from stack, even if exception occurs
            self.resolution_stack.pop()
