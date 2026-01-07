"""
Duplicate Validation

Identifies naming conflicts and duplicate definitions across semantic models.

Prevents confusion and errors by ensuring unique names for all components,
which is critical for Cortex Analyst to correctly identify and use the
intended metrics, relationships, and filters.

Also validates that table-level synonyms are unique within each semantic view,
as Snowflake requires unique synonyms for semantic view generation.
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional

from snowflake_semantic_tools.core.models import ValidationResult


class DuplicateValidator:
    """
    Validates naming uniqueness and detects duplicate definitions.

    Ensures uniqueness across semantic model components:

    **Name Uniqueness**:
    - Metric names must be globally unique
    - Relationship names must not conflict
    - Filter names must be distinct

    **Expression Analysis**:
    - Identifies identical metric expressions (exact duplicates)
    - Detects similar expressions (potential duplicates)
    - Warns about semantically equivalent definitions

    Unique naming is essential for Cortex Analyst to unambiguously
    resolve references and generate correct queries.
    """

    def validate(
        self, semantic_data: Dict[str, Any], dbt_data: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """
        Detect duplicates in semantic data.

        Args:
            semantic_data: Parsed semantic model data
            dbt_data: Optional parsed dbt model data (for table synonym validation)

        Returns:
            ValidationResult with duplicate issues
        """
        result = ValidationResult()

        # Check metrics
        metrics_data = semantic_data.get("metrics", {})
        if metrics_data:
            self._check_metric_duplicates(metrics_data, result)

        # Check relationships
        relationships_data = semantic_data.get("relationships", {})
        if relationships_data:
            self._check_relationship_duplicates(relationships_data, result)

        # Check filters
        filters_data = semantic_data.get("filters", {})
        if filters_data:
            self._check_filter_duplicates(filters_data, result)

        # Check semantic views
        views_data = semantic_data.get("semantic_views", {})
        if views_data:
            self._check_semantic_view_duplicates(views_data, result)

        # Check custom instructions
        instructions_data = semantic_data.get("custom_instructions", {})
        if instructions_data:
            self._check_custom_instruction_duplicates(instructions_data, result)

        # Check verified queries
        queries_data = semantic_data.get("verified_queries", {})
        if queries_data:
            self._check_verified_query_duplicates(queries_data, result)

        # Check tables (dbt models)
        tables_data = semantic_data.get("tables", {})
        if tables_data:
            self._check_table_duplicates(tables_data, result)

        # Check table-level synonym duplicates per semantic view
        if views_data and dbt_data:
            self._check_table_synonym_duplicates_per_semantic_view(views_data, dbt_data, result)

        return result

    def _check_metric_duplicates(self, metrics_data: Dict, result: ValidationResult):
        """Check for duplicate metrics."""
        items = metrics_data.get("items", [])

        # Track names and expressions
        names_seen = defaultdict(list)
        expressions_seen = defaultdict(list)

        for i, metric in enumerate(items):
            if isinstance(metric, dict):
                name = metric.get("name", "")
                expr = metric.get("expr", "")

                # Check duplicate names
                if name:
                    names_seen[name.lower()].append(i)

                # Check duplicate expressions (normalized)
                if expr:
                    normalized_expr = self._normalize_expression(expr)
                    expressions_seen[normalized_expr].append((i, name))

        # Report duplicate names
        for name, indices in names_seen.items():
            if len(indices) > 1:
                result.add_error(
                    f"Duplicate metric name '{name}' found {len(indices)} times", context={"indices": indices}
                )

        # Report duplicate expressions
        for expr, occurrences in expressions_seen.items():
            if len(occurrences) > 1:
                metric_names = [name for _, name in occurrences]
                result.add_warning(
                    f"Metrics {metric_names} have identical expressions",
                    context={"expression": expr[:100]},  # First 100 chars
                )

    def _check_relationship_duplicates(self, relationships_data: Dict, result: ValidationResult):
        """Check for duplicate relationships."""
        items = relationships_data.get("items", [])

        names_seen = defaultdict(list)
        table_pairs_seen = defaultdict(list)

        for i, rel in enumerate(items):
            if isinstance(rel, dict):
                name = rel.get("name", "")
                left_table = rel.get("left_table", "").lower()
                right_table = rel.get("right_table", "").lower()

                # Check duplicate names
                if name:
                    names_seen[name.lower()].append(i)

                # Check duplicate table pairs
                if left_table and right_table:
                    # Use sorted tuple for bidirectional comparison
                    pair = tuple(sorted([left_table, right_table]))
                    table_pairs_seen[pair].append((i, name))

        # Report duplicate names
        for name, indices in names_seen.items():
            if len(indices) > 1:
                result.add_error(
                    f"Duplicate relationship name '{name}' found {len(indices)} times", context={"indices": indices}
                )

        # Report duplicate table pairs
        for (left, right), occurrences in table_pairs_seen.items():
            if len(occurrences) > 1:
                rel_names = [name for _, name in occurrences]
                result.add_warning(
                    f"Multiple relationships between tables {left} and {right}: {rel_names}",
                    context={"tables": [left, right]},
                )

    def _check_filter_duplicates(self, filters_data: Dict, result: ValidationResult):
        """Check for duplicate filters."""
        items = filters_data.get("items", [])

        names_seen = defaultdict(list)

        for i, filter_item in enumerate(items):
            if isinstance(filter_item, dict):
                name = filter_item.get("name", "")

                # Check duplicate names
                if name:
                    names_seen[name.lower()].append(i)

        # Report duplicate names
        for name, indices in names_seen.items():
            if len(indices) > 1:
                result.add_error(
                    f"Duplicate filter name '{name}' found {len(indices)} times", context={"indices": indices}
                )

    def _check_semantic_view_duplicates(self, views_data: Dict, result: ValidationResult):
        """Check for duplicate semantic views."""
        import json

        items = views_data.get("items", [])

        names_seen = defaultdict(list)
        table_sets_seen = defaultdict(list)

        for i, view in enumerate(items):
            if isinstance(view, dict):
                name = view.get("name", "")
                tables = view.get("tables", [])

                # Check duplicate names
                if name:
                    names_seen[name.lower()].append(i)

                # Check identical table lists (order-independent)
                if tables:
                    # BUG FIX: tables may be stored as JSON string, parse it first
                    if isinstance(tables, str):
                        try:
                            tables = json.loads(tables)
                        except (json.JSONDecodeError, TypeError):
                            # If parsing fails, treat as empty list
                            tables = []

                    # Now iterate over the actual list, not the JSON string
                    table_set = frozenset(t.lower() for t in tables if isinstance(t, str))
                    table_sets_seen[table_set].append((i, name))

        # Report duplicate names
        for name, indices in names_seen.items():
            if len(indices) > 1:
                result.add_error(
                    f"Duplicate semantic view name '{name}' found {len(indices)} times", context={"indices": indices}
                )

        # Report identical table sets
        for table_set, occurrences in table_sets_seen.items():
            if len(occurrences) > 1:
                view_names = [name for _, name in occurrences]
                result.add_warning(
                    f"Semantic views {view_names} have identical table lists", context={"tables": list(table_set)}
                )

    def _check_custom_instruction_duplicates(self, instructions_data: Dict, result: ValidationResult):
        """Check for duplicate custom instructions."""
        items = instructions_data.get("items", [])

        names_seen = defaultdict(list)

        for i, instruction in enumerate(items):
            if isinstance(instruction, dict):
                name = instruction.get("name", "")

                # Check duplicate names
                if name:
                    names_seen[name.lower()].append(i)

        # Report duplicate names
        for name, indices in names_seen.items():
            if len(indices) > 1:
                result.add_error(
                    f"Duplicate custom instruction name '{name}' found {len(indices)} times",
                    context={"indices": indices},
                )

    def _check_verified_query_duplicates(self, queries_data: Dict, result: ValidationResult):
        """Check for duplicate verified queries."""
        items = queries_data.get("items", [])

        names_seen = defaultdict(list)

        for i, query in enumerate(items):
            if isinstance(query, dict):
                name = query.get("name", "")

                # Check duplicate names
                if name:
                    names_seen[name.lower()].append(i)

        # Report duplicate names
        for name, indices in names_seen.items():
            if len(indices) > 1:
                result.add_error(
                    f"Duplicate verified query name '{name}' found {len(indices)} times", context={"indices": indices}
                )

    def _check_table_duplicates(self, tables_data: Dict, result: ValidationResult):
        """Check for duplicate table names across dbt models."""
        items = tables_data.get("items", [])

        names_seen = defaultdict(list)

        for i, table in enumerate(items):
            if isinstance(table, dict):
                table_name = table.get("table_name", "")

                # Check duplicate table names
                if table_name:
                    names_seen[table_name.lower()].append(i)

        # Report duplicate names
        for table_name, indices in names_seen.items():
            if len(indices) > 1:
                result.add_error(
                    f"Duplicate table name '{table_name}' found {len(indices)} times", context={"indices": indices}
                )

    def _normalize_expression(self, expr: str) -> str:
        """
        Normalize an expression for comparison.

        Removes whitespace, converts to lowercase, etc.
        """
        import re

        # Remove extra whitespace
        normalized = " ".join(expr.split())

        # Convert to lowercase
        normalized = normalized.lower()

        # Remove comments
        normalized = re.sub(r"--[^\n]*", "", normalized)
        normalized = re.sub(r"/\*.*?\*/", "", normalized, flags=re.DOTALL)

        return normalized.strip()

    def _check_table_synonym_duplicates_per_semantic_view(
        self,
        views_data: Dict[str, Any],
        dbt_data: Dict[str, Any],
        result: ValidationResult,
    ) -> None:
        """
        Check for duplicate TABLE-LEVEL synonyms within each semantic view.

        Snowflake requires that table synonyms are unique within a semantic view.
        This validates that no two tables in the same view share the same synonym.

        Note: Column synonyms are NOT checked here - they can duplicate across tables
        because the same column concept (e.g., "customer name") may exist in multiple
        tables and should share synonyms.

        Args:
            views_data: Semantic views data from parser
            dbt_data: dbt model data containing table-level synonyms
            result: ValidationResult to add errors/warnings to
        """
        import json

        # Build table synonym lookup from dbt data (sm_tables)
        # Format: table_name -> {synonyms: [...], source_file: ...}
        table_synonyms_lookup: Dict[str, Dict[str, Any]] = {}
        sm_tables = dbt_data.get("sm_tables", [])

        for table in sm_tables:
            if isinstance(table, dict):
                table_name = table.get("table_name", "").upper()
                synonyms = table.get("synonyms", [])
                source_file = table.get("source_file", "unknown")

                if table_name and synonyms:
                    table_synonyms_lookup[table_name] = {
                        "synonyms": synonyms,
                        "source_file": source_file,
                    }

        # Check each semantic view for duplicate table synonyms
        items = views_data.get("items", [])

        for view in items:
            if not isinstance(view, dict):
                continue

            view_name = view.get("name", "unknown")
            tables = view.get("tables", [])

            # Handle tables stored as JSON string
            if isinstance(tables, str):
                try:
                    tables = json.loads(tables)
                except (json.JSONDecodeError, TypeError):
                    tables = []

            if not tables:
                continue

            # Collect all synonyms for tables in this view
            # synonym_lower -> [(table_name, original_synonym, source_file), ...]
            synonym_occurrences: Dict[str, List[tuple]] = defaultdict(list)

            for table_name in tables:
                if not isinstance(table_name, str):
                    continue

                table_upper = table_name.upper()
                table_info = table_synonyms_lookup.get(table_upper)

                if table_info:
                    for synonym in table_info["synonyms"]:
                        if isinstance(synonym, str) and synonym.strip():
                            synonym_lower = synonym.lower().strip()
                            synonym_occurrences[synonym_lower].append(
                                (table_name, synonym, table_info["source_file"])
                            )

            # Report duplicates
            for synonym_lower, occurrences in synonym_occurrences.items():
                if len(occurrences) > 1:
                    # Build detailed error message listing ALL conflicting tables
                    table_lines = []
                    for table_name, original_synonym, source_file in occurrences:
                        # Convert to relative path (find models/ or similar common root)
                        rel_path = source_file
                        for marker in ["models/", "staging/", "marts/", "intermediate/"]:
                            if marker in source_file:
                                rel_path = source_file[source_file.find(marker) :]
                                break
                        table_lines.append(f"    - Table '{table_name}' ({rel_path})")

                    tables_list = "\n".join(table_lines)
                    result.add_error(
                        f"Duplicate table synonym '{occurrences[0][1]}' in semantic view '{view_name}'\n"
                        f"  Found in {len(occurrences)} tables:\n"
                        f"{tables_list}\n"
                        f"  Table synonyms must be unique within a semantic view.",
                        context={
                            "type": "DUPLICATE_TABLE_SYNONYM",
                            "semantic_view": view_name,
                            "synonym": occurrences[0][1],
                            "tables": [t for t, _, _ in occurrences],
                        },
                    )
