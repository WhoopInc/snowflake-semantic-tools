"""
Dependency Validation

Ensures semantic model relationships form valid acyclic graphs.

Prevents infinite loops and undefined behavior by detecting circular
dependencies in metric compositions and relationship chains, which would
cause Cortex Analyst to fail when expanding definitions.
"""

from collections import defaultdict
from typing import Any, Dict, List, Set

from snowflake_semantic_tools.core.models import ValidationResult


class DependencyValidator:
    """
    Validates dependency graphs in semantic models.

    Ensures Composability:
    - **Circular Detection**: Identifies cycles in metric references
    - **Missing References**: Finds metrics that reference non-existent metrics
    - **Depth Analysis**: Warns about deeply nested metric compositions

    Uses topological sorting to verify that metric dependencies form
    a directed acyclic graph (DAG), ensuring all definitions can be
    fully expanded without infinite recursion.

    Deep nesting warnings help maintain readability and performance
    of complex metric compositions.
    """

    def validate(self, semantic_data: Dict[str, Any]) -> ValidationResult:
        """
        Validate dependencies in semantic data.

        Args:
            semantic_data: Parsed semantic model data

        Returns:
            ValidationResult with dependency issues
        """
        result = ValidationResult()

        metrics_data = semantic_data.get("metrics", {})
        if not metrics_data:
            return result

        items = metrics_data.get("items", [])

        # Build dependency graph
        dep_graph = self._build_dependency_graph(items)

        # Check for circular dependencies
        self._check_circular_dependencies(dep_graph, result)

        # Check dependency depth
        self._check_dependency_depth(dep_graph, result)

        # Validate all referenced metrics exist
        self._validate_metric_references(dep_graph, items, result)

        return result

    def _build_dependency_graph(self, metrics: List[Dict]) -> Dict[str, Set[str]]:
        """
        Build a graph of metric dependencies.

        Returns:
            Dictionary mapping metric name to set of metrics it depends on
        """
        import re

        graph = defaultdict(set)

        for metric in metrics:
            if isinstance(metric, dict):
                name = metric.get("name", "")
                expr = metric.get("expr", "")

                if name and expr:
                    # Find metric references in expression
                    # Looking for patterns that were metric references before resolution
                    # Or patterns like (metric_expression) that indicate composition

                    # This is a simplified check - in reality, the template
                    # resolver would have already processed these
                    metric_refs = re.findall(r'{{\\s*metric\\([\'"]([^\'")]+)[\'"]\\)\\s*}}', expr)

                    for ref in metric_refs:
                        graph[name].add(ref)

        return dict(graph)

    def _check_circular_dependencies(self, graph: Dict[str, Set[str]], result: ValidationResult):
        """Check for circular dependencies in the graph."""
        visited = set()
        rec_stack = set()

        def has_cycle(node: str, path: List[str]) -> bool:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            # Check all dependencies
            for neighbor in graph.get(node, set()):
                if neighbor not in visited:
                    if has_cycle(neighbor, path.copy()):
                        return True
                elif neighbor in rec_stack:
                    # Found a cycle
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    result.add_error(f"Circular dependency detected: {' -> '.join(cycle)}", context={"cycle": cycle})
                    return True

            rec_stack.remove(node)
            return False

        # Check each metric
        for metric in graph.keys():
            if metric not in visited:
                has_cycle(metric, [])

    def _check_dependency_depth(self, graph: Dict[str, Set[str]], result: ValidationResult, max_depth: int = 5):
        """Check for excessive dependency depth."""

        def get_depth(metric: str, visited: Set[str] = None) -> int:
            if visited is None:
                visited = set()

            if metric in visited:
                return 0  # Cycle, already reported

            visited.add(metric)

            if metric not in graph or not graph[metric]:
                return 0

            max_child_depth = 0
            for dep in graph[metric]:
                child_depth = get_depth(dep, visited.copy())
                max_child_depth = max(max_child_depth, child_depth)

            return max_child_depth + 1

        # Check depth for each metric
        for metric in graph.keys():
            depth = get_depth(metric)

            if depth > max_depth:
                result.add_warning(
                    f"Metric '{metric}' has dependency depth of {depth} "
                    f"(exceeds recommended maximum of {max_depth})",
                    context={"depth": depth, "max_depth": max_depth},
                )

    def _validate_metric_references(self, graph: Dict[str, Set[str]], metrics: List[Dict], result: ValidationResult):
        """Validate that all referenced metrics exist."""
        # Get all defined metric names
        defined_metrics = set()
        for metric in metrics:
            if isinstance(metric, dict):
                name = metric.get("name", "")
                if name:
                    defined_metrics.add(name)

        # Check all references
        for metric, deps in graph.items():
            for dep in deps:
                if dep not in defined_metrics:
                    result.add_error(
                        f"Metric '{metric}' references undefined metric '{dep}'",
                        context={"metric": metric, "reference": dep},
                    )
