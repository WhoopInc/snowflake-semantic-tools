"""
Snowflake SQL Syntax Validator

Validates SQL expressions (metrics, filters, verified queries) against actual Snowflake
to catch syntax errors, typos, and dialect issues before deployment.

This validator catches issues that local parsing cannot detect:
- Invalid function names (CUONT instead of COUNT)
- Snowflake-specific syntax errors
- Type mismatches in expressions

The validation compiles expressions without executing them, so there's no
warehouse compute cost.
"""

import re
from difflib import get_close_matches
from typing import Any, Dict, List, Optional, Tuple

from snowflake_semantic_tools.core.models import ValidationResult
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger(__name__)


# Common Snowflake functions for "Did you mean?" suggestions
SNOWFLAKE_FUNCTIONS = [
    # Aggregate functions
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "MEDIAN",
    "STDDEV",
    "VARIANCE",
    "LISTAGG",
    "ARRAY_AGG",
    "OBJECT_AGG",
    "APPROX_COUNT_DISTINCT",
    # Date/Time functions
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "CURRENT_TIME",
    "GETDATE",
    "DATEADD",
    "DATEDIFF",
    "DATE_TRUNC",
    "DATE_PART",
    "EXTRACT",
    "YEAR",
    "MONTH",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    "WEEK",
    "QUARTER",
    "TO_DATE",
    "TO_TIMESTAMP",
    "TO_TIME",
    "TRY_TO_DATE",
    "TRY_TO_TIMESTAMP",
    # String functions
    "CONCAT",
    "SUBSTRING",
    "SUBSTR",
    "LEFT",
    "RIGHT",
    "LENGTH",
    "LEN",
    "UPPER",
    "LOWER",
    "TRIM",
    "LTRIM",
    "RTRIM",
    "REPLACE",
    "SPLIT",
    "SPLIT_PART",
    "REGEXP_REPLACE",
    "REGEXP_SUBSTR",
    "REGEXP_COUNT",
    # Numeric functions
    "ROUND",
    "FLOOR",
    "CEIL",
    "CEILING",
    "ABS",
    "MOD",
    "POWER",
    "SQRT",
    "LOG",
    "LN",
    "EXP",
    "SIGN",
    "TRUNCATE",
    "TRUNC",
    # Conditional functions
    "COALESCE",
    "NVL",
    "NVL2",
    "NULLIF",
    "IFF",
    "IFNULL",
    "ZEROIFNULL",
    "CASE",
    "DECODE",
    "GREATEST",
    "LEAST",
    # Conversion functions
    "CAST",
    "TRY_CAST",
    "TO_CHAR",
    "TO_NUMBER",
    "TO_DECIMAL",
    "TO_DOUBLE",
    "TO_BOOLEAN",
    "TO_VARIANT",
    "TO_ARRAY",
    "TO_OBJECT",
    # Window functions
    "ROW_NUMBER",
    "RANK",
    "DENSE_RANK",
    "NTILE",
    "LAG",
    "LEAD",
    "FIRST_VALUE",
    "LAST_VALUE",
    "NTH_VALUE",
    # Semi-structured functions
    "PARSE_JSON",
    "TRY_PARSE_JSON",
    "OBJECT_CONSTRUCT",
    "ARRAY_CONSTRUCT",
    "FLATTEN",
    "GET",
    "GET_PATH",
    "ARRAY_SIZE",
    "OBJECT_KEYS",
    # Other common functions
    "DISTINCT",
    "EXISTS",
    "IN",
    "BETWEEN",
    "LIKE",
    "ILIKE",
    "RLIKE",
    "IS_NULL",
    "IS_NOT_NULL",
    "ANY_VALUE",
    "HASH",
    "UUID_STRING",
]


class SnowflakeSyntaxValidator:
    """
    Validates SQL expressions by compiling them against Snowflake.

    This validator catches syntax errors that local parsing cannot detect,
    such as invalid function names, Snowflake-specific syntax issues, and
    type mismatches.

    The validation uses a compile-only approach: expressions are wrapped in
    test queries that compile but don't execute, avoiding any compute cost.
    """

    def __init__(self, snowflake_client):
        """
        Initialize the validator.

        Args:
            snowflake_client: A SnowflakeClient instance for executing queries.
        """
        self.client = snowflake_client
        self._error_cache: Dict[str, str] = {}

    def validate(self, parse_result: Dict[str, Any]) -> ValidationResult:
        """
        Validate all SQL expressions in the parsed semantic models.

        Args:
            parse_result: The parsed semantic model data from Parser.

        Returns:
            ValidationResult with any syntax errors found.
        """
        result = ValidationResult()

        # Extract expressions to validate
        expressions = self._extract_expressions(parse_result)

        if not expressions:
            logger.debug("No SQL expressions found to validate")
            return result

        logger.info(f"Validating {len(expressions)} SQL expressions against Snowflake...")

        # Batch validate for performance
        errors, env_issues = self._batch_validate(expressions)

        # Add syntax errors to result
        for expr_info, error_msg, suggestion in errors:
            entity_type = expr_info.get("type", "expression")
            entity_name = expr_info.get("name", "unknown")
            expression = expr_info.get("expression", "")

            error_text = (
                f"{entity_type.title()} '{entity_name}' has invalid SQL syntax\n"
                f"  Expression: {expression}\n"
                f"  Snowflake: {error_msg}"
            )

            if suggestion:
                error_text += f"\n  Did you mean: {suggestion}?"

            result.add_error(
                error_text,
                context={
                    "type": entity_type,
                    "name": entity_name,
                    "expression": expression,
                    "snowflake_error": error_msg,
                    "suggestion": suggestion,
                },
            )

        # Add environment issues as warnings (not errors)
        # These are NOT syntax errors - just missing tables in current context
        for expr_info, error_msg in env_issues:
            entity_type = expr_info.get("type", "expression")
            entity_name = expr_info.get("name", "unknown")

            warning_text = (
                f"{entity_type.title()} '{entity_name}' could not be fully validated: "
                f"table/object not found in current Snowflake context. "
                f"This is likely an environment issue, not a syntax error."
            )

            result.add_warning(
                warning_text,
                context={
                    "type": entity_type,
                    "name": entity_name,
                    "snowflake_error": error_msg,
                    "is_env_issue": True,
                },
            )

        if errors:
            logger.warning(f"Found {len(errors)} SQL syntax error(s)")
        if env_issues:
            logger.info(f"Skipped {len(env_issues)} expression(s) - tables not found in current context")
        if not errors and not env_issues:
            logger.info("All SQL expressions validated successfully")

        return result

    def _extract_expressions(self, parse_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract all SQL expressions from parsed semantic models.

        Returns a list of dicts with:
        - type: "metric", "filter", or "verified_query"
        - name: The entity name
        - expression: The SQL expression to validate

        Handles both formats:
        - Old format: semantic.sm_metrics (list)
        - New format: semantic.metrics.items (dict with 'items' key)
        """
        expressions = []
        semantic = parse_result.get("semantic", {})

        # Extract metric expressions - handle both formats
        metrics = semantic.get("sm_metrics", [])
        if not metrics:
            # Try new format: metrics.items
            metrics_data = semantic.get("metrics", {})
            if isinstance(metrics_data, dict):
                metrics = metrics_data.get("items", [])
            elif isinstance(metrics_data, list):
                metrics = metrics_data

        for metric in metrics:
            if isinstance(metric, dict):
                expr = metric.get("expr") or metric.get("expression", "")
                if expr:
                    expressions.append(
                        {
                            "type": "metric",
                            "name": metric.get("metric_name") or metric.get("name", "unknown"),
                            "expression": expr,
                        }
                    )

        # Extract filter expressions - handle both formats
        filters = semantic.get("sm_filters", [])
        if not filters:
            filters_data = semantic.get("filters", {})
            if isinstance(filters_data, dict):
                filters = filters_data.get("items", [])
            elif isinstance(filters_data, list):
                filters = filters_data

        for filter_def in filters:
            if isinstance(filter_def, dict):
                expr = filter_def.get("expr") or filter_def.get("expression", "")
                if expr:
                    expressions.append(
                        {
                            "type": "filter",
                            "name": filter_def.get("filter_name") or filter_def.get("name", "unknown"),
                            "expression": expr,
                        }
                    )

        # Extract verified query SQL - handle both formats
        verified_queries = semantic.get("sm_verified_queries", [])
        if not verified_queries:
            vq_data = semantic.get("verified_queries", {})
            if isinstance(vq_data, dict):
                verified_queries = vq_data.get("items", [])
            elif isinstance(vq_data, list):
                verified_queries = vq_data

        for query in verified_queries:
            if isinstance(query, dict):
                sql = query.get("sql", "")
                if sql:
                    expressions.append(
                        {
                            "type": "verified_query",
                            "name": query.get("query_name") or query.get("name", "unknown"),
                            "expression": sql,
                        }
                    )

        return expressions

    def _batch_validate(
        self, expressions: List[Dict[str, Any]]
    ) -> Tuple[List[Tuple[Dict[str, Any], str, Optional[str]]], List[Tuple[Dict[str, Any], str]]]:
        """
        Validate expressions in batches for performance.

        Combines multiple expressions into a single query when possible.
        If a batch fails, isolates individual expressions to identify errors.
        Verified queries are validated individually (can't be batched).

        Returns:
            Tuple of:
            - List of (expression_info, error_message, suggestion) for syntax errors
            - List of (expression_info, error_message) for environment issues (tables not found)
        """
        errors = []
        batch_size = 50  # Combine up to 50 expressions per query

        # Separate verified queries from expressions (VQs can't be batched)
        batchable = [e for e in expressions if e.get("type") != "verified_query"]
        verified_queries = [e for e in expressions if e.get("type") == "verified_query"]

        # Track environment issues separately (not syntax errors)
        env_issues = []

        # Validate verified queries individually (they're full SQL statements)
        for vq in verified_queries:
            result = self._validate_single(vq)
            if result:
                error_msg, suggestion, is_env_issue = result
                if is_env_issue:
                    # Environment issue - track separately, don't report as syntax error
                    env_issues.append((vq, error_msg))
                else:
                    errors.append((vq, error_msg, suggestion))

        # Batch validate expressions (metrics, filters)
        for i in range(0, len(batchable), batch_size):
            batch = batchable[i : i + batch_size]

            # Try batch validation first
            batch_error = self._validate_batch(batch)

            if batch_error is None:
                # Batch passed, all expressions valid
                continue

            # Batch failed - validate individually to isolate errors
            for expr_info in batch:
                result = self._validate_single(expr_info)
                if result:
                    error_msg, suggestion, is_env_issue = result
                    if is_env_issue:
                        env_issues.append((expr_info, error_msg))
                    else:
                        errors.append((expr_info, error_msg, suggestion))

        return errors, env_issues

    def _validate_batch(self, expressions: List[Dict[str, Any]]) -> Optional[str]:
        """
        Validate a batch of expressions in a single query.

        Returns None if all valid, or an error message if any failed.
        """
        if not expressions:
            return None

        # Build batch test query
        select_parts = []
        for idx, expr_info in enumerate(expressions):
            test_expr = self._build_test_expression(expr_info["expression"])
            select_parts.append(f"({test_expr}) AS expr_{idx}")

        test_query = f"SELECT {', '.join(select_parts)} FROM (SELECT 1 WHERE FALSE) AS t"

        try:
            self._execute_test_query(test_query)
            return None
        except Exception as e:
            return str(e)

    def _validate_single(self, expr_info: Dict[str, Any]) -> Optional[Tuple[str, Optional[str], bool]]:
        """
        Validate a single expression.

        Returns None if valid, or (error_message, suggestion, is_env_issue) if invalid.
        is_env_issue=True means it's an environment issue (table not found), not a syntax error.
        """
        expression = expr_info["expression"]
        expr_type = expr_info.get("type", "expression")

        # For verified queries (full SQL statements), use EXPLAIN to validate
        if expr_type == "verified_query":
            test_query = self._build_verified_query_test(expression)
        else:
            # For expressions (metrics, filters), wrap in SELECT
            test_expr = self._build_test_expression(expression)
            test_query = f"SELECT {test_expr} FROM (SELECT 1 WHERE FALSE) AS t"

        try:
            self._execute_test_query(test_query)
            return None
        except Exception as e:
            error_str = str(e)
            error_msg = self._parse_snowflake_error(error_str)

            # Check if this is an environment issue (table/object not found)
            # These are not syntax errors - they're deployment/permission issues
            is_env_issue = self._is_environment_error(error_str)

            suggestion = self._suggest_fix(error_msg, expression)
            return (error_msg, suggestion, is_env_issue)

    def _is_environment_error(self, error: str) -> bool:
        """
        Check if an error is an environment issue rather than a syntax error.

        Environment issues include:
        - Table/object not found
        - Permission denied
        - Schema not found

        These are NOT syntax errors - they occur because the validation
        environment doesn't have access to the actual tables.
        """
        env_patterns = [
            r"does not exist or not authorized",
            r"Object '[^']+' does not exist",
            r"Schema '[^']+' does not exist",
            r"Database '[^']+' does not exist",
            r"Insufficient privileges",
            r"Access denied",
        ]

        error_lower = error.lower()
        for pattern in env_patterns:
            if re.search(pattern, error, re.IGNORECASE):
                return True

        return False

    def _build_verified_query_test(self, sql: str) -> str:
        """
        Build a test query for verified queries (full SQL statements).

        Uses EXPLAIN to validate syntax without executing.
        Falls back to wrapping in subquery if EXPLAIN not supported.
        """
        # Use EXPLAIN to validate without execution
        # This checks syntax without running the query
        return f"EXPLAIN {sql.strip()}"

    def _build_test_expression(self, expression: str) -> str:
        """
        Convert a semantic model expression to a testable SQL expression.

        Replaces column references (TABLE.COLUMN) with NULL to allow
        syntax validation without actual data.
        """
        # Handle common patterns:
        # 1. Qualified column refs: TABLE.COLUMN -> NULL
        # 2. Simple column refs in context: preserve function structure

        # Replace qualified column references with NULL
        # Pattern: WORD.WORD (but not functions like DATEADD.something)
        test_expr = re.sub(r"\b([A-Z_][A-Z0-9_]*)\s*\.\s*([A-Z_][A-Z0-9_]*)\b", "NULL", expression, flags=re.IGNORECASE)

        # If expression is just a simple aggregation with *, preserve it
        # e.g., COUNT(*) should stay as COUNT(*)
        if re.match(r"^\s*\w+\s*\(\s*\*\s*\)\s*$", test_expr):
            return test_expr

        # Replace standalone column names that aren't functions
        # This is tricky - we want to keep function names but replace column names
        # For now, rely on the qualified replacement above

        return test_expr

    def _execute_test_query(self, query: str) -> None:
        """
        Execute a test query against Snowflake.

        Raises an exception if the query has syntax errors.
        """
        with self.client.connection_manager.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
            finally:
                cursor.close()

    def _parse_snowflake_error(self, error: str) -> str:
        """
        Parse a Snowflake error message to extract the meaningful part.

        Snowflake errors often include verbose context; this extracts
        the core error message.
        """
        # Common patterns in Snowflake errors
        patterns = [
            r"SQL compilation error:\s*(.+?)(?:\n|$)",
            r"Unknown function\s+(\w+)",
            r"syntax error.+?unexpected '(.+?)'",
            r"Invalid argument types for function '(\w+)'",
            r"Object '([^']+)' does not exist",
        ]

        for pattern in patterns:
            match = re.search(pattern, error, re.IGNORECASE)
            if match:
                return match.group(0).strip()

        # Return first line if no pattern matches
        first_line = error.split("\n")[0].strip()
        return first_line[:200]  # Limit length

    def _suggest_fix(self, error_msg: str, expression: str) -> Optional[str]:
        """
        Suggest a fix based on the error message.

        Uses fuzzy matching to suggest corrections for typos.
        """
        # Check for unknown function errors
        unknown_func_match = re.search(r"Unknown function\s+(\w+)", error_msg, re.IGNORECASE)
        if unknown_func_match:
            typo = unknown_func_match.group(1).upper()
            suggestions = get_close_matches(typo, SNOWFLAKE_FUNCTIONS, n=1, cutoff=0.6)
            if suggestions:
                return suggestions[0]

        # Check for common typos in the expression itself
        # Extract potential function names from the expression
        func_names = re.findall(r"\b([A-Z_][A-Z0-9_]*)\s*\(", expression, re.IGNORECASE)
        for func_name in func_names:
            func_upper = func_name.upper()
            if func_upper not in SNOWFLAKE_FUNCTIONS:
                suggestions = get_close_matches(func_upper, SNOWFLAKE_FUNCTIONS, n=1, cutoff=0.6)
                if suggestions:
                    return suggestions[0]

        # Check for parenthesis issues
        if "unexpected ')'" in error_msg or "unexpected '('" in error_msg:
            return "Check for unbalanced parentheses"

        # Check for type mismatch
        if "Invalid argument types" in error_msg:
            return "Check argument types match the function requirements"

        return None
