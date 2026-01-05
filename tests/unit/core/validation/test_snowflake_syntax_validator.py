"""
Unit Tests for Snowflake SQL Syntax Validator

Tests the SnowflakeSyntaxValidator class which validates SQL expressions
against Snowflake to catch syntax errors before deployment.
"""

from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.core.validation.rules.snowflake_syntax_validator import (
    SNOWFLAKE_FUNCTIONS,
    SnowflakeSyntaxValidator,
)


class TestBuildTestExpression:
    """Test the _build_test_expression method."""

    @pytest.fixture
    def validator(self):
        """Create validator with mocked client."""
        mock_client = MagicMock()
        return SnowflakeSyntaxValidator(mock_client)

    def test_simple_aggregation(self, validator):
        """Test COUNT(*) is preserved."""
        result = validator._build_test_expression("COUNT(*)")
        assert result == "COUNT(*)"

    def test_qualified_column_replaced_with_null(self, validator):
        """Test TABLE.COLUMN is replaced with NULL."""
        result = validator._build_test_expression("SUM(ORDERS.AMOUNT)")
        assert "NULL" in result
        assert "ORDERS.AMOUNT" not in result

    def test_multiple_qualified_columns(self, validator):
        """Test multiple TABLE.COLUMN refs are replaced."""
        result = validator._build_test_expression("SUM(ORDERS.AMOUNT) / COUNT(ORDERS.ORDER_ID)")
        assert "NULL" in result
        assert "ORDERS.AMOUNT" not in result
        assert "ORDERS.ORDER_ID" not in result

    def test_function_names_preserved(self, validator):
        """Test function names are not replaced."""
        result = validator._build_test_expression("DATEADD(day, 1, ORDERS.DATE)")
        assert "DATEADD" in result
        assert "NULL" in result

    def test_complex_expression(self, validator):
        """Test complex expressions work."""
        result = validator._build_test_expression("CASE WHEN ORDERS.STATUS = 'completed' THEN 1 ELSE 0 END")
        assert "CASE" in result
        assert "WHEN" in result


class TestParseSnowflakeError:
    """Test the _parse_snowflake_error method."""

    @pytest.fixture
    def validator(self):
        """Create validator with mocked client."""
        mock_client = MagicMock()
        return SnowflakeSyntaxValidator(mock_client)

    def test_unknown_function(self, validator):
        """Test parsing unknown function error."""
        error = "SQL compilation error: Unknown function CUONT"
        result = validator._parse_snowflake_error(error)
        assert "Unknown function CUONT" in result

    def test_syntax_error_unexpected(self, validator):
        """Test parsing unexpected token error."""
        error = "SQL compilation error: syntax error line 1 at position 5 unexpected ')'"
        result = validator._parse_snowflake_error(error)
        assert "unexpected" in result or "syntax error" in result

    def test_object_does_not_exist(self, validator):
        """Test parsing object not found error."""
        error = "Object 'MYTABLE' does not exist or not authorized"
        result = validator._parse_snowflake_error(error)
        assert "Object 'MYTABLE' does not exist" in result

    def test_long_error_truncated(self, validator):
        """Test long errors are truncated."""
        long_error = "x" * 500
        result = validator._parse_snowflake_error(long_error)
        assert len(result) <= 200


class TestSuggestFix:
    """Test the _suggest_fix method."""

    @pytest.fixture
    def validator(self):
        """Create validator with mocked client."""
        mock_client = MagicMock()
        return SnowflakeSyntaxValidator(mock_client)

    def test_suggest_count_for_cuont(self, validator):
        """Test COUNT suggested for CUONT typo."""
        error = "Unknown function CUONT"
        result = validator._suggest_fix(error, "CUONT(*)")
        assert result == "COUNT"

    def test_suggest_sum_for_suom(self, validator):
        """Test SUM suggested for SUOM typo."""
        error = "Unknown function SUOM"
        result = validator._suggest_fix(error, "SUOM(x)")
        assert result == "SUM"

    def test_suggest_avg_for_avge(self, validator):
        """Test AVG suggested for AVGE typo."""
        error = "Unknown function AVGE"
        result = validator._suggest_fix(error, "AVGE(x)")
        assert result == "AVG"

    def test_suggest_min_for_minn(self, validator):
        """Test MIN suggested for MINN typo."""
        error = "Unknown function MINN"
        result = validator._suggest_fix(error, "MINN(x)")
        assert result == "MIN"

    def test_suggest_parentheses_for_unbalanced(self, validator):
        """Test parentheses suggestion for syntax error."""
        error = "syntax error unexpected ')'"
        result = validator._suggest_fix(error, "SUM(x))")
        assert "parentheses" in result.lower()

    def test_suggest_type_check(self, validator):
        """Test type check suggestion for invalid arguments."""
        error = "Invalid argument types for function 'SUM'"
        result = validator._suggest_fix(error, "SUM('text')")
        assert "type" in result.lower()

    def test_no_suggestion_for_valid(self, validator):
        """Test no suggestion when no match."""
        error = "Some random error"
        result = validator._suggest_fix(error, "some_expr")
        assert result is None


class TestExtractExpressions:
    """Test the _extract_expressions method."""

    @pytest.fixture
    def validator(self):
        """Create validator with mocked client."""
        mock_client = MagicMock()
        return SnowflakeSyntaxValidator(mock_client)

    def test_extract_metric_expressions_old_format(self, validator):
        """Test extracting metric expressions from old sm_metrics format."""
        parse_result = {
            "semantic": {
                "sm_metrics": [
                    {"metric_name": "total_orders", "expr": "COUNT(*)"},
                    {"name": "revenue", "expression": "SUM(amount)"},
                ]
            }
        }
        expressions = validator._extract_expressions(parse_result)

        assert len(expressions) == 2
        assert expressions[0]["type"] == "metric"
        assert expressions[0]["name"] == "total_orders"
        assert expressions[0]["expression"] == "COUNT(*)"

    def test_extract_metric_expressions_new_format(self, validator):
        """Test extracting metric expressions from new metrics.items format."""
        parse_result = {
            "semantic": {
                "metrics": {
                    "items": [
                        {"name": "total_orders", "expr": "COUNT(*)"},
                        {"name": "revenue", "expr": "SUM(amount)"},
                    ],
                    "warnings": [],
                }
            }
        }
        expressions = validator._extract_expressions(parse_result)

        assert len(expressions) == 2
        assert expressions[0]["type"] == "metric"
        assert expressions[0]["name"] == "total_orders"
        assert expressions[0]["expression"] == "COUNT(*)"

    def test_extract_filter_expressions(self, validator):
        """Test extracting filter expressions."""
        parse_result = {
            "semantic": {
                "sm_filters": [
                    {"filter_name": "active_only", "expr": "status = 'active'"},
                ]
            }
        }
        expressions = validator._extract_expressions(parse_result)

        assert len(expressions) == 1
        assert expressions[0]["type"] == "filter"
        assert expressions[0]["name"] == "active_only"

    def test_extract_filter_expressions_new_format(self, validator):
        """Test extracting filter expressions from new filters.items format."""
        parse_result = {
            "semantic": {
                "filters": {
                    "items": [
                        {"name": "active_only", "expr": "status = 'active'"},
                    ],
                    "warnings": [],
                }
            }
        }
        expressions = validator._extract_expressions(parse_result)

        assert len(expressions) == 1
        assert expressions[0]["type"] == "filter"
        assert expressions[0]["name"] == "active_only"

    def test_extract_verified_query_sql(self, validator):
        """Test extracting verified query SQL."""
        parse_result = {
            "semantic": {
                "sm_verified_queries": [
                    {"query_name": "top_customers", "sql": "SELECT * FROM customers LIMIT 10"},
                ]
            }
        }
        expressions = validator._extract_expressions(parse_result)

        assert len(expressions) == 1
        assert expressions[0]["type"] == "verified_query"
        assert expressions[0]["name"] == "top_customers"

    def test_extract_verified_query_new_format(self, validator):
        """Test extracting verified query SQL from new format."""
        parse_result = {
            "semantic": {
                "verified_queries": {
                    "items": [
                        {"name": "top_customers", "sql": "SELECT * FROM customers LIMIT 10"},
                    ],
                    "warnings": [],
                }
            }
        }
        expressions = validator._extract_expressions(parse_result)

        assert len(expressions) == 1
        assert expressions[0]["type"] == "verified_query"
        assert expressions[0]["name"] == "top_customers"

    def test_extract_all_types(self, validator):
        """Test extracting all expression types."""
        parse_result = {
            "semantic": {
                "sm_metrics": [{"metric_name": "m1", "expr": "COUNT(*)"}],
                "sm_filters": [{"filter_name": "f1", "expr": "x > 0"}],
                "sm_verified_queries": [{"query_name": "q1", "sql": "SELECT 1"}],
            }
        }
        expressions = validator._extract_expressions(parse_result)

        assert len(expressions) == 3
        types = {e["type"] for e in expressions}
        assert types == {"metric", "filter", "verified_query"}

    def test_skip_empty_expressions(self, validator):
        """Test empty expressions are skipped."""
        parse_result = {
            "semantic": {
                "sm_metrics": [
                    {"metric_name": "m1", "expr": "COUNT(*)"},
                    {"metric_name": "m2", "expr": ""},  # Empty
                    {"metric_name": "m3"},  # Missing
                ]
            }
        }
        expressions = validator._extract_expressions(parse_result)

        assert len(expressions) == 1
        assert expressions[0]["name"] == "m1"


class TestBatchValidation:
    """Test batch validation logic."""

    @pytest.fixture
    def validator(self):
        """Create validator with mocked client."""
        mock_client = MagicMock()
        return SnowflakeSyntaxValidator(mock_client)

    def test_batch_success(self, validator):
        """Test batch validation when all expressions valid."""
        validator._execute_test_query = MagicMock(return_value=None)

        expressions = [
            {"type": "metric", "name": "m1", "expression": "COUNT(*)"},
            {"type": "metric", "name": "m2", "expression": "SUM(x)"},
        ]
        errors, env_issues = validator._batch_validate(expressions)

        assert errors == []
        assert env_issues == []

    def test_batch_failure_isolates_errors(self, validator):
        """Test batch failure isolates individual errors."""
        # Batch fails, then individual tests
        call_count = [0]

        def mock_execute(query):
            call_count[0] += 1
            # First call (batch) fails
            if call_count[0] == 1:
                raise Exception("SQL compilation error: Unknown function CUONT")
            # Individual calls - second expression fails
            if "CUONT" in query:
                raise Exception("SQL compilation error: Unknown function CUONT")
            return None

        validator._execute_test_query = mock_execute

        expressions = [
            {"type": "metric", "name": "m1", "expression": "COUNT(*)"},
            {"type": "metric", "name": "m2", "expression": "CUONT(*)"},
        ]
        errors, env_issues = validator._batch_validate(expressions)

        # Only the bad expression should be in errors
        assert len(errors) == 1
        assert errors[0][0]["name"] == "m2"
        assert env_issues == []


class TestValidate:
    """Test the main validate method."""

    @pytest.fixture
    def validator(self):
        """Create validator with mocked client."""
        mock_client = MagicMock()
        return SnowflakeSyntaxValidator(mock_client)

    def test_validate_no_expressions(self, validator):
        """Test validation with no expressions."""
        parse_result = {"semantic": {}}
        result = validator.validate(parse_result)

        assert result.is_valid
        assert result.error_count == 0

    def test_validate_all_valid(self, validator):
        """Test validation with all valid expressions."""
        validator._execute_test_query = MagicMock(return_value=None)

        parse_result = {
            "semantic": {
                "sm_metrics": [
                    {"metric_name": "m1", "expr": "COUNT(*)"},
                ]
            }
        }
        result = validator.validate(parse_result)

        assert result.is_valid
        assert result.error_count == 0

    def test_validate_with_errors(self, validator):
        """Test validation catches errors."""
        validator._execute_test_query = MagicMock(
            side_effect=Exception("SQL compilation error: Unknown function CUONT")
        )

        parse_result = {
            "semantic": {
                "sm_metrics": [
                    {"metric_name": "bad_metric", "expr": "CUONT(*)"},
                ]
            }
        }
        result = validator.validate(parse_result)

        assert not result.is_valid
        assert result.error_count == 1
        # Check error message contains useful info
        errors = result.get_errors()
        assert len(errors) == 1
        error_msg = errors[0].message
        assert "bad_metric" in error_msg
        assert "CUONT" in error_msg


class TestValidateSingle:
    """Test single expression validation."""

    @pytest.fixture
    def validator(self):
        """Create validator with mocked client."""
        mock_client = MagicMock()
        return SnowflakeSyntaxValidator(mock_client)

    def test_valid_expression(self, validator):
        """Test valid expression returns None."""
        validator._execute_test_query = MagicMock(return_value=None)

        expr_info = {"type": "metric", "name": "m1", "expression": "COUNT(*)"}
        result = validator._validate_single(expr_info)

        assert result is None

    def test_invalid_expression_returns_error_and_suggestion(self, validator):
        """Test invalid expression returns error tuple."""
        validator._execute_test_query = MagicMock(side_effect=Exception("Unknown function CUONT"))

        expr_info = {"type": "metric", "name": "m1", "expression": "CUONT(*)"}
        result = validator._validate_single(expr_info)

        assert result is not None
        error_msg, suggestion, is_env_issue = result
        assert "CUONT" in error_msg
        assert suggestion == "COUNT"
        assert is_env_issue is False  # Syntax error, not env issue

    def test_environment_error_detected(self, validator):
        """Test environment errors (table not found) are detected separately."""
        validator._execute_test_query = MagicMock(
            side_effect=Exception("Object 'MY_TABLE' does not exist or not authorized")
        )

        expr_info = {"type": "verified_query", "name": "vq1", "expression": "SELECT * FROM MY_TABLE"}
        result = validator._validate_single(expr_info)

        assert result is not None
        error_msg, suggestion, is_env_issue = result
        assert "does not exist" in error_msg
        assert is_env_issue is True  # Environment issue, not syntax error


class TestEnvironmentIssueDetection:
    """Test detection of environment vs syntax errors."""

    @pytest.fixture
    def validator(self):
        """Create validator with mocked client."""
        mock_client = MagicMock()
        return SnowflakeSyntaxValidator(mock_client)

    def test_table_not_found_is_env_issue(self, validator):
        """Test 'does not exist' errors are classified as environment issues."""
        assert validator._is_environment_error("Object 'MY_TABLE' does not exist or not authorized")
        assert validator._is_environment_error("SQL compilation error: Schema 'BAD_SCHEMA' does not exist")
        assert validator._is_environment_error("Database 'BAD_DB' does not exist")

    def test_syntax_error_is_not_env_issue(self, validator):
        """Test syntax errors are not classified as environment issues."""
        assert not validator._is_environment_error("Unknown function CUONT")
        assert not validator._is_environment_error("SQL compilation error: syntax error")
        assert not validator._is_environment_error("Invalid argument types for function")

    def test_verified_query_env_issue_becomes_warning(self, validator):
        """Test verified queries with env issues become warnings, not errors."""

        # Mock execute to return env issue for VQ
        def mock_execute(query):
            if "EXPLAIN" in query:
                raise Exception("Object 'MISSING_TABLE' does not exist or not authorized")
            return None

        validator._execute_test_query = mock_execute

        parse_result = {
            "semantic": {"sm_verified_queries": [{"query_name": "vq1", "sql": "SELECT * FROM MISSING_TABLE"}]}
        }

        result = validator.validate(parse_result)

        # Should have warning, not error
        assert result.error_count == 0
        assert result.warning_count == 1


class TestSnowflakeFunctionsList:
    """Test the SNOWFLAKE_FUNCTIONS constant."""

    def test_common_aggregates_included(self):
        """Test common aggregate functions are in the list."""
        for func in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
            assert func in SNOWFLAKE_FUNCTIONS

    def test_date_functions_included(self):
        """Test date functions are in the list."""
        for func in ["DATEADD", "DATEDIFF", "DATE_TRUNC"]:
            assert func in SNOWFLAKE_FUNCTIONS

    def test_string_functions_included(self):
        """Test string functions are in the list."""
        for func in ["CONCAT", "UPPER", "LOWER", "TRIM"]:
            assert func in SNOWFLAKE_FUNCTIONS

    def test_conditional_functions_included(self):
        """Test conditional functions are in the list."""
        for func in ["COALESCE", "NVL", "IFF", "CASE"]:
            assert func in SNOWFLAKE_FUNCTIONS


class TestConfigIntegration:
    """Test config/CLI flag integration for syntax check."""

    def test_cli_flag_true_overrides_config_false(self):
        """Test CLI flag takes precedence over config."""
        from snowflake_semantic_tools.interfaces.cli.commands.validate import _should_run_syntax_check

        # CLI flag explicitly set should override config
        with patch(
            "snowflake_semantic_tools.interfaces.cli.commands.validate.get_config",
            return_value={"validation": {"snowflake_syntax_check": False}},
        ):
            assert _should_run_syntax_check(cli_flag=True) is True

    def test_cli_flag_false_overrides_config_true(self):
        """Test CLI --no-snowflake-check overrides config."""
        from snowflake_semantic_tools.interfaces.cli.commands.validate import _should_run_syntax_check

        with patch(
            "snowflake_semantic_tools.interfaces.cli.commands.validate.get_config",
            return_value={"validation": {"snowflake_syntax_check": True}},
        ):
            assert _should_run_syntax_check(cli_flag=False) is False

    def test_config_true_enables_check(self):
        """Test config enables check when no CLI flag."""
        from snowflake_semantic_tools.interfaces.cli.commands.validate import _should_run_syntax_check

        with patch(
            "snowflake_semantic_tools.interfaces.cli.commands.validate.get_config",
            return_value={"validation": {"snowflake_syntax_check": True}},
        ):
            assert _should_run_syntax_check(cli_flag=None) is True

    def test_default_is_false(self):
        """Test default is False when no config or flag."""
        from snowflake_semantic_tools.interfaces.cli.commands.validate import _should_run_syntax_check

        with patch(
            "snowflake_semantic_tools.interfaces.cli.commands.validate.get_config",
            return_value=None,
        ):
            assert _should_run_syntax_check(cli_flag=None) is False
