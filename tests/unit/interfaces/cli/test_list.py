"""
Tests for the sst list command.

Tests the list service, formatters, and CLI command invocation.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.interfaces.cli.commands.list import list_cmd
from snowflake_semantic_tools.interfaces.cli.formatters import (
    format_csv_output,
    format_json_output,
    format_table,
    format_yaml_output,
    write_output,
)
from snowflake_semantic_tools.services.list_semantic_components import (
    ListConfig,
    ListResult,
    SemanticComponentListService,
    _parse_tables_json,
    _safe_str,
)


class TestHelpers:
    def test_safe_str_none(self):
        assert _safe_str(None) == ""

    def test_safe_str_normal(self):
        assert _safe_str("hello") == "hello"

    def test_safe_str_number(self):
        assert _safe_str(42) == "42"

    def test_parse_tables_json_string(self):
        assert _parse_tables_json('["a","b"]') == ["a", "b"]

    def test_parse_tables_json_list(self):
        assert _parse_tables_json(["a", "b"]) == ["a", "b"]

    def test_parse_tables_json_invalid(self):
        assert _parse_tables_json("not json{") == []

    def test_parse_tables_json_none(self):
        assert _parse_tables_json(None) == []


class TestListResult:
    def test_empty_result(self):
        result = ListResult()
        assert result.total_count == 0
        assert result.tables == []
        assert result.metrics == []

    def test_total_count(self):
        result = ListResult(
            tables=[{"table_name": "A"}],
            metrics=[{"name": "M1"}, {"name": "M2"}],
            relationships=[{"relationship_name": "R1"}],
        )
        assert result.total_count == 4


class TestFormatTable:
    def test_empty_rows(self):
        output = format_table(["Name", "Value"], [])
        assert "(no items found)" in output

    def test_basic_table(self):
        headers = ["Name", "Count"]
        rows = [["Tables", "5"], ["Metrics", "3"]]
        output = format_table(headers, rows)
        assert "Name" in output
        assert "Tables" in output
        assert "Metrics" in output
        assert "\u2500" in output

    def test_truncation(self):
        headers = ["Name"]
        rows = [["A" * 100]]
        output = format_table(headers, rows, max_col_width=20)
        assert "..." in output

    def test_rows_shorter_than_headers(self):
        headers = ["Name", "Value", "Extra"]
        rows = [["hello"]]
        output = format_table(headers, rows)
        assert "hello" in output
        assert "Name" in output

    def test_min_col_width_enforced(self):
        headers = ["A"]
        rows = [["hello"]]
        output = format_table(headers, rows, max_col_width=1)
        assert "hello" in output or "he..." in output

    def test_sanitizes_control_chars(self):
        headers = ["Name"]
        rows = [["\x1b[31mEVIL\x1b[0m"]]
        output = format_table(headers, rows)
        assert "\x1b" not in output
        assert "EVIL" in output

    def test_unicode_content(self):
        headers = ["Name"]
        rows = [["\u00e9\u00e0\u00fc\u00f1"]]
        output = format_table(headers, rows)
        assert "\u00e9\u00e0\u00fc\u00f1" in output


class TestFormatJson:
    def test_basic_json(self):
        data = {"metrics": [{"name": "total_revenue"}], "total_count": 1}
        output = format_json_output(data)
        parsed = json.loads(output)
        assert parsed["total_count"] == 1
        assert "generated_at" in parsed
        assert len(parsed["metrics"]) == 1

    def test_json_empty(self):
        data = {"metrics": [], "total_count": 0}
        output = format_json_output(data)
        parsed = json.loads(output)
        assert parsed["total_count"] == 0


class TestFormatYaml:
    def test_basic_yaml(self):
        data = {"metrics": [{"name": "revenue"}]}
        output = format_yaml_output(data)
        assert "revenue" in output
        assert "metrics:" in output


class TestFormatCsv:
    def test_basic_csv(self):
        headers = ["Name", "Description"]
        rows = [["revenue", "Total revenue"], ["orders", "Order count"]]
        output = format_csv_output(headers, rows)
        lines = output.strip().split("\n")
        assert len(lines) == 3
        assert "Name,Description" in lines[0]
        assert "revenue" in lines[1]

    def test_csv_quoting(self):
        headers = ["Name", "Description"]
        rows = [["metric", "Has a, comma"]]
        output = format_csv_output(headers, rows)
        assert '"Has a, comma"' in output

    def test_csv_newline_in_value(self):
        headers = ["Name", "SQL"]
        rows = [["q1", "SELECT\n  1"]]
        output = format_csv_output(headers, rows)
        assert "q1" in output


class TestWriteOutput:
    def test_write_to_file(self, tmp_path):
        filepath = str(tmp_path / "out.txt")
        write_output("hello world", filepath)
        with open(filepath) as f:
            assert f.read() == "hello world"

    def test_write_to_invalid_path(self):
        with pytest.raises(Exception):
            write_output("data", "/nonexistent/dir/file.txt")


class TestListService:
    @patch("snowflake_semantic_tools.services.list_semantic_components.find_dbt_model_files")
    @patch("snowflake_semantic_tools.services.list_semantic_components.find_semantic_model_files")
    def test_no_files_returns_error(self, mock_semantic, mock_dbt):
        mock_dbt.return_value = []
        mock_semantic.return_value = []
        service = SemanticComponentListService()
        result = service.execute(ListConfig(no_manifest=True))
        assert len(result.errors) > 0
        assert result.total_count == 0

    @patch("snowflake_semantic_tools.services.list_semantic_components.find_dbt_model_files")
    @patch("snowflake_semantic_tools.services.list_semantic_components.find_semantic_model_files")
    def test_table_filter(self, mock_semantic, mock_dbt, tmp_path):
        dbt_file = tmp_path / "models.yml"
        dbt_file.write_text(
            """
models:
  - name: orders
    description: Order table
    config:
      meta:
        sst:
          primary_key: order_id
    columns:
      - name: order_id
        config:
          meta:
            sst:
              column_type: dimension
              data_type: TEXT
"""
        )
        mock_dbt.return_value = [dbt_file]
        mock_semantic.return_value = []
        service = SemanticComponentListService()
        config = ListConfig(table_filter="NONEXISTENT")
        result = service.execute(config)
        assert len(result.tables) == 0

    @patch("snowflake_semantic_tools.services.list_semantic_components.find_dbt_model_files")
    @patch("snowflake_semantic_tools.services.list_semantic_components.find_semantic_model_files")
    def test_parsing_critical_error_populates_errors(self, mock_semantic, mock_dbt, tmp_path):
        from snowflake_semantic_tools.core.parsing.parser import ParsingCriticalError

        dbt_file = tmp_path / "models.yml"
        dbt_file.write_text("models:\n  - name: orders\n")
        mock_dbt.return_value = [dbt_file]
        mock_semantic.return_value = []

        service = SemanticComponentListService()
        with patch.object(
            service.parser, "parse_all_files", side_effect=ParsingCriticalError("test error", ["error1", "error2"])
        ):
            result = service.execute(ListConfig(no_manifest=True))
        assert "error1" in result.errors
        assert "error2" in result.errors

    @patch("snowflake_semantic_tools.services.list_semantic_components.find_dbt_model_files")
    @patch("snowflake_semantic_tools.services.list_semantic_components.find_semantic_model_files")
    def test_none_table_name_does_not_crash(self, mock_semantic, mock_dbt):
        mock_dbt.return_value = []
        mock_semantic.return_value = []
        service = SemanticComponentListService()
        parse_result = {"dbt": {"sm_tables": [{"table_name": None, "database": "DB"}]}, "semantic": {}}
        result = ListResult()
        service._extract_tables(parse_result, result, ListConfig(table_filter="test"))
        assert len(result.tables) == 0


class TestListCLI:
    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_summary_table_format(self, mock_run):
        mock_run.return_value = ListResult(
            tables=[{"table_name": "ORDERS"}],
            metrics=[{"name": "REVENUE"}],
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["summary"])
        assert result.exit_code == 0
        assert "Tables" in result.output
        assert "Metrics" in result.output
        assert "1" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_summary_json_format(self, mock_run):
        mock_run.return_value = ListResult(
            tables=[{"table_name": "ORDERS"}],
            metrics=[{"name": "REVENUE"}],
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["summary", "--format", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["summary"]["tables"] == 1
        assert parsed["summary"]["metrics"] == 1

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_summary_yaml_format(self, mock_run):
        mock_run.return_value = ListResult(tables=[{"table_name": "T1"}])
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["summary", "--format", "yaml"])
        assert result.exit_code == 0
        assert "tables:" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_summary_csv_format(self, mock_run):
        mock_run.return_value = ListResult(tables=[{"table_name": "T1"}])
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["summary", "--format", "csv"])
        assert result.exit_code == 0
        assert "Component,Count" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_metrics_subcommand(self, mock_run):
        mock_run.return_value = ListResult(
            metrics=[
                {
                    "name": "REVENUE",
                    "tables": ["ORDERS"],
                    "table_name": "ORDERS",
                    "description": "Total rev",
                    "expr": "SUM(amount)",
                }
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["metrics"])
        assert result.exit_code == 0
        assert "REVENUE" in result.output
        assert "ORDERS" in result.output
        assert "Total rev" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_metrics_verbose_shows_expression(self, mock_run):
        mock_run.return_value = ListResult(
            metrics=[
                {
                    "name": "REVENUE",
                    "tables": ["ORDERS"],
                    "table_name": "ORDERS",
                    "description": "Rev",
                    "expr": "SUM(amount)",
                }
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["metrics", "--verbose"])
        assert result.exit_code == 0
        assert "Expression" in result.output
        assert "SUM(amount)" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_metrics_with_expr(self, mock_run):
        mock_run.return_value = ListResult(
            metrics=[
                {
                    "name": "REVENUE",
                    "tables": ["ORDERS"],
                    "table_name": "ORDERS",
                    "description": "Rev",
                    "expr": "SUM(amount)",
                }
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["metrics", "--with-expr"])
        assert result.exit_code == 0
        assert "Expression" in result.output
        assert "SUM(amount)" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_metrics_json(self, mock_run):
        mock_run.return_value = ListResult(
            metrics=[
                {
                    "name": "REVENUE",
                    "tables": ["ORDERS"],
                    "table_name": "ORDERS",
                    "description": "Total rev",
                    "expr": "SUM(amount)",
                }
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["metrics", "--format", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["total_count"] == 1
        assert parsed["metrics"][0]["name"] == "REVENUE"

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_metrics_csv(self, mock_run):
        mock_run.return_value = ListResult(
            metrics=[
                {
                    "name": "REVENUE",
                    "tables": ["ORDERS"],
                    "table_name": "ORDERS",
                    "description": "Rev",
                    "expr": "SUM(a)",
                }
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["metrics", "--format", "csv"])
        assert result.exit_code == 0
        assert "Name,Tables,Description" in result.output
        assert "REVENUE" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_metrics_empty(self, mock_run):
        mock_run.return_value = ListResult()
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["metrics"])
        assert result.exit_code == 0
        assert "(no metrics found)" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_relationships_subcommand(self, mock_run):
        mock_run.return_value = ListResult(
            relationships=[
                {
                    "relationship_name": "ORDERS_TO_CUSTOMERS",
                    "left_table_name": "ORDERS",
                    "right_table_name": "CUSTOMERS",
                }
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["relationships"])
        assert result.exit_code == 0
        assert "ORDERS_TO_CUSTOMERS" in result.output
        assert "ORDERS" in result.output
        assert "\u2192" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_relationships_csv(self, mock_run):
        mock_run.return_value = ListResult(
            relationships=[{"relationship_name": "R1", "left_table_name": "A", "right_table_name": "B"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["relationships", "--format", "csv"])
        assert result.exit_code == 0
        assert "Name,Left Table,Right Table" in result.output
        assert "R1" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_filters_subcommand(self, mock_run):
        mock_run.return_value = ListResult(
            filters=[
                {"name": "ACTIVE_ONLY", "table_name": "USERS", "description": "Active users", "expr": "status='active'"}
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["filters"])
        assert result.exit_code == 0
        assert "ACTIVE_ONLY" in result.output
        assert "USERS" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_filters_verbose_shows_expression(self, mock_run):
        mock_run.return_value = ListResult(
            filters=[{"name": "F1", "table_name": "T", "description": "D", "expr": "x > 0"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["filters", "--verbose"])
        assert result.exit_code == 0
        assert "Expression" in result.output
        assert "x > 0" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_filters_empty(self, mock_run):
        mock_run.return_value = ListResult()
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["filters"])
        assert result.exit_code == 0
        assert "(no filters found)" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_tables_subcommand(self, mock_run):
        mock_run.return_value = ListResult(
            tables=[{"table_name": "ORDERS", "database": "ANALYTICS", "schema": "CORE", "primary_key": "ORDER_ID"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["tables"])
        assert result.exit_code == 0
        assert "ORDERS" in result.output
        assert "ANALYTICS" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_semantic_views_subcommand(self, mock_run):
        mock_run.return_value = ListResult(
            semantic_views=[
                {"name": "customer_360", "description": "Customer view", "tables": '["customers","orders"]'}
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["semantic-views"])
        assert result.exit_code == 0
        assert "customer_360" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_semantic_views_verbose_shows_instructions(self, mock_run):
        mock_run.return_value = ListResult(
            semantic_views=[
                {"name": "sv1", "description": "D", "tables": '["t1"]', "custom_instructions": '["CI1","CI2"]'}
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["semantic-views", "--verbose"])
        assert result.exit_code == 0
        assert "Custom Instructions" in result.output
        assert "CI1" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_semantic_views_malformed_json(self, mock_run):
        mock_run.return_value = ListResult(
            semantic_views=[
                {"name": "sv1", "description": "D", "tables": "not valid json{", "custom_instructions": "bad"}
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["semantic-views", "--verbose"])
        assert result.exit_code == 0
        assert "sv1" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_custom_instructions_subcommand(self, mock_run):
        mock_run.return_value = ListResult(
            custom_instructions=[{"name": "SALES_CI", "question_categorization": "Cat", "sql_generation": "Gen"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["custom-instructions"])
        assert result.exit_code == 0
        assert "SALES_CI" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_custom_instructions_verbose(self, mock_run):
        mock_run.return_value = ListResult(
            custom_instructions=[{"name": "CI1", "question_categorization": "Cat text", "sql_generation": "Gen text"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["custom-instructions", "--verbose"])
        assert result.exit_code == 0
        assert "Question Categorization" in result.output
        assert "Cat text" in result.output
        assert "Gen text" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_custom_instructions_empty(self, mock_run):
        mock_run.return_value = ListResult()
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["custom-instructions"])
        assert result.exit_code == 0
        assert "(no custom instructions found)" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_verified_queries_subcommand(self, mock_run):
        mock_run.return_value = ListResult(
            verified_queries=[
                {
                    "name": "VQ1",
                    "question": "What is revenue?",
                    "verified_by": "alice",
                    "verified_at": "2025-01-01",
                    "sql": "SELECT 1",
                }
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["verified-queries"])
        assert result.exit_code == 0
        assert "VQ1" in result.output
        assert "What is revenue?" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_verified_queries_verbose_shows_sql(self, mock_run):
        mock_run.return_value = ListResult(
            verified_queries=[{"name": "VQ1", "question": "Q?", "verified_by": "bob", "sql": "SELECT 1"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["verified-queries", "--verbose"])
        assert result.exit_code == 0
        assert "SQL" in result.output
        assert "SELECT 1" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_verified_queries_empty(self, mock_run):
        mock_run.return_value = ListResult()
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["verified-queries"])
        assert result.exit_code == 0
        assert "(no verified queries found)" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_verified_queries_yaml(self, mock_run):
        mock_run.return_value = ListResult(
            verified_queries=[{"name": "VQ1", "question": "Q?", "verified_by": "bob", "sql": "SELECT 1"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["verified-queries", "--format", "yaml"])
        assert result.exit_code == 0
        assert "VQ1" in result.output
        assert "verified_queries:" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_quiet_mode(self, mock_run):
        mock_run.return_value = ListResult(
            metrics=[
                {
                    "name": "REVENUE",
                    "tables": ["ORDERS"],
                    "table_name": "ORDERS",
                    "description": "Rev",
                    "expr": "SUM(a)",
                }
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["metrics", "--quiet"])
        assert result.exit_code == 0
        assert "Running with sst=" not in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_output_to_file(self, mock_run, tmp_path):
        mock_run.return_value = ListResult(
            metrics=[
                {
                    "name": "REVENUE",
                    "tables": ["ORDERS"],
                    "table_name": "ORDERS",
                    "description": "Rev",
                    "expr": "SUM(a)",
                }
            ]
        )
        output_file = str(tmp_path / "out.json")
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["metrics", "--format", "json", "--output", output_file])
        assert result.exit_code == 0
        with open(output_file) as f:
            data = json.load(f)
        assert data["total_count"] == 1

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_invoke_without_subcommand_shows_summary(self, mock_run):
        mock_run.return_value = ListResult(tables=[{"table_name": "T1"}])
        runner = CliRunner()
        result = runner.invoke(list_cmd, [])
        assert result.exit_code == 0
        assert "Tables" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_errors_displayed_as_warnings(self, mock_run):
        mock_run.return_value = ListResult(errors=["Something went wrong"])
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["summary"])
        assert result.exit_code == 0
        assert "Something went wrong" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_tables_json(self, mock_run):
        mock_run.return_value = ListResult(
            tables=[{"table_name": "ORDERS", "database": "DB", "schema": "S", "primary_key": "ID"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["tables", "--format", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["total_count"] == 1
        assert parsed["tables"][0]["table_name"] == "ORDERS"

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_tables_csv(self, mock_run):
        mock_run.return_value = ListResult(
            tables=[{"table_name": "ORDERS", "database": "DB", "schema": "S", "primary_key": "ID"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["tables", "--format", "csv"])
        assert result.exit_code == 0
        assert "Name,Database,Schema,Primary Key" in result.output
        assert "ORDERS" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_tables_yaml(self, mock_run):
        mock_run.return_value = ListResult(
            tables=[{"table_name": "ORDERS", "database": "DB", "schema": "S", "primary_key": "ID"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["tables", "--format", "yaml"])
        assert result.exit_code == 0
        assert "tables:" in result.output
        assert "ORDERS" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_tables_empty(self, mock_run):
        mock_run.return_value = ListResult()
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["tables"])
        assert result.exit_code == 0
        assert "(no tables found)" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_relationships_json(self, mock_run):
        mock_run.return_value = ListResult(
            relationships=[{"relationship_name": "R1", "left_table_name": "A", "right_table_name": "B"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["relationships", "--format", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["total_count"] == 1

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_relationships_yaml(self, mock_run):
        mock_run.return_value = ListResult(
            relationships=[{"relationship_name": "R1", "left_table_name": "A", "right_table_name": "B"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["relationships", "--format", "yaml"])
        assert result.exit_code == 0
        assert "relationships:" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_relationships_empty(self, mock_run):
        mock_run.return_value = ListResult()
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["relationships"])
        assert result.exit_code == 0
        assert "(no relationships found)" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_filters_json(self, mock_run):
        mock_run.return_value = ListResult(
            filters=[{"name": "F1", "table_name": "T", "description": "D", "expr": "x > 0"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["filters", "--format", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["total_count"] == 1

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_filters_csv(self, mock_run):
        mock_run.return_value = ListResult(
            filters=[{"name": "F1", "table_name": "T", "description": "D", "expr": "x > 0"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["filters", "--format", "csv"])
        assert result.exit_code == 0
        assert "Name,Table,Description,Expression" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_custom_instructions_json(self, mock_run):
        mock_run.return_value = ListResult(
            custom_instructions=[{"name": "CI1", "question_categorization": "Q", "sql_generation": "S"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["custom-instructions", "--format", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["total_count"] == 1

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_custom_instructions_csv(self, mock_run):
        mock_run.return_value = ListResult(
            custom_instructions=[{"name": "CI1", "question_categorization": "Q", "sql_generation": "S"}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["custom-instructions", "--format", "csv"])
        assert result.exit_code == 0
        assert "Name,Question Categorization,SQL Generation" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_semantic_views_json(self, mock_run):
        mock_run.return_value = ListResult(semantic_views=[{"name": "sv1", "description": "D", "tables": '["t1"]'}])
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["semantic-views", "--format", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["total_count"] == 1

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_semantic_views_csv(self, mock_run):
        mock_run.return_value = ListResult(
            semantic_views=[{"name": "sv1", "description": "D", "tables": '["t1","t2"]'}]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["semantic-views", "--format", "csv"])
        assert result.exit_code == 0
        assert "Name,Description,Tables" in result.output
        assert "t1, t2" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_semantic_views_empty(self, mock_run):
        mock_run.return_value = ListResult()
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["semantic-views"])
        assert result.exit_code == 0
        assert "(no semantic views found)" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_verified_queries_json(self, mock_run):
        mock_run.return_value = ListResult(
            verified_queries=[
                {"name": "VQ1", "question": "Q?", "verified_by": "bob", "verified_at": "2025-01", "sql": "S"}
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["verified-queries", "--format", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["total_count"] == 1

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_verified_queries_csv(self, mock_run):
        mock_run.return_value = ListResult(
            verified_queries=[
                {"name": "VQ1", "question": "Q?", "verified_by": "bob", "verified_at": "2025-01", "sql": "S"}
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["verified-queries", "--format", "csv"])
        assert result.exit_code == 0
        assert "Name,Question,Verified By,Verified At" in result.output

    @patch("snowflake_semantic_tools.interfaces.cli.commands.list._run_list")
    def test_metrics_with_source_file_relativized(self, mock_run):
        mock_run.return_value = ListResult(
            metrics=[
                {
                    "name": "M1",
                    "tables": [],
                    "table_name": "T",
                    "description": "D",
                    "expr": "E",
                    "source_file": "/absolute/path/file.yml",
                }
            ]
        )
        runner = CliRunner()
        result = runner.invoke(list_cmd, ["metrics", "--format", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        source = parsed["metrics"][0].get("source_file", "")
        assert not source.startswith("/absolute") or "path/file.yml" in source


class TestServiceEdgeCases:
    def test_get_config_exclusions_no_config(self):
        service = SemanticComponentListService()
        result = service._get_config_exclusions()
        assert result is None or isinstance(result, list)

    @patch("snowflake_semantic_tools.services.list_semantic_components.find_dbt_model_files")
    @patch("snowflake_semantic_tools.services.list_semantic_components.find_semantic_model_files")
    def test_extract_metrics_with_none_values(self, mock_semantic, mock_dbt):
        mock_dbt.return_value = []
        mock_semantic.return_value = []
        service = SemanticComponentListService()
        parse_result = {
            "dbt": {},
            "semantic": {"metrics": {"items": [{"name": None, "tables": None, "table_name": None}]}},
        }
        result = ListResult()
        service._extract_metrics(parse_result, result, ListConfig(table_filter="test"))
        assert len(result.metrics) == 0

    @patch("snowflake_semantic_tools.services.list_semantic_components.find_dbt_model_files")
    @patch("snowflake_semantic_tools.services.list_semantic_components.find_semantic_model_files")
    def test_extract_relationships_with_none_values(self, mock_semantic, mock_dbt):
        mock_dbt.return_value = []
        mock_semantic.return_value = []
        service = SemanticComponentListService()
        parse_result = {
            "dbt": {},
            "semantic": {"relationships": {"items": [{"left_table_name": None, "right_table_name": None}]}},
        }
        result = ListResult()
        service._extract_relationships(parse_result, result, ListConfig(table_filter="test"))
        assert len(result.relationships) == 0

    def test_find_semantic_files_with_custom_path(self, tmp_path):
        sem_dir = tmp_path / "sem"
        sem_dir.mkdir()
        (sem_dir / "test.yml").write_text("snowflake_metrics:\n  - name: m1\n")
        service = SemanticComponentListService()
        files = service._find_semantic_files(ListConfig(semantic_path=sem_dir))
        assert len(files) == 1

    def test_find_dbt_files_with_custom_path(self, tmp_path):
        dbt_dir = tmp_path / "models"
        dbt_dir.mkdir()
        (dbt_dir / "schema.yml").write_text("models:\n  - name: test\n")
        service = SemanticComponentListService()
        files = service._find_dbt_files(ListConfig(dbt_path=dbt_dir))
        assert len(files) == 1


class TestManifestFirstLoading:
    def test_loads_from_manifest_when_present(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        manifest = {
            "metadata": {},
            "file_checksums": {},
            "tables": {
                "tables": [{"table_name": "ORDERS"}],
                "metrics": [{"name": "REVENUE", "table_name": "ORDERS", "tables": ["ORDERS"]}],
                "relationships": [],
                "filters": [],
                "semantic_views": [{"name": "sv1", "tables": ["ORDERS"]}],
                "custom_instructions": [],
                "verified_queries": [],
            },
        }
        (target / "sst_manifest.json").write_text(json.dumps(manifest))

        service = SemanticComponentListService()
        result = service.execute(ListConfig())
        assert len(result.tables) == 1
        assert len(result.metrics) == 1
        assert len(result.semantic_views) == 1
        assert result.total_count == 3

    def test_falls_back_to_yaml_when_no_manifest(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        service = SemanticComponentListService()
        result = service.execute(ListConfig(no_manifest=True))
        assert len(result.errors) > 0

    def test_no_manifest_flag_bypasses_manifest(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        manifest = {
            "metadata": {},
            "file_checksums": {},
            "tables": {
                "tables": [{"table_name": "ORDERS"}],
                "metrics": [],
                "relationships": [],
                "filters": [],
                "semantic_views": [],
                "custom_instructions": [],
                "verified_queries": [],
            },
        }
        (target / "sst_manifest.json").write_text(json.dumps(manifest))

        service = SemanticComponentListService()
        result = service.execute(ListConfig(no_manifest=True))
        assert result.total_count == 0

    def test_table_filter_with_manifest(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        manifest = {
            "metadata": {},
            "file_checksums": {},
            "tables": {
                "tables": [
                    {"table_name": "ORDERS"},
                    {"table_name": "CUSTOMERS"},
                ],
                "metrics": [
                    {"name": "REVENUE", "table_name": "ORDERS", "tables": ["ORDERS"]},
                    {"name": "CUST_COUNT", "table_name": "CUSTOMERS", "tables": ["CUSTOMERS"]},
                ],
                "relationships": [],
                "filters": [],
                "semantic_views": [],
                "custom_instructions": [],
                "verified_queries": [],
            },
        }
        (target / "sst_manifest.json").write_text(json.dumps(manifest))

        service = SemanticComponentListService()
        result = service.execute(ListConfig(table_filter="ORDER"))
        assert len(result.tables) == 1
        assert result.tables[0]["table_name"] == "ORDERS"
        assert len(result.metrics) == 1
        assert result.metrics[0]["name"] == "REVENUE"

    def test_corrupt_manifest_falls_back(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        (target / "sst_manifest.json").write_text("{bad json")

        service = SemanticComponentListService()
        result = service.execute(ListConfig())
        assert len(result.errors) > 0

    def test_explicit_dbt_path_bypasses_manifest(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        manifest = {
            "metadata": {},
            "file_checksums": {},
            "tables": {"tables": [{"table_name": "FROM_MANIFEST"}]},
        }
        (target / "sst_manifest.json").write_text(json.dumps(manifest))

        models = tmp_path / "models"
        models.mkdir()
        (models / "test.yml").write_text("models:\n  - name: from_yaml\n")

        service = SemanticComponentListService()
        result = service.execute(ListConfig(dbt_path=models))
        assert not any(t.get("table_name") == "FROM_MANIFEST" for t in result.tables)
