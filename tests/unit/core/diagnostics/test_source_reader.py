"""Tests for source reader."""

import pytest

from snowflake_semantic_tools.core.diagnostics.source_reader import SourceReader


class TestSourceReader:
    @pytest.fixture(autouse=True)
    def clear_cache(self):
        SourceReader.clear_cache()
        yield
        SourceReader.clear_cache()

    def test_find_entity_line(self, tmp_path):
        test_file = tmp_path / "metrics.yml"
        test_file.write_text(
            "snowflake_metrics:\n"
            "  - name: total_revenue\n"
            "    expr: SUM(amount)\n"
            "  - name: total_orders\n"
            "    expr: COUNT(*)\n"
        )
        assert SourceReader.find_entity_line(str(test_file), "total_revenue") == 2
        assert SourceReader.find_entity_line(str(test_file), "total_orders") == 4

    def test_find_entity_line_not_found(self, tmp_path):
        test_file = tmp_path / "metrics.yml"
        test_file.write_text("snowflake_metrics:\n  - name: foo\n")
        assert SourceReader.find_entity_line(str(test_file), "nonexistent") is None

    def test_find_entity_line_nonexistent_file(self):
        assert SourceReader.find_entity_line("/nonexistent/path.yml", "foo") is None

    def test_get_source_lines(self, tmp_path):
        test_file = tmp_path / "test.yml"
        test_file.write_text("line1\nline2\nline3\nline4\nline5\n")
        lines = SourceReader.get_source_lines(str(test_file), 3, context_before=1, context_after=1)
        assert len(lines) == 3
        assert lines[0] == (2, "line2")
        assert lines[1] == (3, "line3")
        assert lines[2] == (4, "line4")

    def test_get_source_lines_at_start(self, tmp_path):
        test_file = tmp_path / "test.yml"
        test_file.write_text("line1\nline2\nline3\n")
        lines = SourceReader.get_source_lines(str(test_file), 1, context_before=1, context_after=0)
        assert len(lines) == 1
        assert lines[0] == (1, "line1")

    def test_find_field_line(self, tmp_path):
        test_file = tmp_path / "metrics.yml"
        test_file.write_text(
            "snowflake_metrics:\n"
            "  - name: total_revenue\n"
            "    description: Total revenue\n"
            "    expr: SUM(amount)\n"
            "    tables:\n"
            "      - orders\n"
        )
        assert SourceReader.find_field_line(str(test_file), "total_revenue", "expr") == 4
        assert SourceReader.find_field_line(str(test_file), "total_revenue", "tables") == 5

    def test_find_template_column(self):
        line = "    - {{ ref('orders_v2') }}"
        col = SourceReader.find_template_column(line, "orders_v2")
        assert col == 14

    def test_find_template_column_not_found(self):
        line = "    - {{ ref('orders') }}"
        col = SourceReader.find_template_column(line, "nonexistent")
        assert col is None
