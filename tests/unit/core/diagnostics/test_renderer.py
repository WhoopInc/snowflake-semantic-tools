"""Tests for the diagnostic renderer."""

import pytest

from snowflake_semantic_tools.core.diagnostics.renderer import DiagnosticRenderer
from snowflake_semantic_tools.core.models.validation import ValidationIssue, ValidationSeverity


class TestDiagnosticRenderer:
    @pytest.fixture
    def renderer(self):
        return DiagnosticRenderer(use_colors=False)

    def test_render_basic_error(self, renderer):
        issue = ValidationIssue(
            severity=ValidationSeverity.ERROR,
            message="Metric 'revenue' is missing required field: expr",
            rule_id="SST-V001",
        )
        output = renderer.render(issue)
        assert "error[SST-V001]" in output
        assert "Metric 'revenue' is missing required field: expr" in output

    def test_render_with_file_path(self, renderer):
        issue = ValidationIssue(
            severity=ValidationSeverity.ERROR,
            message="Test error",
            file_path="metrics/metrics.yml",
            line_number=42,
            rule_id="SST-V002",
        )
        output = renderer.render(issue)
        assert "metrics/metrics.yml:42" in output
        assert "-->" in output

    def test_render_with_suggestion(self, renderer):
        issue = ValidationIssue(
            severity=ValidationSeverity.WARNING,
            message="Missing description",
            rule_id="SST-V012",
            suggestion="Add a description to the model definition",
        )
        output = renderer.render(issue)
        assert "= help:" in output
        assert "Add a description to the model definition" in output

    def test_render_warning_severity(self, renderer):
        issue = ValidationIssue(
            severity=ValidationSeverity.WARNING,
            message="Test warning",
            rule_id="SST-V014",
        )
        output = renderer.render(issue)
        assert "warning[SST-V014]" in output

    def test_render_without_rule_id(self, renderer):
        issue = ValidationIssue(
            severity=ValidationSeverity.ERROR,
            message="Generic error",
        )
        output = renderer.render(issue)
        assert "error:" in output
        assert "Generic error" in output
        assert "[]" not in output

    def test_render_batch_groups_by_file(self, renderer):
        issues = [
            ValidationIssue(severity=ValidationSeverity.ERROR, message="Error 1", file_path="a.yml", line_number=10),
            ValidationIssue(severity=ValidationSeverity.ERROR, message="Error 2", file_path="b.yml", line_number=5),
            ValidationIssue(severity=ValidationSeverity.ERROR, message="Error 3", file_path="a.yml", line_number=20),
        ]
        output = renderer.render_batch(issues, group_by_file=True)
        a_first = output.index("Error 1")
        a_second = output.index("Error 3")
        assert a_first < a_second

    def test_render_with_column_and_source(self, renderer, tmp_path):
        test_file = tmp_path / "test.yml"
        test_file.write_text("line1\nline2\n    - {{ ref('bad_table') }}\nline4\n")

        issue = ValidationIssue(
            severity=ValidationSeverity.ERROR,
            message="Unknown table 'bad_table'",
            file_path=str(test_file),
            line_number=3,
            column_number=11,
            rule_id="SST-V002",
            context={"highlight_text": "bad_table"},
        )
        output = renderer.render(issue)
        assert "ref('bad_table')" in output
        assert "^^^^^^^^^" in output

    def test_colors_disabled_with_no_color(self, monkeypatch):
        monkeypatch.setenv("NO_COLOR", "1")
        renderer = DiagnosticRenderer(use_colors=None)
        assert renderer.use_colors is False
