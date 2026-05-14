"""Tests for sst docs CLI commands."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.interfaces.cli.commands.docs import docs_cmd, generate_cmd, serve_cmd

_MODULE = "snowflake_semantic_tools.interfaces.cli.commands.docs"


def _write_manifest(directory: Path, **overrides):
    tables = {
        "tables": [{"table_name": "orders", "database": "DB", "schema": "PUBLIC"}],
        "metrics": [{"name": "total", "tables": ["orders"], "expr": "COUNT(*)"}],
        "relationships": [],
        "filters": [],
        "custom_instructions": [],
        "verified_queries": [],
        "semantic_views": [{"name": "test_view", "tables": ["orders"]}],
    }
    tables.update(overrides)
    manifest = {"metadata": {"sst_version": "0.3.0", "generated_at": "2026-05-13T00:00:00Z"}, "tables": tables}
    target = directory / "target"
    target.mkdir(exist_ok=True)
    (target / "sst_manifest.json").write_text(json.dumps(manifest))


class TestGenerateCommand:
    def test_generate_html_default(self, tmp_path):
        _write_manifest(tmp_path)
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            _write_manifest(Path(td))
            result = runner.invoke(generate_cmd, ["--output", "docs-out"], catch_exceptions=False)
            assert result.exit_code == 0
            assert Path(td, "docs-out", "index.html").exists()
            assert Path(td, "docs-out", "lineage.html").exists()
            assert Path(td, "docs-out", "data.json").exists()

    def test_generate_json_format(self, tmp_path):
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            _write_manifest(Path(td))
            result = runner.invoke(generate_cmd, ["--format", "json", "--output", "json-out"], catch_exceptions=False)
            assert result.exit_code == 0
            assert Path(td, "json-out", "data.json").exists()
            assert not Path(td, "json-out", "index.html").exists()

    def test_generate_custom_output_dir(self, tmp_path):
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            _write_manifest(Path(td))
            result = runner.invoke(generate_cmd, ["--output", "my/custom/path"], catch_exceptions=False)
            assert result.exit_code == 0
            assert Path(td, "my/custom/path/index.html").exists()

    def test_generate_quiet(self, tmp_path):
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            _write_manifest(Path(td))
            result = runner.invoke(generate_cmd, ["--output", "docs", "--quiet"], catch_exceptions=False)
            assert result.exit_code == 0
            assert "Running with sst" not in result.output

    def test_generate_verbose(self, tmp_path):
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            _write_manifest(Path(td))
            result = runner.invoke(generate_cmd, ["--output", "docs", "--verbose"], catch_exceptions=False)
            assert result.exit_code == 0

    def test_generate_shows_component_counts(self, tmp_path):
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            _write_manifest(Path(td))
            result = runner.invoke(generate_cmd, ["--output", "docs"], catch_exceptions=False)
            assert "1 tables" in result.output or "tables" in result.output.lower()
            assert "1 metrics" in result.output or "metrics" in result.output.lower()


class TestServeCommand:
    def test_serve_missing_directory(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(serve_cmd, ["--dir", str(tmp_path / "nonexistent"), "--no-open"])
        assert result.exit_code != 0
        assert "not found" in result.output.lower() or "Directory" in result.output

    def test_serve_no_index_html(self, tmp_path):
        docs_dir = tmp_path / "empty-docs"
        docs_dir.mkdir()
        runner = CliRunner()
        result = runner.invoke(serve_cmd, ["--dir", str(docs_dir), "--no-open"])
        assert result.exit_code != 0
        assert "index.html" in result.output


class TestDocsGroupCommand:
    def test_docs_help(self):
        runner = CliRunner()
        result = runner.invoke(docs_cmd, ["--help"])
        assert result.exit_code == 0
        assert "generate" in result.output.lower()
        assert "serve" in result.output.lower()

    def test_generate_help(self):
        runner = CliRunner()
        result = runner.invoke(generate_cmd, ["--help"])
        assert result.exit_code == 0
        assert "--output" in result.output
        assert "--format" in result.output

    def test_serve_help(self):
        runner = CliRunner()
        result = runner.invoke(serve_cmd, ["--help"])
        assert result.exit_code == 0
        assert "--port" in result.output
        assert "--dir" in result.output


class TestGenerateOutputContent:
    def test_generated_html_is_valid(self, tmp_path):
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            _write_manifest(Path(td))
            runner.invoke(generate_cmd, ["--output", "docs"], catch_exceptions=False)
            html = Path(td, "docs", "index.html").read_text()
            assert html.startswith("<!DOCTYPE html>")
            assert "</html>" in html

    def test_generated_json_is_valid(self, tmp_path):
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            _write_manifest(Path(td))
            runner.invoke(generate_cmd, ["--output", "docs"], catch_exceptions=False)
            data = json.loads(Path(td, "docs", "data.json").read_text())
            assert "catalog" in data
            assert "lineage" in data

    def test_lineage_html_has_graph(self, tmp_path):
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            _write_manifest(Path(td))
            runner.invoke(generate_cmd, ["--output", "docs"], catch_exceptions=False)
            html = Path(td, "docs", "lineage.html").read_text()
            assert "lineage-graph" in html
            assert "d3" in html.lower()

    def test_index_references_lineage(self, tmp_path):
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            _write_manifest(Path(td))
            runner.invoke(generate_cmd, ["--output", "docs"], catch_exceptions=False)
            html = Path(td, "docs", "index.html").read_text()
            assert "lineage.html" in html
