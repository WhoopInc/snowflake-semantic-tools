"""Tests for CompileService."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.services.compile import (
    MANIFEST_FILENAME,
    MANIFEST_VERSION,
    CompileConfig,
    CompileResult,
    CompileService,
)

_MODULE = "snowflake_semantic_tools.services.compile"


@pytest.fixture
def project_dir(tmp_path):
    models = tmp_path / "models" / "marts"
    models.mkdir(parents=True)
    (models / "customers.yml").write_text("version: 2\nmodels:\n  - name: customers\n    description: Customer table\n")
    (models / "customers.sql").write_text("SELECT 1")

    sem = tmp_path / "snowflake_semantic_models"
    sem.mkdir()
    (sem / "semantic_views.yml").write_text(
        "semantic_views:\n" "  - name: test_view\n" "    tables:\n" "      - customers\n"
    )
    (sem / "metrics").mkdir()
    (sem / "metrics" / "metrics.yml").write_text(
        "snowflake_metrics:\n  - name: total\n    table_name: customers\n    expr: COUNT(*)\n"
    )

    (tmp_path / "dbt_project.yml").write_text('name: test\nmodel-paths: ["models"]\n')
    (tmp_path / "sst_config.yml").write_text("project:\n  semantic_models_dir: snowflake_semantic_models\n")

    target = tmp_path / "target"
    target.mkdir()
    manifest = {
        "metadata": {"dbt_version": "1.7.0"},
        "nodes": {
            "model.test.customers": {
                "resource_type": "model",
                "name": "customers",
                "database": "DB",
                "schema": "PUBLIC",
                "alias": "customers",
                "original_file_path": "models/marts/customers.sql",
                "checksum": {"checksum": "abc123"},
            }
        },
        "sources": {},
    }
    (target / "manifest.json").write_text(json.dumps(manifest))

    return tmp_path


def _mock_config():
    mock = MagicMock()
    mock.get.side_effect = lambda key, default=None: {
        "project.semantic_models_dir": "snowflake_semantic_models",
    }.get(key, default)
    return mock


class TestCompileService:
    def test_compile_produces_manifest(self, project_dir, monkeypatch):
        monkeypatch.chdir(project_dir)
        with patch(f"{_MODULE}.get_config", return_value=_mock_config()):
            service = CompileService()
            result = service.compile(CompileConfig())

        assert result.success
        assert result.manifest_path is not None
        assert result.manifest_path.exists()
        assert result.tables_count >= 1

    def test_manifest_has_correct_structure(self, project_dir, monkeypatch):
        monkeypatch.chdir(project_dir)
        with patch(f"{_MODULE}.get_config", return_value=_mock_config()):
            service = CompileService()
            result = service.compile(CompileConfig())

        data = json.loads(result.manifest_path.read_text())
        assert "metadata" in data
        assert "file_checksums" in data
        assert "tables" in data
        assert data["metadata"]["schema_version"] == MANIFEST_VERSION

    def test_manifest_tables_have_clean_keys(self, project_dir, monkeypatch):
        monkeypatch.chdir(project_dir)
        with patch(f"{_MODULE}.get_config", return_value=_mock_config()):
            service = CompileService()
            result = service.compile(CompileConfig())

        data = json.loads(result.manifest_path.read_text())
        tables = data["tables"]
        for key in tables:
            assert not key.startswith("sm_"), f"Key '{key}' should not have sm_ prefix"

    def test_manifest_has_file_checksums(self, project_dir, monkeypatch):
        monkeypatch.chdir(project_dir)
        with patch(f"{_MODULE}.get_config", return_value=_mock_config()):
            service = CompileService()
            result = service.compile(CompileConfig())

        data = json.loads(result.manifest_path.read_text())
        assert len(data["file_checksums"]) > 0
        for path, info in data["file_checksums"].items():
            assert "checksum" in info
            assert info["checksum"].startswith("sha256:")

    def test_compile_result_counts(self, project_dir, monkeypatch):
        monkeypatch.chdir(project_dir)
        with patch(f"{_MODULE}.get_config", return_value=_mock_config()):
            service = CompileService()
            result = service.compile(CompileConfig())

        assert result.tables_count >= 1
        assert result.files_tracked >= 1
        assert result.duration > 0

    def test_compile_with_custom_dbt_path(self, project_dir, monkeypatch):
        monkeypatch.chdir(project_dir)
        with patch(f"{_MODULE}.get_config", return_value=_mock_config()):
            service = CompileService()
            result = service.compile(CompileConfig(dbt_path=project_dir / "models"))

        assert result.success

    def test_compile_handles_missing_semantic_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        models = tmp_path / "models"
        models.mkdir()
        (models / "test.yml").write_text("version: 2\nmodels:\n  - name: test\n")
        (tmp_path / "dbt_project.yml").write_text('name: test\nmodel-paths: ["models"]\n')
        (tmp_path / "sst_config.yml").write_text("project:\n  semantic_models_dir: nonexistent\n")

        mock_cfg = MagicMock()
        mock_cfg.get.return_value = "nonexistent"

        with patch(f"{_MODULE}.get_config", return_value=mock_cfg):
            service = CompileService()
            result = service.compile(CompileConfig())

        assert result.success

    def test_internal_fields_stripped(self, project_dir, monkeypatch):
        monkeypatch.chdir(project_dir)
        with patch(f"{_MODULE}.get_config", return_value=_mock_config()):
            service = CompileService()
            result = service.compile(CompileConfig())

        data = json.loads(result.manifest_path.read_text())
        for table_key, records in data["tables"].items():
            for rec in records:
                for field_name in rec:
                    assert not field_name.startswith("_"), f"Internal field '{field_name}' found in {table_key}"


class TestCompileConfig:
    def test_defaults(self):
        config = CompileConfig()
        assert config.dbt_path is None
        assert config.semantic_path is None
        assert config.target_database is None


class TestCompileResult:
    def test_defaults(self):
        result = CompileResult(success=True)
        assert result.manifest_path is None
        assert result.tables_count == 0
        assert result.errors == []


class TestCompileServiceErrorPaths:
    def test_find_dbt_files_failure_returns_c005(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "sst_config.yml").write_text("project:\n  semantic_models_dir: sem\n")
        mock_cfg = MagicMock()
        mock_cfg.get.return_value = "sem"

        with patch(f"{_MODULE}.get_config", return_value=mock_cfg), patch(
            f"{_MODULE}.find_dbt_model_files", side_effect=RuntimeError("boom")
        ):
            service = CompileService()
            result = service.compile(CompileConfig())

        assert not result.success
        assert any("SST-C005" in e for e in result.errors)

    def test_parse_all_files_failure_returns_c005(self, project_dir, monkeypatch):
        monkeypatch.chdir(project_dir)
        with patch(f"{_MODULE}.get_config", return_value=_mock_config()), patch.object(
            CompileService, "_prepare_tables_data"
        ) as mock_prep:
            mock_prep.side_effect = None
            service = CompileService()
            service.parser = MagicMock()
            service.parser.parse_all_files.side_effect = RuntimeError("parse failed")

            result = service.compile(CompileConfig())

        assert not result.success
        assert any("SST-C005" in e for e in result.errors)

    def test_manifest_write_failure_returns_c006(self, project_dir, monkeypatch):
        monkeypatch.chdir(project_dir)
        with patch(f"{_MODULE}.get_config", return_value=_mock_config()), patch(
            "builtins.open", side_effect=OSError("read-only filesystem")
        ):
            service = CompileService()
            service.parser = MagicMock()
            service.parser.parse_all_files.return_value = {"dbt": {"sm_tables": []}, "semantic": {}}
            service.parser.manifest_parser = None

            result = service.compile(CompileConfig())

        assert not result.success
        assert any("SST-C006" in e for e in result.errors)

    def test_compile_with_target_database(self, project_dir, monkeypatch):
        monkeypatch.chdir(project_dir)
        with patch(f"{_MODULE}.get_config", return_value=_mock_config()):
            service = CompileService()
            result = service.compile(CompileConfig(target_database="MY_DB"))

        assert result.success
        assert service.parser.target_database == "MY_DB"


class TestCleanRecords:
    def test_strips_internal_fields(self):
        records = [{"name": "x", "_internal": True, "_raw": "y"}]
        result = CompileService._clean_records(records)
        assert result == [{"name": "x"}]

    def test_strips_none_values(self):
        records = [{"name": "x", "visibility": None, "tags": None}]
        result = CompileService._clean_records(records)
        assert result == [{"name": "x"}]

    def test_strips_empty_lists(self):
        records = [{"name": "x", "sample_values": [], "non_additive_by": []}]
        result = CompileService._clean_records(records)
        assert result == [{"name": "x"}]

    def test_preserves_false_and_zero(self):
        records = [{"name": "x", "derived": False, "count": 0}]
        result = CompileService._clean_records(records)
        assert result == [{"name": "x", "derived": False, "count": 0}]

    def test_preserves_empty_string(self):
        records = [{"name": "x", "sql_file": ""}]
        result = CompileService._clean_records(records)
        assert result == [{"name": "x", "sql_file": ""}]

    def test_relativizes_source_file(self, tmp_path):
        abs_path = str(tmp_path / "models" / "test.yml")
        records = [{"name": "x", "source_file": abs_path}]
        result = CompileService._clean_records(records, project_dir=tmp_path.resolve())
        assert result[0]["source_file"] == "models/test.yml"

    def test_source_file_outside_project_kept_absolute(self, tmp_path):
        records = [{"name": "x", "source_file": "/other/project/test.yml"}]
        result = CompileService._clean_records(records, project_dir=tmp_path.resolve())
        assert result[0]["source_file"] == "/other/project/test.yml"

    def test_tables_json_string_to_native_list(self):
        records = [{"name": "v1", "tables": '["ORDERS", "CUSTOMERS"]'}]
        result = CompileService._clean_records(records)
        assert result[0]["tables"] == ["ORDERS", "CUSTOMERS"]

    def test_custom_instructions_json_string_to_native_list(self):
        records = [{"name": "v1", "custom_instructions": '["RULE_1"]'}]
        result = CompileService._clean_records(records)
        assert result[0]["custom_instructions"] == ["RULE_1"]

    def test_tables_malformed_json_kept_as_string(self):
        records = [{"name": "v1", "tables": "not-json"}]
        result = CompileService._clean_records(records)
        assert result[0]["tables"] == "not-json"

    def test_tables_native_list_preserved(self):
        records = [{"name": "v1", "tables": ["A", "B"]}]
        result = CompileService._clean_records(records)
        assert result[0]["tables"] == ["A", "B"]


class TestTableFiltering:
    def test_staging_tables_filtered_out(self):
        parse_result = {
            "dbt": {
                "sm_tables": [
                    {"table_name": "ORDERS", "database": "DB", "schema": "S"},
                    {"table_name": "STG_ORDERS", "database": "DB", "schema": "S"},
                ],
                "sm_dimensions": [
                    {"table_name": "ORDERS", "name": "STATUS"},
                    {"table_name": "STG_ORDERS", "name": "RAW_STATUS"},
                ],
                "sm_facts": [
                    {"table_name": "ORDERS", "name": "AMOUNT"},
                    {"table_name": "STG_ORDERS", "name": "RAW_AMOUNT"},
                ],
            },
            "semantic": {
                "semantic_views": [
                    {"name": "my_view", "tables": '["ORDERS"]'},
                ],
                "metrics": [
                    {"name": "TOTAL", "table_name": "ORDERS", "tables": ["ORDERS"]},
                ],
            },
        }
        service = CompileService()
        result = service._prepare_tables_data(parse_result)

        table_names = [t["table_name"] for t in result["tables"]]
        assert "ORDERS" in table_names
        assert "STG_ORDERS" not in table_names

        dim_tables = {d["table_name"] for d in result.get("dimensions", [])}
        assert "STG_ORDERS" not in dim_tables

        fact_tables = {f["table_name"] for f in result.get("facts", [])}
        assert "STG_ORDERS" not in fact_tables

    def test_no_filtering_without_semantic_views(self):
        parse_result = {
            "dbt": {
                "sm_tables": [
                    {"table_name": "A", "database": "DB", "schema": "S"},
                    {"table_name": "B", "database": "DB", "schema": "S"},
                ],
            },
            "semantic": {},
        }
        service = CompileService()
        result = service._prepare_tables_data(parse_result)
        assert len(result["tables"]) == 2
