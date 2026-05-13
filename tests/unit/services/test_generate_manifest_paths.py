"""Tests for manifest-first generation paths (error handling, staleness)."""

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.services.generate_semantic_views import (
    SemanticViewGenerationService,
    UnifiedGenerationConfig,
    UnifiedGenerationResult,
)

_MODULE = "snowflake_semantic_tools.services.generate_semantic_views"
_FILE_UTILS = "snowflake_semantic_tools.shared.utils.file_utils"


@pytest.fixture
def gen_service():
    mock_config = MagicMock()
    service = SemanticViewGenerationService.__new__(SemanticViewGenerationService)
    service.config = mock_config
    service.builder = MagicMock()
    service._conn = None
    return service


@pytest.fixture
def gen_config():
    return UnifiedGenerationConfig(
        metadata_database="DB",
        metadata_schema="SCHEMA",
        target_database="DB",
        target_schema="SCHEMA",
    )


class TestGenerateFromManifestErrors:
    def test_missing_manifest_returns_c007(self, gen_service, gen_config, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        progress = MagicMock()
        result = UnifiedGenerationResult()

        gen_service._generate_from_manifest(gen_config, progress, result)

        assert not result.success
        assert any("SST-C007" in e for e in result.errors)
        assert any("not found" in e for e in result.errors)

    def test_corrupt_json_returns_c007(self, gen_service, gen_config, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        (target / "sst_manifest.json").write_text("{invalid json")

        progress = MagicMock()
        result = UnifiedGenerationResult()

        with patch(f"{_FILE_UTILS}.find_dbt_model_files", return_value=[]), patch(
            f"{_FILE_UTILS}.find_semantic_model_files", return_value=[]
        ):
            gen_service._generate_from_manifest(gen_config, progress, result)

        assert not result.success
        assert any("SST-C007" in e for e in result.errors)

    def test_empty_tables_returns_c007(self, gen_service, gen_config, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        (target / "sst_manifest.json").write_text(json.dumps({"metadata": {}, "file_checksums": {}, "tables": {}}))

        progress = MagicMock()
        result = UnifiedGenerationResult()

        with patch(f"{_FILE_UTILS}.find_dbt_model_files", return_value=[]), patch(
            f"{_FILE_UTILS}.find_semantic_model_files", return_value=[]
        ):
            gen_service._generate_from_manifest(gen_config, progress, result)

        assert not result.success
        assert any("SST-C007" in e and "no tables" in e for e in result.errors)


class TestStaleManifestWarning:
    def test_stale_manifest_emits_c008(self, gen_service, gen_config, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        manifest_data = {
            "metadata": {},
            "file_checksums": {},
            "tables": {
                "tables": [{"table_name": "t1"}],
                "semantic_views": [{"name": "v1", "tables": '["t1"]'}],
            },
        }
        manifest_path = target / "sst_manifest.json"
        manifest_path.write_text(json.dumps(manifest_data))

        time.sleep(0.1)
        newer_file = tmp_path / "newer.yml"
        newer_file.write_text("version: 2")

        progress = MagicMock()
        result = UnifiedGenerationResult()

        with patch(f"{_FILE_UTILS}.find_dbt_model_files", return_value=[newer_file]), patch(
            f"{_FILE_UTILS}.find_semantic_model_files", return_value=[]
        ):
            gen_service._generate_from_manifest(gen_config, progress, result)

        assert any("SST-C008" in w for w in result.warnings)

    def test_fresh_manifest_no_warning(self, gen_service, gen_config, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        older_file = tmp_path / "older.yml"
        older_file.write_text("version: 2")
        time.sleep(0.1)

        manifest_data = {
            "metadata": {},
            "file_checksums": {},
            "tables": {
                "tables": [{"table_name": "t1"}],
                "semantic_views": [{"name": "v1", "tables": '["t1"]'}],
            },
        }
        manifest_path = target / "sst_manifest.json"
        manifest_path.write_text(json.dumps(manifest_data))

        progress = MagicMock()
        result = UnifiedGenerationResult()

        with patch(f"{_FILE_UTILS}.find_dbt_model_files", return_value=[older_file]), patch(
            f"{_FILE_UTILS}.find_semantic_model_files", return_value=[]
        ):
            gen_service._generate_from_manifest(gen_config, progress, result)

        assert not any("SST-C008" in w for w in result.warnings)

    def test_staleness_check_handles_missing_config(self, gen_service, gen_config, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        manifest_data = {
            "metadata": {},
            "file_checksums": {},
            "tables": {
                "tables": [{"table_name": "t1"}],
                "semantic_views": [{"name": "v1", "tables": '["t1"]'}],
            },
        }
        (target / "sst_manifest.json").write_text(json.dumps(manifest_data))

        progress = MagicMock()
        result = UnifiedGenerationResult()

        with patch(f"{_FILE_UTILS}.find_dbt_model_files", side_effect=ValueError("no config")), patch(
            f"{_FILE_UTILS}.find_semantic_model_files", return_value=[]
        ):
            gen_service._generate_from_manifest(gen_config, progress, result)

        assert not any("SST-C008" in w for w in result.warnings)
