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

        gen_service._generate_from_manifest(gen_config, progress, result)

        assert not result.success
        assert any("SST-C007" in e for e in result.errors)

    def test_empty_tables_returns_c007(self, gen_service, gen_config, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        manifest = {"metadata": {"project_dir": str(tmp_path)}, "file_checksums": {}, "tables": {}}
        (target / "sst_manifest.json").write_text(json.dumps(manifest))

        progress = MagicMock()
        result = UnifiedGenerationResult()

        gen_service._generate_from_manifest(gen_config, progress, result)

        assert not result.success
        assert any("SST-C007" in e and "no tables" in e for e in result.errors)


class TestStaleManifestWarning:
    def test_stale_manifest_emits_c008(self, gen_service, gen_config, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        newer_file = tmp_path / "models" / "test.yml"
        newer_file.parent.mkdir(parents=True)

        manifest_data = {
            "metadata": {"project_dir": str(tmp_path)},
            "file_checksums": {"models/test.yml": {"checksum": "sha256:abc", "type": "dbt"}},
            "tables": {
                "tables": [{"table_name": "t1"}],
                "semantic_views": [{"name": "v1", "tables": ["t1"]}],
            },
        }
        manifest_path = target / "sst_manifest.json"
        manifest_path.write_text(json.dumps(manifest_data))

        time.sleep(0.1)
        newer_file.write_text("version: 2")

        progress = MagicMock()
        result = UnifiedGenerationResult()

        gen_service._generate_from_manifest(gen_config, progress, result)

        assert any("SST-C008" in w for w in result.warnings)

    def test_fresh_manifest_no_warning(self, gen_service, gen_config, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        older_file = tmp_path / "models" / "test.yml"
        older_file.parent.mkdir(parents=True)
        older_file.write_text("version: 2")
        time.sleep(0.1)

        manifest_data = {
            "metadata": {"project_dir": str(tmp_path)},
            "file_checksums": {"models/test.yml": {"checksum": "sha256:abc", "type": "dbt"}},
            "tables": {
                "tables": [{"table_name": "t1"}],
                "semantic_views": [{"name": "v1", "tables": ["t1"]}],
            },
        }
        manifest_path = target / "sst_manifest.json"
        manifest_path.write_text(json.dumps(manifest_data))

        progress = MagicMock()
        result = UnifiedGenerationResult()

        gen_service._generate_from_manifest(gen_config, progress, result)

        assert not any("SST-C008" in w for w in result.warnings)

    def test_staleness_check_skips_missing_files(self, gen_service, gen_config, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        manifest_data = {
            "metadata": {"project_dir": str(tmp_path)},
            "file_checksums": {"models/nonexistent.yml": {"checksum": "sha256:abc", "type": "dbt"}},
            "tables": {
                "tables": [{"table_name": "t1"}],
                "semantic_views": [{"name": "v1", "tables": ["t1"]}],
            },
        }
        (target / "sst_manifest.json").write_text(json.dumps(manifest_data))

        progress = MagicMock()
        result = UnifiedGenerationResult()

        gen_service._generate_from_manifest(gen_config, progress, result)

        assert not any("SST-C008" in w for w in result.warnings)
