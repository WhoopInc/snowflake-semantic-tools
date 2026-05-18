"""Tests for clean service."""

import json
from pathlib import Path

from snowflake_semantic_tools.services.clean import SST_ARTIFACTS, clean


class TestCleanService:
    def test_removes_manifest(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        (target / "sst_manifest.json").write_text("{}")

        result = clean(target)

        assert result.success
        assert len(result.removed) == 1
        assert not (target / "sst_manifest.json").exists()

    def test_removes_semantic_views_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        sv_dir = target / "semantic_views"
        sv_dir.mkdir()
        (sv_dir / "view1.sql").write_text("CREATE OR REPLACE")
        (sv_dir / "view2.sql").write_text("CREATE OR REPLACE")

        result = clean(target)

        assert result.success
        assert len(result.removed) == 1
        assert "2 files" in result.removed[0]
        assert not sv_dir.exists()

    def test_removes_both(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        (target / "sst_manifest.json").write_text("{}")
        sv_dir = target / "semantic_views"
        sv_dir.mkdir()
        (sv_dir / "v.sql").write_text("")

        result = clean(target)

        assert result.success
        assert len(result.removed) == 2

    def test_preserves_dbt_artifacts(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        (target / "manifest.json").write_text("{}")
        (target / "run_results.json").write_text("{}")
        (target / "partial_parse.msgpack").write_bytes(b"\x00")
        (target / "sst_manifest.json").write_text("{}")

        result = clean(target)

        assert result.success
        assert (target / "manifest.json").exists()
        assert (target / "run_results.json").exists()
        assert (target / "partial_parse.msgpack").exists()

    def test_nothing_to_clean(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        result = clean(target)

        assert result.success
        assert len(result.removed) == 0

    def test_target_not_found(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        result = clean(tmp_path / "nonexistent")

        assert not result.success
        assert any("SST-K001" in e for e in result.errors)

    def test_file_count_property(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        (target / "sst_manifest.json").write_text("{}")
        sv_dir = target / "semantic_views"
        sv_dir.mkdir()
        (sv_dir / "v.sql").write_text("")

        result = clean(target)

        assert result.file_count == 1
        assert result.dir_count == 1
