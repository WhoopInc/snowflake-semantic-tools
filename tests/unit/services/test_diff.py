"""Tests for DiffService."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.services.diff_service import DiffConfig, DiffResult, DiffService, ViewDiff

_MODULE = "snowflake_semantic_tools.services.diff_service"


@pytest.fixture
def diff_config():
    return DiffConfig(database="DB", schema="SCHEMA")


@pytest.fixture
def mock_snowflake_config():
    return MagicMock()


def _write_manifest(tmp_path, tables_data):
    target = tmp_path / "target"
    target.mkdir(exist_ok=True)
    manifest = {"metadata": {}, "file_checksums": {}, "tables": tables_data}
    (target / "sst_manifest.json").write_text(json.dumps(manifest))


def _existing(ddl_dict):
    return (set(ddl_dict.keys()), ddl_dict)


class TestDiffServiceNewViews:
    def test_new_view_detected(self, tmp_path, monkeypatch, mock_snowflake_config, diff_config):
        monkeypatch.chdir(tmp_path)
        _write_manifest(tmp_path, {"tables": [], "semantic_views": [{"name": "v1", "tables": ["T1"]}]})

        service = DiffService(mock_snowflake_config)
        proposed = {"V1": "CREATE SEMANTIC VIEW V1 ..."}

        with patch.object(service, "_get_proposed_ddl", return_value=proposed), patch.object(
            service, "_get_existing_ddl", return_value=(set(), {})
        ):
            result = service.diff(diff_config)

        assert len(result.new) == 1
        assert result.new[0].name == "V1"
        assert result.new[0].status == "new"
        assert result.success

    def test_empty_snowflake_all_views_new(self, mock_snowflake_config, diff_config):
        service = DiffService(mock_snowflake_config)
        proposed = {"V1": "CREATE ...", "V2": "CREATE ..."}

        with patch.object(service, "_get_proposed_ddl", return_value=proposed), patch.object(
            service, "_get_existing_ddl", return_value=(set(), {})
        ):
            result = service.diff(diff_config)

        assert len(result.new) == 2
        assert len(result.modified) == 0
        assert len(result.unchanged) == 0


class TestDiffServiceModifiedViews:
    def test_modified_view_detected(self, mock_snowflake_config, diff_config):
        service = DiffService(mock_snowflake_config)
        proposed = {"V1": "CREATE SEMANTIC VIEW V1 TABLES (T1) METRICS (new_metric)"}
        existing_ddl = {"V1": "CREATE SEMANTIC VIEW V1 TABLES (T1) METRICS (old_metric)"}

        with patch.object(service, "_get_proposed_ddl", return_value=proposed), patch.object(
            service, "_get_existing_ddl", return_value=_existing(existing_ddl)
        ):
            result = service.diff(diff_config)

        assert len(result.modified) == 1
        assert result.modified[0].name == "V1"
        assert result.modified[0].unified_diff is not None
        assert "new_metric" in result.modified[0].unified_diff

    def test_unchanged_view_detected(self, mock_snowflake_config, diff_config):
        sql = "CREATE SEMANTIC VIEW V1 TABLES (T1)"
        service = DiffService(mock_snowflake_config)

        with patch.object(service, "_get_proposed_ddl", return_value={"V1": sql}), patch.object(
            service, "_get_existing_ddl", return_value=_existing({"V1": sql})
        ):
            result = service.diff(diff_config)

        assert len(result.unchanged) == 1
        assert len(result.modified) == 0
        assert not result.has_changes


class TestDiffServiceExtraDeployed:
    def test_extra_deployed_views_warned(self, mock_snowflake_config, diff_config):
        service = DiffService(mock_snowflake_config)
        proposed = {"V1": "CREATE ..."}

        with patch.object(service, "_get_proposed_ddl", return_value=proposed), patch.object(
            service, "_get_existing_ddl", return_value=({"V1", "V2"}, {"V1": "CREATE ..."})
        ):
            result = service.diff(diff_config)

        assert "V2" in result.extra_deployed
        assert any("V2" in w for w in result.warnings)


class TestDiffServiceMixed:
    def test_multiple_views_categorized(self, mock_snowflake_config, diff_config):
        service = DiffService(mock_snowflake_config)
        proposed = {
            "V1": "CREATE V1 same",
            "V2": "CREATE V2 modified_new",
            "V3": "CREATE V3 brand new",
        }
        existing_ddl = {
            "V1": "CREATE V1 same",
            "V2": "CREATE V2 modified_old",
        }
        deployed_names = {"V1", "V2", "V4"}

        with patch.object(service, "_get_proposed_ddl", return_value=proposed), patch.object(
            service, "_get_existing_ddl", return_value=(deployed_names, existing_ddl)
        ):
            result = service.diff(diff_config)

        assert len(result.unchanged) == 1
        assert len(result.modified) == 1
        assert len(result.new) == 1
        assert "V4" in result.extra_deployed

    def test_view_filter_limits_comparison(self, mock_snowflake_config):
        config = DiffConfig(database="DB", schema="S", views_filter=["V1"])
        service = DiffService(mock_snowflake_config)
        proposed = {"V1": "CREATE V1 new", "V2": "CREATE V2 new"}
        existing_ddl = {"V1": "CREATE V1 old"}

        with patch.object(service, "_get_proposed_ddl", return_value=proposed), patch.object(
            service, "_get_existing_ddl", return_value=_existing(existing_ddl)
        ):
            result = service.diff(config)

        assert len(result.modified) == 1
        assert result.modified[0].name == "V1"
        assert len(result.new) == 0


class TestDDLNormalization:
    def test_strips_whitespace(self):
        sql = "  CREATE VIEW V1  \n  TABLES (T1)  \n  "
        assert "  " not in DiffService._normalize_ddl(sql).split("\n")[0]

    def test_case_insensitive_keywords(self):
        sql1 = "create or replace semantic view V1"
        sql2 = "CREATE OR REPLACE SEMANTIC VIEW V1"
        assert DiffService._normalize_ddl(sql1) == DiffService._normalize_ddl(sql2)

    def test_ignores_trailing_semicolons(self):
        sql1 = "CREATE VIEW V1 TABLES (T1);"
        sql2 = "CREATE VIEW V1 TABLES (T1)"
        assert DiffService._normalize_ddl(sql1) == DiffService._normalize_ddl(sql2)

    def test_removes_blank_lines(self):
        sql = "CREATE VIEW V1\n\n\nTABLES (T1)"
        normalized = DiffService._normalize_ddl(sql)
        assert "\n\n" not in normalized


class TestDiffServiceErrors:
    def test_missing_manifest_returns_d002(self, tmp_path, monkeypatch, mock_snowflake_config, diff_config):
        monkeypatch.chdir(tmp_path)
        service = DiffService(mock_snowflake_config)
        result = service.diff(diff_config)

        assert not result.success
        assert any("SST-D002" in e for e in result.errors)

    def test_corrupt_manifest_returns_d002(self, tmp_path, monkeypatch, mock_snowflake_config, diff_config):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        (target / "sst_manifest.json").write_text("{bad json")

        service = DiffService(mock_snowflake_config)
        result = service.diff(diff_config)

        assert not result.success
        assert any("SST-D002" in e for e in result.errors)

    def test_connection_failure_returns_d001(self, mock_snowflake_config, diff_config):
        service = DiffService(mock_snowflake_config)

        def fail_existing(config, result, proposed_names=None):
            result.errors.append("SST-D001: Could not connect to Snowflake: connection refused")
            result.success = False
            return set(), {}

        with patch.object(service, "_get_proposed_ddl", return_value={"V1": "CREATE ..."}):
            service._get_existing_ddl = fail_existing
            result = service.diff(diff_config)

        assert not result.success
        assert any("SST-D001" in e for e in result.errors)

    def test_no_views_returns_d003(self, mock_snowflake_config, diff_config):
        service = DiffService(mock_snowflake_config)

        with patch.object(service, "_get_proposed_ddl", return_value={}), patch.object(
            service, "_get_existing_ddl", return_value=(set(), {})
        ):
            result = service.diff(diff_config)

        assert not result.success
        assert any("SST-D003" in e for e in result.errors)

    def test_partial_get_ddl_failure_continues(self, mock_snowflake_config, diff_config):
        service = DiffService(mock_snowflake_config)
        proposed = {"V1": "CREATE V1", "V2": "CREATE V2 new"}

        def mock_get_existing(config, result, proposed_names=None):
            result.warnings.append("SST-D004: Could not retrieve DDL for 'V2': permission denied")
            return ({"V1"}, {"V1": "CREATE V1"})

        with patch.object(service, "_get_proposed_ddl", return_value=proposed):
            service._get_existing_ddl = mock_get_existing
            result = service.diff(diff_config)

        assert result.success
        assert any("SST-D004" in w for w in result.warnings)
        assert len(result.new) == 1


class TestDiffResult:
    def test_has_changes_with_new(self):
        result = DiffResult(new=[ViewDiff(name="V1", status="new")])
        assert result.has_changes

    def test_has_changes_with_modified(self):
        result = DiffResult(modified=[ViewDiff(name="V1", status="modified")])
        assert result.has_changes

    def test_no_changes_when_all_unchanged(self):
        result = DiffResult(unchanged=[ViewDiff(name="V1", status="unchanged")])
        assert not result.has_changes

    def test_has_changes_with_extra_deployed(self):
        result = DiffResult(extra_deployed=["V1"])
        assert result.has_changes
