"""Tests for SST manifest integration in defer.py."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.interfaces.cli.defer import DeferConfig, _get_sst_yaml_changes, get_modified_views_filter

_DEFER_MODULE = "snowflake_semantic_tools.interfaces.cli.defer"
_MANIFEST_MODULE = "snowflake_semantic_tools.core.parsing.sst_manifest"


def _mock_output():
    m = MagicMock()
    m.info = MagicMock()
    m.warning = MagicMock()
    m.debug = MagicMock()
    return m


def _defer_config(state_path=None, manifest_path=None):
    return DeferConfig(
        enabled=True,
        target="prod",
        state_path=state_path,
        manifest_path=manifest_path or Path("target/manifest.json"),
        only_modified=True,
        source="cli",
    )


class TestGetSSTYamlChanges:
    def test_no_baseline_manifest_returns_none(self, tmp_path):
        output = _mock_output()
        cfg = _defer_config(state_path=tmp_path)
        mock_manifest = MagicMock()
        mock_manifest.build.return_value = mock_manifest

        with patch(f"{_MANIFEST_MODULE}.SSTManifest", return_value=mock_manifest) as MockCls:
            MockCls.load.return_value = None
            result = _get_sst_yaml_changes(cfg, output)

        assert result is None
        output.warning.assert_called()
        assert "SST-G007" in output.warning.call_args[0][0]

    def test_no_sst_changes_returns_empty(self, tmp_path):
        output = _mock_output()
        cfg = _defer_config(state_path=tmp_path)

        mock_current = MagicMock()
        mock_current.build.return_value = mock_current
        mock_current.get_file_view_map.return_value = {}

        mock_baseline = MagicMock()

        mock_diff = MagicMock()
        mock_diff.total_changes = 0
        mock_diff.config_changed = False
        mock_current.compare_to.return_value = mock_diff

        with patch(f"{_MANIFEST_MODULE}.SSTManifest", return_value=mock_current) as MockCls:
            MockCls.load.return_value = mock_baseline
            result = _get_sst_yaml_changes(cfg, output)

        assert result == []

    def test_sst_change_returns_impacted_views(self, tmp_path):
        output = _mock_output()
        cfg = _defer_config(state_path=tmp_path)

        mock_current = MagicMock()
        mock_current.build.return_value = mock_current
        mock_current.get_file_view_map.return_value = {
            "models/customers.yml": ["sales_analytics"],
        }

        mock_baseline = MagicMock()

        mock_diff = MagicMock()
        mock_diff.total_changes = 1
        mock_diff.config_changed = False
        mock_diff.changed_files = ["models/customers.yml"]
        mock_diff.get_impacted_views.return_value = ["sales_analytics"]
        mock_current.compare_to.return_value = mock_diff

        with patch(f"{_MANIFEST_MODULE}.SSTManifest", return_value=mock_current) as MockCls:
            MockCls.load.return_value = mock_baseline
            result = _get_sst_yaml_changes(cfg, output)

        assert result == ["sales_analytics"]

    def test_config_change_returns_none(self, tmp_path):
        output = _mock_output()
        cfg = _defer_config(state_path=tmp_path)

        mock_current = MagicMock()
        mock_current.build.return_value = mock_current
        mock_current.get_file_view_map.return_value = {}

        mock_baseline = MagicMock()

        mock_diff = MagicMock()
        mock_diff.total_changes = 0
        mock_diff.config_changed = True
        mock_diff.changed_files = []
        mock_diff.get_impacted_views.return_value = None
        mock_current.compare_to.return_value = mock_diff

        with patch(f"{_MANIFEST_MODULE}.SSTManifest", return_value=mock_current) as MockCls:
            MockCls.load.return_value = mock_baseline
            result = _get_sst_yaml_changes(cfg, output)

        assert result is None


class TestGetModifiedViewsFilterMerge:
    def test_merges_dbt_and_sst_changes(self, tmp_path):
        output = _mock_output()
        cfg = _defer_config(
            state_path=tmp_path,
            manifest_path=tmp_path / "manifest.json",
        )
        available_views = [
            {"name": "v1", "tables": '["customers"]'},
            {"name": "v2", "tables": '["orders"]'},
            {"name": "v3", "tables": '["products"]'},
        ]

        with patch(f"{_DEFER_MODULE}._get_dbt_model_changes", return_value=["v1"]), patch(
            f"{_DEFER_MODULE}._get_sst_yaml_changes", return_value=["v2"]
        ):
            result = get_modified_views_filter(cfg, available_views, output)

        assert sorted(result) == ["v1", "v2"]

    def test_sst_none_returns_none(self, tmp_path):
        output = _mock_output()
        cfg = _defer_config(manifest_path=tmp_path / "manifest.json")

        with patch(f"{_DEFER_MODULE}._get_dbt_model_changes", return_value=["v1"]), patch(
            f"{_DEFER_MODULE}._get_sst_yaml_changes", return_value=None
        ):
            result = get_modified_views_filter(cfg, [], output)

        assert result is None

    def test_both_empty_returns_empty(self, tmp_path):
        output = _mock_output()
        cfg = _defer_config(manifest_path=tmp_path / "manifest.json")

        with patch(f"{_DEFER_MODULE}._get_dbt_model_changes", return_value=[]), patch(
            f"{_DEFER_MODULE}._get_sst_yaml_changes", return_value=[]
        ):
            result = get_modified_views_filter(cfg, [], output)

        assert result == []

    def test_dedup_views_appearing_in_both(self, tmp_path):
        output = _mock_output()
        cfg = _defer_config(manifest_path=tmp_path / "manifest.json")

        with patch(f"{_DEFER_MODULE}._get_dbt_model_changes", return_value=["v1", "v2"]), patch(
            f"{_DEFER_MODULE}._get_sst_yaml_changes", return_value=["v2", "v3"]
        ):
            result = get_modified_views_filter(cfg, [], output)

        assert result == ["v1", "v2", "v3"]
