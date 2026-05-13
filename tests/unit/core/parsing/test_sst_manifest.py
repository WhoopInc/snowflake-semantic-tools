"""Tests for SSTManifest — build, save, load, compare."""

import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.core.parsing.sst_manifest import MANIFEST_FILENAME, SSTManifest, SSTManifestDiff, _sha256

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def project_dir(tmp_path):
    models_dir = tmp_path / "models" / "marts"
    models_dir.mkdir(parents=True)
    (models_dir / "customers.yml").write_text("models:\n  - name: customers\n")
    (models_dir / "orders.yml").write_text("models:\n  - name: orders\n")
    (models_dir / "customers.sql").write_text("SELECT 1")

    sem_dir = tmp_path / "snowflake_semantic_models"
    sem_dir.mkdir()
    (sem_dir / "metrics").mkdir()
    (sem_dir / "metrics" / "metrics.yml").write_text("snowflake_metrics:\n  - name: total_revenue\n")
    (sem_dir / "relationships").mkdir()
    (sem_dir / "relationships" / "relationships.yml").write_text(
        "snowflake_relationships:\n  - name: customer_orders\n"
    )
    (sem_dir / "semantic_views.yml").write_text(
        "semantic_views:\n"
        "  - name: sales_analytics\n"
        "    tables:\n"
        "      - customers\n"
        "      - orders\n"
        "  - name: customer_only\n"
        "    tables:\n"
        "      - customers\n"
    )

    (tmp_path / "dbt_project.yml").write_text('name: test_project\nmodel-paths: ["models"]\n')
    (tmp_path / "sst_config.yml").write_text("project:\n  semantic_models_dir: snowflake_semantic_models\n")
    return tmp_path


def _mock_config(project_dir):
    mock_cfg = MagicMock()

    def _get(key, default=None):
        mapping = {
            "project.semantic_models_dir": "snowflake_semantic_models",
        }
        return mapping.get(key, default)

    mock_cfg.get = _get
    return mock_cfg


# ---------------------------------------------------------------------------
# SSTManifestDiff
# ---------------------------------------------------------------------------


class TestSSTManifestDiff:
    def test_changed_files_includes_added_modified_removed(self):
        diff = SSTManifestDiff(added=["a.yml"], modified=["b.yml"], removed=["c.yml"])
        assert diff.changed_files == ["a.yml", "b.yml", "c.yml"]

    def test_total_changes(self):
        diff = SSTManifestDiff(added=["a.yml"], modified=["b.yml", "c.yml"], removed=["d.yml"])
        assert diff.total_changes == 4

    def test_summary(self):
        diff = SSTManifestDiff(
            added=["a"],
            modified=["b"],
            removed=["c"],
            unchanged=["d"],
            config_changed=True,
        )
        s = diff.summary()
        assert "1 added" in s
        assert "1 modified" in s
        assert "1 removed" in s
        assert "config changed" in s

    def test_no_changes_summary(self):
        diff = SSTManifestDiff()
        assert diff.summary() == "no changes"

    def test_get_impacted_views_config_changed_returns_none(self):
        diff = SSTManifestDiff(config_changed=True)
        assert diff.get_impacted_views({}) is None

    def test_get_impacted_views_cross_cutting_file_returns_none(self):
        diff = SSTManifestDiff(modified=["metrics.yml"])
        assert diff.get_impacted_views({"metrics.yml": None}) is None

    def test_get_impacted_views_scoped(self):
        diff = SSTManifestDiff(modified=["customers.yml"])
        result = diff.get_impacted_views(
            {
                "customers.yml": ["sales_analytics", "customer_only"],
            }
        )
        assert result == ["customer_only", "sales_analytics"]

    def test_get_impacted_views_removed_file_uses_baseline(self):
        diff = SSTManifestDiff(removed=["old_metrics.yml"])
        result = diff.get_impacted_views(
            {},
            baseline_file_view_map={"old_metrics.yml": None},
        )
        assert result is None

    def test_get_impacted_views_empty(self):
        diff = SSTManifestDiff()
        assert diff.get_impacted_views({}) == []


# ---------------------------------------------------------------------------
# _sha256
# ---------------------------------------------------------------------------


class TestSha256:
    def test_correct_hash(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello world")
        expected = hashlib.sha256(b"hello world").hexdigest()
        assert _sha256(f) == f"sha256:{expected}"


# ---------------------------------------------------------------------------
# SSTManifest.build
# ---------------------------------------------------------------------------


class TestSSTManifestBuild:
    def test_discovers_dbt_yaml_files(self, project_dir):
        with patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_config",
            return_value=_mock_config(project_dir),
        ), patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_dbt_model_paths",
            return_value=[project_dir / "models"],
        ):
            m = SSTManifest(project_dir)
            m.build()
            dbt_files = [k for k, v in m.files.items() if v["type"] == "dbt"]
            assert len(dbt_files) >= 2
            basenames = [Path(f).name for f in dbt_files]
            assert "customers.yml" in basenames
            assert "orders.yml" in basenames

    def test_discovers_semantic_yaml_files(self, project_dir):
        with patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_config",
            return_value=_mock_config(project_dir),
        ), patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_dbt_model_paths",
            return_value=[project_dir / "models"],
        ):
            m = SSTManifest(project_dir)
            m.build()
            sem_files = [k for k, v in m.files.items() if v["type"] != "dbt"]
            assert len(sem_files) >= 3
            types = {v["type"] for k, v in m.files.items() if v["type"] != "dbt"}
            assert "metrics" in types
            assert "relationships" in types
            assert "semantic_views" in types

    def test_computes_sha256_checksums(self, project_dir):
        with patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_config",
            return_value=_mock_config(project_dir),
        ), patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_dbt_model_paths",
            return_value=[project_dir / "models"],
        ):
            m = SSTManifest(project_dir)
            m.build()
            for entry in m.files.values():
                assert entry["checksum"].startswith("sha256:")
                assert len(entry["checksum"]) == 7 + 64  # "sha256:" + 64 hex chars

    def test_maps_dbt_yaml_to_views(self, project_dir):
        with patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_config",
            return_value=_mock_config(project_dir),
        ), patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_dbt_model_paths",
            return_value=[project_dir / "models"],
        ):
            m = SSTManifest(project_dir)
            m.build()
            customers_entry = None
            for k, v in m.files.items():
                if "customers.yml" in k and v["type"] == "dbt":
                    customers_entry = v
                    break
            assert customers_entry is not None
            assert customers_entry["views_impacted"] is not None
            assert "sales_analytics" in customers_entry["views_impacted"]
            assert "customer_only" in customers_entry["views_impacted"]

    def test_maps_metrics_to_all_views(self, project_dir):
        with patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_config",
            return_value=_mock_config(project_dir),
        ), patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_dbt_model_paths",
            return_value=[project_dir / "models"],
        ):
            m = SSTManifest(project_dir)
            m.build()
            metrics_entry = None
            for k, v in m.files.items():
                if "metrics" in k and v["type"] == "metrics":
                    metrics_entry = v
                    break
            assert metrics_entry is not None
            assert metrics_entry["views_impacted"] is None

    def test_includes_config_checksum(self, project_dir):
        with patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_config",
            return_value=_mock_config(project_dir),
        ), patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_dbt_model_paths",
            return_value=[project_dir / "models"],
        ):
            m = SSTManifest(project_dir)
            m.build()
            assert m.config_checksum is not None
            assert m.config_checksum.startswith("sha256:")

    def test_missing_semantic_models_dir_handled(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text('name: test\nmodel-paths: ["models"]\n')
        models = tmp_path / "models"
        models.mkdir()
        (models / "test.yml").write_text("models:\n  - name: test\n")

        mock_cfg = MagicMock()
        mock_cfg.get = MagicMock(return_value=None)

        with patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_config",
            return_value=mock_cfg,
        ), patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_dbt_model_paths",
            return_value=[models],
        ):
            m = SSTManifest(tmp_path)
            m.build()
            assert len(m.files) >= 1


# ---------------------------------------------------------------------------
# SSTManifest.save / load roundtrip
# ---------------------------------------------------------------------------


class TestSSTManifestSaveLoad:
    def test_roundtrip(self, project_dir):
        with patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_config",
            return_value=_mock_config(project_dir),
        ), patch(
            "snowflake_semantic_tools.core.parsing.sst_manifest.get_dbt_model_paths",
            return_value=[project_dir / "models"],
        ):
            m = SSTManifest(project_dir)
            m.build()
            out_dir = project_dir / "target"
            m.save(out_dir)

            loaded = SSTManifest.load(out_dir)
            assert loaded is not None
            assert loaded.files == m.files
            assert loaded.config_checksum == m.config_checksum
            assert loaded.metadata["sst_version"] == m.metadata["sst_version"]

    def test_load_missing_returns_none(self, tmp_path):
        assert SSTManifest.load(tmp_path / "nonexistent") is None

    def test_load_corrupted_returns_none(self, tmp_path):
        bad_file = tmp_path / MANIFEST_FILENAME
        bad_file.write_text("not json{{{")
        assert SSTManifest.load(tmp_path) is None


# ---------------------------------------------------------------------------
# SSTManifest.compare_to
# ---------------------------------------------------------------------------


class TestSSTManifestCompare:
    def _make_manifest(self, files, config_checksum=None):
        m = SSTManifest()
        m.files = files
        m.config_checksum = config_checksum
        return m

    def test_detects_added_files(self):
        current = self._make_manifest({"a.yml": {"checksum": "sha256:aaa"}})
        baseline = self._make_manifest({})
        diff = current.compare_to(baseline)
        assert diff.added == ["a.yml"]
        assert diff.total_changes == 1

    def test_detects_modified_files(self):
        current = self._make_manifest({"a.yml": {"checksum": "sha256:new"}})
        baseline = self._make_manifest({"a.yml": {"checksum": "sha256:old"}})
        diff = current.compare_to(baseline)
        assert diff.modified == ["a.yml"]

    def test_detects_removed_files(self):
        current = self._make_manifest({})
        baseline = self._make_manifest({"a.yml": {"checksum": "sha256:aaa"}})
        diff = current.compare_to(baseline)
        assert diff.removed == ["a.yml"]

    def test_unchanged_files(self):
        files = {"a.yml": {"checksum": "sha256:same"}}
        current = self._make_manifest(files)
        baseline = self._make_manifest(files.copy())
        diff = current.compare_to(baseline)
        assert diff.unchanged == ["a.yml"]
        assert diff.total_changes == 0

    def test_config_change_detected(self):
        current = self._make_manifest({}, config_checksum="sha256:new")
        baseline = self._make_manifest({}, config_checksum="sha256:old")
        diff = current.compare_to(baseline)
        assert diff.config_changed is True

    def test_config_no_change(self):
        cs = "sha256:same"
        current = self._make_manifest({}, config_checksum=cs)
        baseline = self._make_manifest({}, config_checksum=cs)
        diff = current.compare_to(baseline)
        assert diff.config_changed is False


# ---------------------------------------------------------------------------
# SSTManifest.get_file_view_map
# ---------------------------------------------------------------------------


class TestGetFileViewMap:
    def test_returns_views_impacted(self):
        m = SSTManifest()
        m.files = {
            "a.yml": {"checksum": "x", "type": "dbt", "views_impacted": ["v1"]},
            "b.yml": {"checksum": "x", "type": "metrics", "views_impacted": None},
        }
        fvm = m.get_file_view_map()
        assert fvm["a.yml"] == ["v1"]
        assert fvm["b.yml"] is None
