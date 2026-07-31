"""
Unit tests for sst drop command.
"""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from snowflake_semantic_tools.interfaces.cli.commands.drop import _drop_specific_view, _prune_orphaned_views
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput


class TestDropSpecificView:
    """Test dropping a single named view."""

    def test_dry_run_does_not_execute(self):
        client = MagicMock()
        output = CLIOutput(verbose=False)
        _drop_specific_view(client, "DB", "SCHEMA", "MY_VIEW", dry_run=True, output=output)
        client.execute_query.assert_not_called()

    def test_executes_drop_statement(self):
        client = MagicMock()
        output = CLIOutput(verbose=False)

        _drop_specific_view(client, "DB", "SCHEMA", "old_view", dry_run=False, output=output)

        client.execute_query.assert_called_once_with("DROP SEMANTIC VIEW IF EXISTS DB.SCHEMA.OLD_VIEW")

    def test_view_name_uppercased(self):
        client = MagicMock()
        output = CLIOutput(verbose=False)

        _drop_specific_view(client, "MY_DB", "MY_SCHEMA", "lowercase_view", dry_run=False, output=output)

        client.execute_query.assert_called_once_with("DROP SEMANTIC VIEW IF EXISTS MY_DB.MY_SCHEMA.LOWERCASE_VIEW")


class TestPruneOrphanedViews:
    """Test the prune logic (legacy fallback when no manifest exists)."""

    def _mock_client(self, actual_views, tracked_views):
        client = MagicMock()

        def side_effect(sql):
            if "SHOW SEMANTIC VIEWS" in sql:
                return pd.DataFrame({"name": actual_views}) if actual_views else pd.DataFrame()
            elif "SM_SEMANTIC_VIEWS" in sql:
                return pd.DataFrame({"NAME": [v.upper() for v in tracked_views]}) if tracked_views else pd.DataFrame()
            return pd.DataFrame()

        client.execute_query.side_effect = side_effect
        return client

    def test_no_orphans_reports_clean(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        client = self._mock_client(
            actual_views=["VIEW_A", "VIEW_B"],
            tracked_views=["VIEW_A", "VIEW_B"],
        )
        output = CLIOutput(verbose=False)

        _prune_orphaned_views(client, "DB", "SCH", dry_run=False, yes=True, verbose=False, output=output)

        all_calls = [str(c) for c in client.execute_query.call_args_list]
        drop_calls = [c for c in all_calls if "DROP" in c]
        assert len(drop_calls) == 0

    def test_orphans_detected_in_dry_run(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        client = self._mock_client(
            actual_views=["VIEW_A", "VIEW_B", "ORPHAN_1"],
            tracked_views=["VIEW_A", "VIEW_B"],
        )
        output = CLIOutput(verbose=False)

        _prune_orphaned_views(client, "DB", "SCH", dry_run=True, yes=False, verbose=False, output=output)

        all_calls = [str(c) for c in client.execute_query.call_args_list]
        drop_calls = [c for c in all_calls if "DROP" in c]
        assert len(drop_calls) == 0

    def test_orphans_dropped_with_yes(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        client = MagicMock()

        def side_effect(sql):
            if "SHOW SEMANTIC VIEWS" in sql:
                return pd.DataFrame({"name": ["VIEW_A", "ORPHAN_1", "ORPHAN_2"]})
            elif "SM_SEMANTIC_VIEWS" in sql:
                return pd.DataFrame({"NAME": ["VIEW_A"]})
            return pd.DataFrame()

        client.execute_query.side_effect = side_effect
        output = CLIOutput(verbose=False)

        _prune_orphaned_views(client, "DB", "SCH", dry_run=False, yes=True, verbose=False, output=output)

        all_calls = [str(c) for c in client.execute_query.call_args_list]
        drop_calls = [c for c in all_calls if "DROP SEMANTIC VIEW" in c]
        assert len(drop_calls) == 2


class TestPruneWithManifest:
    """Test prune reads from compiled manifest when available."""

    def _mock_client(self, actual_views):
        """Client that only handles SHOW SEMANTIC VIEWS (no SM_ table needed)."""
        client = MagicMock()

        def side_effect(sql):
            if "SHOW SEMANTIC VIEWS" in sql:
                return pd.DataFrame({"name": actual_views}) if actual_views else pd.DataFrame()
            if "SM_SEMANTIC_VIEWS" in sql:
                raise AssertionError("Should not query SM_SEMANTIC_VIEWS when manifest exists")
            return pd.DataFrame()

        client.execute_query.side_effect = side_effect
        return client

    def _write_manifest(self, tmp_path, view_names):
        target = tmp_path / "target"
        target.mkdir(exist_ok=True)
        manifest = {
            "metadata": {},
            "file_checksums": {},
            "tables": {
                "semantic_views": [{"name": name} for name in view_names],
            },
        }
        (target / "sst_manifest.json").write_text(json.dumps(manifest))

    def test_manifest_present_no_orphans(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_manifest(tmp_path, ["VIEW_A", "VIEW_B"])
        client = self._mock_client(actual_views=["VIEW_A", "VIEW_B"])
        output = CLIOutput(verbose=False)

        _prune_orphaned_views(client, "DB", "SCH", dry_run=False, yes=True, verbose=False, output=output)

        all_calls = [str(c) for c in client.execute_query.call_args_list]
        drop_calls = [c for c in all_calls if "DROP" in c]
        assert len(drop_calls) == 0

    def test_manifest_identifies_orphans(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_manifest(tmp_path, ["VIEW_A", "VIEW_B"])
        client = self._mock_client(actual_views=["VIEW_A", "VIEW_B", "ORPHAN_1"])
        output = CLIOutput(verbose=False)

        _prune_orphaned_views(client, "DB", "SCH", dry_run=False, yes=True, verbose=False, output=output)

        all_calls = [str(c) for c in client.execute_query.call_args_list]
        drop_calls = [c for c in all_calls if "DROP SEMANTIC VIEW" in c]
        assert len(drop_calls) == 1
        assert "ORPHAN_1" in drop_calls[0]

    def test_manifest_protects_new_views(self, tmp_path, monkeypatch):
        """The core bug: payments_invoices is in manifest but not SM_ table."""
        monkeypatch.chdir(tmp_path)
        self._write_manifest(tmp_path, ["VIEW_A", "PAYMENTS_INVOICES"])
        client = self._mock_client(actual_views=["VIEW_A", "PAYMENTS_INVOICES"])
        output = CLIOutput(verbose=False)

        _prune_orphaned_views(client, "DB", "SCH", dry_run=False, yes=True, verbose=False, output=output)

        all_calls = [str(c) for c in client.execute_query.call_args_list]
        drop_calls = [c for c in all_calls if "DROP" in c]
        assert len(drop_calls) == 0

    def test_manifest_case_insensitive(self, tmp_path, monkeypatch):
        """Manifest names are matched case-insensitively."""
        monkeypatch.chdir(tmp_path)
        self._write_manifest(tmp_path, ["view_a", "payments_invoices"])
        client = self._mock_client(actual_views=["VIEW_A", "PAYMENTS_INVOICES"])
        output = CLIOutput(verbose=False)

        _prune_orphaned_views(client, "DB", "SCH", dry_run=False, yes=True, verbose=False, output=output)

        all_calls = [str(c) for c in client.execute_query.call_args_list]
        drop_calls = [c for c in all_calls if "DROP" in c]
        assert len(drop_calls) == 0

    def test_no_manifest_falls_back_to_sm_table(self, tmp_path, monkeypatch):
        """When no manifest exists, uses legacy SM_SEMANTIC_VIEWS."""
        monkeypatch.chdir(tmp_path)
        client = MagicMock()

        def side_effect(sql):
            if "SHOW SEMANTIC VIEWS" in sql:
                return pd.DataFrame({"name": ["VIEW_A", "ORPHAN_1"]})
            elif "SM_SEMANTIC_VIEWS" in sql:
                return pd.DataFrame({"NAME": ["VIEW_A"]})
            return pd.DataFrame()

        client.execute_query.side_effect = side_effect
        output = CLIOutput(verbose=False)

        _prune_orphaned_views(client, "DB", "SCH", dry_run=False, yes=True, verbose=False, output=output)

        all_calls = [str(c) for c in client.execute_query.call_args_list]
        drop_calls = [c for c in all_calls if "DROP SEMANTIC VIEW" in c]
        assert len(drop_calls) == 1
