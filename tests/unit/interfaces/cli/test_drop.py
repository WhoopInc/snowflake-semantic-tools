"""
Unit tests for sst drop command.
"""

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
    """Test the prune logic."""

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

    def test_no_orphans_reports_clean(self):
        client = self._mock_client(
            actual_views=["VIEW_A", "VIEW_B"],
            tracked_views=["VIEW_A", "VIEW_B"],
        )
        output = CLIOutput(verbose=False)

        _prune_orphaned_views(client, "DB", "SCH", dry_run=False, yes=True, verbose=False, output=output)

        all_calls = [str(c) for c in client.execute_query.call_args_list]
        drop_calls = [c for c in all_calls if "DROP" in c]
        assert len(drop_calls) == 0

    def test_orphans_detected_in_dry_run(self):
        client = self._mock_client(
            actual_views=["VIEW_A", "VIEW_B", "ORPHAN_1"],
            tracked_views=["VIEW_A", "VIEW_B"],
        )
        output = CLIOutput(verbose=False)

        _prune_orphaned_views(client, "DB", "SCH", dry_run=True, yes=False, verbose=False, output=output)

        all_calls = [str(c) for c in client.execute_query.call_args_list]
        drop_calls = [c for c in all_calls if "DROP" in c]
        assert len(drop_calls) == 0

    def test_orphans_dropped_with_yes(self):
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
