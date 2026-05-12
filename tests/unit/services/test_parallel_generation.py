"""
Unit tests for parallel semantic view generation (--threads).

Tests the ConnectionPool, parallel execution, error isolation,
timeout handling, retry logic, and config resolution.
"""

import queue
import threading
import time
from concurrent.futures import TimeoutError
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.services.connection_pool import ConnectionPool
from snowflake_semantic_tools.services.generate_semantic_views import (
    GenerateConfig,
    SemanticViewGenerationService,
    UnifiedGenerationConfig,
)


class TestConnectionPool:
    """Tests for ConnectionPool lifecycle."""

    def test_pool_size(self):
        config = MagicMock()
        pool = ConnectionPool(config=config, size=4)
        assert pool.size == 4

    @patch("snowflake_semantic_tools.services.connection_pool.ConnectionManager")
    def test_warmup_success(self, MockCM):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cm_instance = MagicMock()
        mock_cm_instance.get_connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_cm_instance.get_connection.return_value.__exit__ = MagicMock(return_value=False)
        MockCM.return_value = mock_cm_instance

        config = MagicMock()
        pool = ConnectionPool(config=config, size=2)
        pool.warmup()
        assert pool._pool.qsize() == 2

    @patch("snowflake_semantic_tools.services.connection_pool.ConnectionManager")
    def test_warmup_failure_raises(self, MockCM):
        mock_cm_instance = MagicMock()
        mock_cm_instance.get_connection.return_value.__enter__ = MagicMock(side_effect=Exception("Auth failed"))
        mock_cm_instance.get_connection.return_value.__exit__ = MagicMock(return_value=False)
        MockCM.return_value = mock_cm_instance

        config = MagicMock()
        pool = ConnectionPool(config=config, size=2)
        with pytest.raises(RuntimeError, match="Failed to establish"):
            pool.warmup()

    def test_checkout_checkin(self):
        config = MagicMock()
        pool = ConnectionPool(config=config, size=2)
        cm1 = MagicMock()
        cm2 = MagicMock()
        pool._pool.put(cm1)
        pool._pool.put(cm2)
        pool._all_managers = [cm1, cm2]

        checked_out = pool.checkout(timeout=1.0)
        assert checked_out is cm1
        pool.checkin(checked_out)
        assert pool._pool.qsize() == 2

    def test_close_all(self):
        config = MagicMock()
        pool = ConnectionPool(config=config, size=2)
        cm1 = MagicMock()
        cm2 = MagicMock()
        pool._all_managers = [cm1, cm2]
        pool._pool.put(cm1)
        pool._pool.put(cm2)

        pool.close_all()
        cm1.close.assert_called_once()
        cm2.close.assert_called_once()
        assert pool._closed is True


class TestParallelExecution:
    """Tests for parallel generation in SemanticViewGenerationService."""

    def _make_service(self):
        """Create a service with mocked Snowflake config."""
        config = MagicMock()
        config.account = "test"
        config.user = "test_user"
        config.role = "test_role"
        config.connection_params = {}
        with patch("snowflake_semantic_tools.services.generate_semantic_views.ConnectionManager") as MockCM:
            mock_cm = MagicMock()
            MockCM.return_value = mock_cm
            service = SemanticViewGenerationService(config)
        return service

    def test_threads_1_uses_sequential_path(self):
        """threads=1 should NOT call _execute_parallel."""
        service = self._make_service()
        service._execute_parallel = MagicMock()
        service._query_available_views = MagicMock(return_value=[])

        gen_config = GenerateConfig(
            views_to_generate=[],
            target_database="DB",
            target_schema="SCH",
            metadata_database="DB",
            metadata_schema="SCH",
            threads=1,
        )
        service.execute(gen_config)
        service._execute_parallel.assert_not_called()

    def test_threads_gt1_uses_parallel_path(self):
        """threads>1 should call _execute_parallel."""
        service = self._make_service()

        views = [
            {"name": "view_a", "tables": ["T1"], "description": "", "custom_instructions": []},
            {"name": "view_b", "tables": ["T2"], "description": "", "custom_instructions": []},
        ]
        service._query_available_views = MagicMock(return_value=views)
        service._execute_parallel = MagicMock(
            return_value=MagicMock(success=True, views_generated=["view_a", "view_b"], views_failed=[], errors=[])
        )

        gen_config = GenerateConfig(
            views_to_generate=views,
            target_database="DB",
            target_schema="SCH",
            metadata_database="DB",
            metadata_schema="SCH",
            threads=4,
        )
        result = service.execute(gen_config)
        service._execute_parallel.assert_called_once()

    @patch("snowflake_semantic_tools.services.generate_semantic_views.ConnectionPool")
    @patch("snowflake_semantic_tools.services.generate_semantic_views.SemanticViewBuilder")
    def test_parallel_generates_all_views(self, MockBuilder, MockPool):
        """Parallel execution should generate all views successfully."""
        mock_pool = MagicMock()
        mock_pool.warmup = MagicMock()
        mock_cm = MagicMock()
        mock_pool.checkout.return_value = mock_cm
        MockPool.return_value = mock_pool

        mock_builder = MagicMock()
        mock_builder.build_semantic_view.return_value = {
            "success": True,
            "sql_statement": "CREATE SEMANTIC VIEW ...",
            "message": "ok",
        }
        MockBuilder.return_value = mock_builder

        service = self._make_service()

        views = [
            {"name": f"view_{i}", "tables": [f"T{i}"], "description": "", "custom_instructions": []} for i in range(8)
        ]

        gen_config = GenerateConfig(
            views_to_generate=views,
            target_database="DB",
            target_schema="SCH",
            metadata_database="DB",
            metadata_schema="SCH",
            threads=4,
            view_timeout=30,
        )

        from snowflake_semantic_tools.shared.progress import NoOpProgressCallback

        result = service._execute_parallel(views, gen_config, NoOpProgressCallback(), time.time())
        assert result.success
        assert len(result.views_generated) == 8
        assert len(result.views_failed) == 0

    @patch("snowflake_semantic_tools.services.generate_semantic_views.ConnectionPool")
    @patch("snowflake_semantic_tools.services.generate_semantic_views.SemanticViewBuilder")
    def test_one_failure_doesnt_crash_others(self, MockBuilder, MockPool):
        """A single view failure should not prevent other views from succeeding."""
        mock_pool = MagicMock()
        mock_pool.warmup = MagicMock()
        mock_cm = MagicMock()
        mock_pool.checkout.return_value = mock_cm
        MockPool.return_value = mock_pool

        call_count = {"n": 0}

        def side_effect(**kwargs):
            call_count["n"] += 1
            if kwargs.get("view_name") == "view_bad":
                return {"success": False, "sql_statement": "", "message": "Snowflake error"}
            return {"success": True, "sql_statement": "CREATE ...", "message": "ok"}

        mock_builder = MagicMock()
        mock_builder.build_semantic_view.side_effect = side_effect
        MockBuilder.return_value = mock_builder

        service = self._make_service()

        views = [
            {"name": "view_good_1", "tables": ["T1"], "description": "", "custom_instructions": []},
            {"name": "view_bad", "tables": ["T2"], "description": "", "custom_instructions": []},
            {"name": "view_good_2", "tables": ["T3"], "description": "", "custom_instructions": []},
        ]

        gen_config = GenerateConfig(
            views_to_generate=views,
            target_database="DB",
            target_schema="SCH",
            metadata_database="DB",
            metadata_schema="SCH",
            threads=2,
            view_timeout=30,
        )

        from snowflake_semantic_tools.shared.progress import NoOpProgressCallback

        result = service._execute_parallel(views, gen_config, NoOpProgressCallback(), time.time())
        assert not result.success
        assert len(result.views_generated) == 2
        assert "view_bad" in result.views_failed


class TestIsTransientError:
    """Tests for transient error detection."""

    @pytest.fixture
    def service(self):
        config = MagicMock()
        config.connection_params = {}
        with patch("snowflake_semantic_tools.services.generate_semantic_views.ConnectionManager"):
            return SemanticViewGenerationService(config)

    def test_250001_is_transient(self, service):
        assert service._is_transient_error(Exception("250001: connection reset"))

    def test_503_is_transient(self, service):
        assert service._is_transient_error(Exception("HTTP 503 Service Unavailable"))

    def test_normal_error_is_not_transient(self, service):
        assert not service._is_transient_error(Exception("Object does not exist"))


class TestConfigResolution:
    """Tests for threads config resolution."""

    def test_unified_config_defaults(self):
        config = UnifiedGenerationConfig(
            metadata_database="DB",
            metadata_schema="SCH",
            target_database="DB",
            target_schema="SCH",
        )
        assert config.threads == 1
        assert config.view_timeout == 300

    def test_unified_config_custom_threads(self):
        config = UnifiedGenerationConfig(
            metadata_database="DB",
            metadata_schema="SCH",
            target_database="DB",
            target_schema="SCH",
            threads=8,
            view_timeout=60,
        )
        assert config.threads == 8
        assert config.view_timeout == 60

    def test_generate_config_defaults(self):
        config = GenerateConfig()
        assert config.threads == 1
        assert config.view_timeout == 300

    def test_config_file_threads_default(self):
        from snowflake_semantic_tools.shared.config import Config

        config = Config.__new__(Config)
        config._config = config._get_defaults()
        assert config.get("generation.threads") == 1
        assert config.get("generation.view_timeout") == 300
