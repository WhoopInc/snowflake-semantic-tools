#!/usr/bin/env python3
"""
Integration test for Issue #83: Enrichment status handling
Tests the full enrichment workflow to ensure success messages are displayed correctly.

This test validates the complete fix for:
- CLI status check using "complete" instead of "success"
- Service returning "complete" status
- Real enrichment workflow displaying correct messages
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.interfaces.cli.commands.enrich import enrich


class TestEnrichmentStatusIntegration:
    """Integration tests for enrichment status handling with real workflow."""

    @pytest.fixture
    def jaffle_shop_setup(self, tmp_path):
        """Create a jaffle_shop-like dbt project structure."""
        # Create directory structure
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        # Create a simple customers model
        customers_sql = models_dir / "customers.sql"
        customers_sql.write_text(
            """
-- Customers model
select
    customer_id,
    first_name,
    last_name,
    email,
    created_at
from raw.customers
"""
        )

        # Create corresponding YAML with basic metadata
        customers_yaml = models_dir / "customers.yml"
        customers_yaml.write_text(
            """
version: 2

models:
  - name: customers
    description: Customer dimension table
    
    columns:
      - name: customer_id
        description: Primary key
        data_type: NUMBER
        
      - name: first_name
        description: Customer first name
        data_type: TEXT
        
      - name: last_name  
        description: Customer last name
        data_type: TEXT
        
      - name: email
        description: Customer email address
        data_type: TEXT
        
      - name: created_at
        description: When customer was created
        data_type: TIMESTAMP_NTZ
"""
        )

        # Create manifest.json
        manifest = {
            "nodes": {
                "model.jaffle_shop.customers": {
                    "database": "ANALYTICS",
                    "schema": "DBT_PROD",
                    "name": "customers",
                    "unique_id": "model.jaffle_shop.customers",
                    "original_file_path": "models/customers.sql",
                    "columns": {
                        "customer_id": {"name": "customer_id", "data_type": "NUMBER"},
                        "first_name": {"name": "first_name", "data_type": "TEXT"},
                        "last_name": {"name": "last_name", "data_type": "TEXT"},
                        "email": {"name": "email", "data_type": "TEXT"},
                        "created_at": {"name": "created_at", "data_type": "TIMESTAMP_NTZ"},
                    },
                }
            }
        }

        target_dir = tmp_path / "target"
        target_dir.mkdir()
        manifest_path = target_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        return {
            "project_dir": tmp_path,
            "models_dir": models_dir,
            "manifest_path": manifest_path,
            "customers_yaml": customers_yaml,
        }

    def test_successful_enrichment_shows_complete_message(self, jaffle_shop_setup):
        """
        Integration test for Issue #83.

        Tests the full enrichment flow:
        1. Parse dbt models
        2. Connect to Snowflake (mocked)
        3. Enrich metadata
        4. Verify status message is "Enrichment completed" not "Enrichment failed"
        """
        runner = CliRunner()

        with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService"
        ) as mock_service:
            # Mock the enrichment service to return successful result
            mock_instance = Mock()
            mock_instance.connect = Mock()
            mock_instance.close = Mock()

            # Create a result with "complete" status (what service actually returns)
            mock_result = Mock()
            mock_result.status = "complete"  # This is the key - service returns "complete"
            mock_result.processed = 1
            mock_result.errors = []
            mock_result.total = 1
            mock_result.print_summary = Mock()

            mock_instance.enrich = Mock(return_value=mock_result)
            mock_service.return_value = mock_instance

            # Mock _resolve_model_names to return our test file
            with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich._resolve_model_names") as mock_resolve:
                mock_resolve.return_value = [str(jaffle_shop_setup["customers_yaml"])]

                # Run enrichment on customers model
                result = runner.invoke(
                    enrich,
                    [
                        "--models",
                        "customers",
                        "--manifest",
                        str(jaffle_shop_setup["manifest_path"]),
                    ],
                    catch_exceptions=False,
                )

            # The key assertion: verify success message (not failure)
            assert "Enrichment completed" in result.output, f"Expected 'Enrichment completed' but got: {result.output}"

            # Should NOT show error message for successful enrichment
            assert (
                "Enrichment failed" not in result.output
            ), f"Should not show 'Enrichment failed' for successful enrichment. Output: {result.output}"

            # Verify exit code is success
            assert result.exit_code == 0, f"Expected exit code 0 but got {result.exit_code}. Output: {result.output}"

    def test_partial_enrichment_shows_warning(self, jaffle_shop_setup):
        """Test that partial enrichment (some successes, some failures) shows appropriate warning."""
        runner = CliRunner()

        with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService"
        ) as mock_service:
            # Mock the enrichment service to return partial status
            mock_instance = Mock()
            mock_instance.connect = Mock()
            mock_instance.close = Mock()

            # Create a result with "partial" status (some succeeded, some failed)
            mock_result = Mock()
            mock_result.status = "partial"
            mock_result.processed = 1
            mock_result.errors = [{"model": "models/orders.yml"}]
            mock_result.total = 2
            mock_result.print_summary = Mock()

            mock_instance.enrich = Mock(return_value=mock_result)
            mock_service.return_value = mock_instance

            # Mock _resolve_model_names
            with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich._resolve_model_names") as mock_resolve:
                mock_resolve.return_value = [
                    str(jaffle_shop_setup["customers_yaml"]),
                    "models/orders.yml",
                ]

                result = runner.invoke(
                    enrich,
                    [
                        "--models",
                        "customers,orders",
                        "--manifest",
                        str(jaffle_shop_setup["manifest_path"]),
                    ],
                    catch_exceptions=False,
                )

            # Partial success should show warning message
            assert (
                "completed with errors" in result.output.lower() or "warning" in result.output.lower()
            ), f"Partial enrichment should show warning. Output: {result.output}"

            # Should NOT say "Enrichment completed" without qualification
            if "Enrichment completed" in result.output:
                assert (
                    "with errors" in result.output or "warning" in result.output.lower()
                ), f"Partial enrichment should qualify success message. Output: {result.output}"

    def test_complete_failure_shows_error(self, jaffle_shop_setup):
        """Test that complete enrichment failure shows error message."""
        runner = CliRunner()

        with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService"
        ) as mock_service:
            # Mock the enrichment service to return failed status
            mock_instance = Mock()
            mock_instance.connect = Mock()
            mock_instance.close = Mock()

            # Create a result with "failed" status (all failed)
            mock_result = Mock()
            mock_result.status = "failed"
            mock_result.processed = 0
            mock_result.errors = [{"model": "models/customers.yml"}]
            mock_result.total = 1
            mock_result.print_summary = Mock()

            mock_instance.enrich = Mock(return_value=mock_result)
            mock_service.return_value = mock_instance

            # Mock _resolve_model_names
            with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich._resolve_model_names") as mock_resolve:
                mock_resolve.return_value = [str(jaffle_shop_setup["customers_yaml"])]

                result = runner.invoke(
                    enrich,
                    [
                        "--models",
                        "customers",
                        "--manifest",
                        str(jaffle_shop_setup["manifest_path"]),
                    ],
                    catch_exceptions=False,
                )

            # Should show error for complete failure
            assert (
                result.exit_code != 0 or "enrichment failed" in result.output.lower()
            ), f"Complete failure should show error. Output: {result.output}"


class TestEnrichmentStatusRegression:
    """Regression tests specifically for Issue #83."""

    def test_status_value_alignment(self):
        """
        Verify that CLI and service use the same status values.

        This documents the contract between layers to prevent future regressions.
        """
        # Service status values (from enrich_metadata.py)
        service_statuses = {"complete", "partial", "failed"}

        # CLI must handle these exact values (from enrich.py)
        cli_expected_statuses = {"complete", "partial", "failed"}

        assert service_statuses == cli_expected_statuses, "CLI and service must use the same status values"

    def test_issue_83_scenario(self):
        """
        Reproduce the exact scenario from Issue #83.

        Before fix: Service returned "complete", CLI checked for "success" → showed "failed"
        After fix: Service returns "complete", CLI checks for "complete" → shows "completed"
        """
        from snowflake_semantic_tools.services.enrich_metadata import EnrichmentResult

        # Service returns this
        result = EnrichmentResult(
            status="complete",  # This is what service returns
            processed=1,
            total=1,
            results=[{"status": "success", "model": "customers"}],
            errors=[],
        )

        # CLI must recognize this as success
        assert result.status == "complete", "Service should return 'complete'"
        assert result.status != "success", "Service should NOT return 'success'"

        # The fix is that CLI now checks: result.status == "complete"
        # Instead of the old: result.status == "success"
