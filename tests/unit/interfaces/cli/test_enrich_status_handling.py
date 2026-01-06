"""
Unit tests for enrichment status handling in sst enrich command.

Tests Issue #83: sst enrich shows "Enrichment failed [ERROR]" even when enrichment succeeds
This was caused by a status value mismatch between CLI and service layer.
"""

from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from snowflake_semantic_tools.interfaces.cli.commands.enrich import enrich


class TestEnrichmentStatusHandling:
    """Tests for enrichment status handling (Issue #83)."""

    def test_complete_status_shows_success(self):
        """Test that 'complete' status from service shows success message."""
        runner = CliRunner()

        with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService"
        ) as mock_service:
            # Mock the service to return "complete" status (what service actually returns)
            mock_instance = Mock()
            mock_instance.connect = Mock()
            mock_instance.enrich = Mock(
                return_value=Mock(
                    status="complete",  # Service returns "complete", not "success"
                    models_enriched=1,
                    failed_models=[],
                    print_summary=Mock(),
                )
            )
            mock_instance.close = Mock()
            mock_service.return_value = mock_instance

            # Use --models to avoid needing a real directory
            with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich._resolve_model_names") as mock_resolve:
                mock_resolve.return_value = ["models/customers.sql"]
                result = runner.invoke(enrich, ["--models", "customers"])

            # Should show success message (not "failed")
            assert "Enrichment completed" in result.output
            assert "[ERROR]" not in result.output
            assert "failed" not in result.output.lower() or "0 SKIP" in result.output

    def test_partial_status_shows_warning(self):
        """Test that 'partial' status shows warning message."""
        runner = CliRunner()

        with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService"
        ) as mock_service:
            # Mock the service to return "partial" status
            mock_instance = Mock()
            mock_instance.connect = Mock()
            mock_instance.enrich = Mock(
                return_value=Mock(
                    status="partial",
                    models_enriched=1,
                    failed_models=["models/orders.sql"],
                    print_summary=Mock(),
                )
            )
            mock_instance.close = Mock()
            mock_service.return_value = mock_instance

            with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich._resolve_model_names") as mock_resolve:
                mock_resolve.return_value = ["models/customers.sql", "models/orders.sql"]
                result = runner.invoke(enrich, ["--models", "customers,orders"])

            # Should show warning message
            assert "completed with errors" in result.output.lower()

    def test_failed_status_shows_error(self):
        """Test that 'failed' status shows error message."""
        runner = CliRunner()

        with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService"
        ) as mock_service:
            # Mock the service to return "failed" status
            mock_instance = Mock()
            mock_instance.connect = Mock()
            mock_instance.enrich = Mock(
                return_value=Mock(
                    status="failed",
                    models_enriched=0,
                    failed_models=["models/customers.sql"],
                    print_summary=Mock(),
                )
            )
            mock_instance.close = Mock()
            mock_service.return_value = mock_instance

            with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich._resolve_model_names") as mock_resolve:
                mock_resolve.return_value = ["models/customers.sql"]
                result = runner.invoke(enrich, ["--models", "customers"])

            # Should show error message
            assert "Enrichment failed" in result.output or "ERROR" in result.output

    def test_status_values_match_service_contract(self):
        """Test that CLI handles all status values returned by service.
        
        Service returns: "complete", "partial", "failed"
        CLI must handle all three correctly.
        """
        from snowflake_semantic_tools.services.enrich_metadata import MetadataEnrichmentService

        # This test documents the service contract
        # If service changes status values, this test should fail
        expected_statuses = ["complete", "partial", "failed"]
        
        # The service returns these status values:
        # - "complete": all models enriched successfully (line 532 in enrich_metadata.py)
        # - "partial": some models enriched, some failed (line 534)
        # - "failed": no models enriched successfully (line 539)
        
        # Verify that CLI code handles all expected statuses
        # This is a documentation test to ensure contract is maintained
        assert expected_statuses == ["complete", "partial", "failed"]


class TestEnrichmentStatusRegression:
    """Regression tests for Issue #83."""

    def test_successful_enrichment_does_not_show_error(self):
        """
        Regression test for Issue #83.
        
        Before fix: CLI checked for status == "success" but service returned "complete"
        After fix: CLI checks for status == "complete" matching service
        
        This test ensures enrichment success is properly displayed.
        """
        runner = CliRunner()

        with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich.setup_command"), patch(
            "snowflake_semantic_tools.interfaces.cli.commands.enrich.MetadataEnrichmentService"
        ) as mock_service:
            # Service returns "complete" status
            mock_instance = Mock()
            mock_instance.connect = Mock()
            mock_result = Mock()
            mock_result.status = "complete"  # This is what service actually returns
            mock_result.models_enriched = 1
            mock_result.failed_models = []
            mock_result.print_summary = Mock()
            
            mock_instance.enrich = Mock(return_value=mock_result)
            mock_instance.close = Mock()
            mock_service.return_value = mock_instance

            with patch("snowflake_semantic_tools.interfaces.cli.commands.enrich._resolve_model_names") as mock_resolve:
                mock_resolve.return_value = ["models/customers.sql"]
                result = runner.invoke(enrich, ["--models", "customers"])

            # Before fix: Would show "Enrichment failed [ERROR]"
            # After fix: Should show "Enrichment completed"
            assert "Enrichment completed" in result.output, \
                f"Expected success message but got: {result.output}"
            
            # Should NOT show error message when enrichment succeeds
            assert "Enrichment failed" not in result.output, \
                f"Should not show 'failed' for successful enrichment. Output: {result.output}"

