"""
Tests for Progress Callback Framework

Tests the progress reporting system used by services to communicate
with CLI without tight coupling.
"""

import pytest
from unittest.mock import Mock, MagicMock
from io import StringIO

from snowflake_semantic_tools.shared.progress import (
    NoOpProgressCallback,
    CLIProgressCallback,
    default_progress_callback
)
from snowflake_semantic_tools.interfaces.cli.output import CLIOutput


class TestNoOpProgressCallback:
    """Test the no-op progress callback (default for services)."""
    
    def test_no_op_callback_all_methods_exist(self):
        """Test all required methods exist and are no-ops."""
        callback = NoOpProgressCallback()
        
        # All methods should exist and not raise errors
        callback.info("test")
        callback.info("test", indent=1)
        callback.stage("test stage")
        callback.stage("test stage", current=1, total=3)
        callback.detail("test detail")
        callback.item_progress(1, 10, "test_item", "RUN")
        callback.item_progress(1, 10, "test_item", "OK", duration=2.5)
        callback.blank_line()
        callback.warning("test warning")
        callback.error("test error")
    
    def test_default_progress_callback_returns_noop(self):
        """Test default factory returns NoOpProgressCallback."""
        callback = default_progress_callback()
        assert isinstance(callback, NoOpProgressCallback)


class TestCLIProgressCallback:
    """Test the CLI progress callback implementation."""
    
    def test_cli_callback_initialization(self):
        """Test CLI callback initializes with CLIOutput."""
        output = CLIOutput(verbose=False, quiet=False, use_colors=False)
        callback = CLIProgressCallback(output)
        
        assert callback.output is output
    
    def test_info_calls_output_info(self):
        """Test info() delegates to CLIOutput.info()."""
        output = Mock(spec=CLIOutput)
        callback = CLIProgressCallback(output)
        
        callback.info("Test message")
        output.info.assert_called_once_with("Test message", indent=0)
    
    def test_info_with_indent(self):
        """Test info() respects indent parameter."""
        output = Mock(spec=CLIOutput)
        callback = CLIProgressCallback(output)
        
        callback.info("Test message", indent=2)
        output.info.assert_called_once_with("Test message", indent=2)
    
    def test_stage_without_numbering(self):
        """Test stage() without current/total."""
        output = Mock(spec=CLIOutput)
        callback = CLIProgressCallback(output)
        
        callback.stage("Processing data")
        
        # Should call blank_line and info
        output.blank_line.assert_called_once()
        output.info.assert_called_once_with("Processing data")
    
    def test_stage_with_numbering(self):
        """Test stage() with current/total numbering."""
        output = Mock(spec=CLIOutput)
        callback = CLIProgressCallback(output)
        
        callback.stage("Processing data", current=2, total=5)
        
        # Should call blank_line and info with step numbering
        output.blank_line.assert_called_once()
        output.info.assert_called_once_with("Step 2/5: Processing data")
    
    def test_detail_calls_debug(self):
        """Test detail() delegates to CLIOutput.debug()."""
        output = Mock(spec=CLIOutput)
        callback = CLIProgressCallback(output)
        
        callback.detail("Detailed info")
        output.debug.assert_called_once_with("Detailed info", indent=1)
    
    def test_detail_with_custom_indent(self):
        """Test detail() respects custom indent."""
        output = Mock(spec=CLIOutput)
        callback = CLIProgressCallback(output)
        
        callback.detail("Detailed info", indent=3)
        output.debug.assert_called_once_with("Detailed info", indent=3)
    
    def test_item_progress_without_duration(self):
        """Test item_progress() without duration."""
        output = Mock(spec=CLIOutput)
        callback = CLIProgressCallback(output)
        
        callback.item_progress(5, 20, "customers", "RUN")
        output.progress.assert_called_once_with(5, 20, "customers", "RUN", duration=None)
    
    def test_item_progress_with_duration(self):
        """Test item_progress() with duration."""
        output = Mock(spec=CLIOutput)
        callback = CLIProgressCallback(output)
        
        callback.item_progress(5, 20, "customers", "OK", duration=2.5)
        output.progress.assert_called_once_with(5, 20, "customers", "OK", duration=2.5)
    
    def test_blank_line(self):
        """Test blank_line() delegates correctly."""
        output = Mock(spec=CLIOutput)
        callback = CLIProgressCallback(output)
        
        callback.blank_line()
        output.blank_line.assert_called_once()
    
    def test_warning(self):
        """Test warning() delegates to CLIOutput.warning()."""
        output = Mock(spec=CLIOutput)
        callback = CLIProgressCallback(output)
        
        callback.warning("Test warning")
        output.warning.assert_called_once_with("Test warning")
    
    def test_error(self):
        """Test error() delegates to CLIOutput.error()."""
        output = Mock(spec=CLIOutput)
        callback = CLIProgressCallback(output)
        
        callback.error("Test error")
        output.error.assert_called_once_with("Test error")


class TestProgressCallbackIntegration:
    """Test progress callbacks in realistic scenarios."""
    
    def test_service_workflow_simulation(self):
        """Test simulating a service using progress callbacks."""
        output = MagicMock(spec=CLIOutput)
        callback = CLIProgressCallback(output)
        
        # Simulate service workflow
        callback.stage("Starting enrichment")
        callback.info("Found 5 models")
        callback.blank_line()
        
        for i in range(1, 6):
            callback.item_progress(i, 5, f"model_{i}", "RUN")
            # Simulate work
            callback.item_progress(i, 5, f"model_{i}", "OK", duration=1.2)
        
        callback.blank_line()
        callback.info("Enrichment complete")
        
        # Verify all calls were made
        assert output.blank_line.call_count >= 2
        assert output.info.call_count >= 2
        assert output.progress.call_count == 10  # 2 per model (RUN + OK)
    
    def test_callback_with_verbose_mode(self):
        """Test callback respects verbose mode through CLIOutput."""
        output = CLIOutput(verbose=True, quiet=False, use_colors=False)
        callback = CLIProgressCallback(output)
        
        # Detail messages should show in verbose mode
        callback.detail("This is verbose output")
        # No assertion - just ensuring it doesn't crash
    
    def test_callback_with_quiet_mode(self):
        """Test callback respects quiet mode through CLIOutput."""
        output = CLIOutput(verbose=False, quiet=True, use_colors=False)
        callback = CLIProgressCallback(output)
        
        # Most messages should be suppressed
        callback.info("This should be suppressed")
        callback.detail("This too")
        # Errors should still show
        callback.error("This should show")
    
    def test_no_callback_doesnt_break_service(self):
        """Test services work without progress callback."""
        # Simulate service with no callback provided
        callback = NoOpProgressCallback()
        
        # All calls should succeed silently
        callback.stage("Test")
        callback.info("Test")
        callback.detail("Test")
        callback.item_progress(1, 5, "test", "OK")
        callback.blank_line()
        
        # No assertion needed - just verify no exceptions


class TestProgressCallbackProtocol:
    """Test the progress callback protocol compliance."""
    
    def test_noop_callback_matches_protocol(self):
        """Test NoOpProgressCallback implements full protocol."""
        callback = NoOpProgressCallback()
        
        # Check all protocol methods exist
        assert hasattr(callback, 'info')
        assert hasattr(callback, 'stage')
        assert hasattr(callback, 'detail')
        assert hasattr(callback, 'item_progress')
        assert hasattr(callback, 'blank_line')
        assert hasattr(callback, 'warning')
        assert hasattr(callback, 'error')
    
    def test_cli_callback_matches_protocol(self):
        """Test CLIProgressCallback implements full protocol."""
        output = CLIOutput()
        callback = CLIProgressCallback(output)
        
        # Check all protocol methods exist
        assert hasattr(callback, 'info')
        assert hasattr(callback, 'stage')
        assert hasattr(callback, 'detail')
        assert hasattr(callback, 'item_progress')
        assert hasattr(callback, 'blank_line')
        assert hasattr(callback, 'warning')
        assert hasattr(callback, 'error')

