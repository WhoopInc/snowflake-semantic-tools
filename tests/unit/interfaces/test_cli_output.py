"""
Tests for CLI Output Formatter

Tests the CLIOutput class and ProgressBatcher helper.
"""

import pytest
import time
from io import StringIO
from unittest.mock import patch
from datetime import datetime

from snowflake_semantic_tools.interfaces.cli.output import CLIOutput, ProgressBatcher


class TestCLIOutput:
    """Test suite for CLIOutput class."""
    
    def test_init_defaults(self):
        """Test CLIOutput initializes with correct defaults."""
        output = CLIOutput()
        assert output.verbose is False
        assert output.quiet is False
        assert isinstance(output.start_time, float)
    
    def test_init_with_options(self):
        """Test CLIOutput initialization with custom options."""
        output = CLIOutput(verbose=True, quiet=False, use_colors=False)
        assert output.verbose is True
        assert output.quiet is False
        assert output.use_colors is False
    
    def test_timestamp_format(self):
        """Test timestamp returns HH:MM:SS format."""
        output = CLIOutput()
        ts = output.timestamp()
        
        # Should match HH:MM:SS format
        assert len(ts) == 8
        assert ts[2] == ':'
        assert ts[5] == ':'
        
        # Should be valid time
        datetime.strptime(ts, "%H:%M:%S")
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_info_basic(self, mock_stdout):
        """Test info() prints timestamped message."""
        output = CLIOutput()
        output.info("Test message")
        
        result = mock_stdout.getvalue()
        assert "Test message" in result
        assert "  " in result  # Timestamp separator
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_info_with_indent(self, mock_stdout):
        """Test info() respects indent levels."""
        output = CLIOutput()
        output.info("Level 0", indent=0)
        output.info("Level 1", indent=1)
        output.info("Level 2", indent=2)
        
        result = mock_stdout.getvalue()
        lines = result.strip().split('\n')
        
        # Check indentation (each indent = 2 spaces after timestamp)
        assert "Level 0" in lines[0]
        assert "  Level 1" in lines[1]  # 2 spaces
        assert "    Level 2" in lines[2]  # 4 spaces
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_info_quiet_mode(self, mock_stdout):
        """Test info() suppresses output in quiet mode."""
        output = CLIOutput(quiet=True)
        output.info("Should not appear")
        
        result = mock_stdout.getvalue()
        assert result == ""
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_debug_verbose_mode(self, mock_stdout):
        """Test debug() only shows in verbose mode."""
        output_normal = CLIOutput(verbose=False)
        output_normal.debug("Debug message")
        
        result_normal = mock_stdout.getvalue()
        assert result_normal == ""
        
        # Reset mock
        mock_stdout.truncate(0)
        mock_stdout.seek(0)
        
        output_verbose = CLIOutput(verbose=True, use_colors=False)
        output_verbose.debug("Debug message")
        
        result_verbose = mock_stdout.getvalue()
        assert "Debug message" in result_verbose
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_success_without_duration(self, mock_stdout):
        """Test success() message without duration."""
        output = CLIOutput(use_colors=False)
        output.success("Operation completed")
        
        result = mock_stdout.getvalue()
        assert "Operation completed" in result
        assert "[OK]" in result
        assert "in" not in result  # No duration
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_success_with_duration(self, mock_stdout):
        """Test success() message with duration."""
        output = CLIOutput(use_colors=False)
        output.success("Operation completed", duration=15.234)
        
        result = mock_stdout.getvalue()
        assert "Operation completed" in result
        assert "[OK in 15.2s]" in result
    
    @patch('sys.stderr', new_callable=StringIO)
    def test_error_without_duration(self, mock_stderr):
        """Test error() message goes to stderr."""
        output = CLIOutput(use_colors=False)
        output.error("Operation failed")
        
        result = mock_stderr.getvalue()
        assert "Operation failed" in result
        assert "[ERROR]" in result
    
    @patch('sys.stderr', new_callable=StringIO)
    def test_error_with_duration(self, mock_stderr):
        """Test error() message with duration."""
        output = CLIOutput(use_colors=False)
        output.error("Operation failed", duration=2.5)
        
        result = mock_stderr.getvalue()
        assert "Operation failed" in result
        assert "[ERROR in 2.5s]" in result
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_warning(self, mock_stdout):
        """Test warning() message."""
        output = CLIOutput(use_colors=False)
        output.warning("Something suspicious")
        
        result = mock_stdout.getvalue()
        assert "Something suspicious" in result
        assert "[WARN]" in result
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_progress_basic(self, mock_stdout):
        """Test progress() basic formatting."""
        output = CLIOutput(use_colors=False)
        output.progress(1, 10, "customers", "RUN")
        
        result = mock_stdout.getvalue()
        assert " 1 of 10" in result
        assert "customers" in result
        assert "." in result  # Dots for alignment
        assert "[RUN]" in result
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_progress_with_duration(self, mock_stdout):
        """Test progress() with duration (for OK/ERROR status)."""
        output = CLIOutput(use_colors=False)
        output.progress(5, 100, "orders", "OK", duration=2.3)
        
        result = mock_stdout.getvalue()
        assert "  5 of 100" in result
        assert "orders" in result
        assert "[OK in 2.3s]" in result
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_progress_number_alignment(self, mock_stdout):
        """Test progress() aligns numbers correctly."""
        output = CLIOutput(use_colors=False)
        
        # Single digit of triple digit
        output.progress(1, 999, "test", "RUN")
        result1 = mock_stdout.getvalue()
        assert "  1 of 999" in result1
        
        # Reset mock
        mock_stdout.truncate(0)
        mock_stdout.seek(0)
        
        # Triple digit
        output.progress(100, 999, "test", "RUN")
        result2 = mock_stdout.getvalue()
        assert "100 of 999" in result2
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_progress_long_name_truncation(self, mock_stdout):
        """Test progress() truncates very long names."""
        output = CLIOutput(use_colors=False)
        long_name = "x" * 50  # Longer than default max_name_len (40)
        
        output.progress(1, 10, long_name, "RUN", max_name_len=40)
        
        result = mock_stdout.getvalue()
        assert "xxx..." in result  # Truncated with ellipsis
        assert len(long_name) > 40  # Verify test setup
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_progress_quiet_mode(self, mock_stdout):
        """Test progress() suppresses in quiet mode."""
        output = CLIOutput(quiet=True)
        output.progress(1, 10, "test", "RUN")
        
        result = mock_stdout.getvalue()
        assert result == ""
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_header(self, mock_stdout):
        """Test header() prints title with separator."""
        output = CLIOutput()
        output.header("TEST HEADER")
        
        result = mock_stdout.getvalue()
        lines = [l for l in result.split('\n') if l.strip()]
        
        assert len(lines) >= 2
        assert "TEST HEADER" in lines[0]
        assert "=" * len("TEST HEADER") in lines[1]
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_header_custom_separator(self, mock_stdout):
        """Test header() with custom separator character."""
        output = CLIOutput()
        output.header("TEST", separator="-")
        
        result = mock_stdout.getvalue()
        assert "----" in result  # Dashes instead of equals
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_summary(self, mock_stdout):
        """Test summary() prints stats dictionary."""
        output = CLIOutput()
        output.summary({
            'Status': 'PASSED',
            'Models': 628,
            'Errors': 0
        }, title="TEST SUMMARY")
        
        result = mock_stdout.getvalue()
        assert "TEST SUMMARY" in result
        assert "Status: PASSED" in result
        assert "Models: 628" in result
        assert "Errors: 0" in result
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_done_line_basic(self, mock_stdout):
        """Test done_line() prints dbt-style summary."""
        output = CLIOutput(use_colors=False)
        output.done_line(passed=616, warned=12, errored=0, skipped=0, total=628)
        
        result = mock_stdout.getvalue()
        assert "Done." in result
        assert "PASS=616" in result
        assert "WARN=12" in result
        assert "ERROR=0" in result
        assert "SKIP=0" in result
        assert "TOTAL=628" in result
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_done_line_auto_total(self, mock_stdout):
        """Test done_line() auto-calculates total if not provided."""
        output = CLIOutput(use_colors=False)
        output.done_line(passed=10, warned=2, errored=1, skipped=1)
        
        result = mock_stdout.getvalue()
        assert "TOTAL=14" in result  # 10 + 2 + 1 + 1
    
    def test_elapsed_time(self):
        """Test elapsed_time() returns positive duration."""
        output = CLIOutput()
        time.sleep(0.01)  # Small sleep to ensure time passes
        elapsed = output.elapsed_time()
        
        assert elapsed > 0
        assert elapsed < 1.0  # Should be very small
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_blank_line(self, mock_stdout):
        """Test blank_line() prints newline."""
        output = CLIOutput()
        output.blank_line()
        
        result = mock_stdout.getvalue()
        assert result == "\n"
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_blank_line_quiet_mode(self, mock_stdout):
        """Test blank_line() respects quiet mode."""
        output = CLIOutput(quiet=True)
        output.blank_line()
        
        result = mock_stdout.getvalue()
        assert result == ""
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_rule(self, mock_stdout):
        """Test rule() prints horizontal line."""
        output = CLIOutput()
        output.rule(char="-", width=20)
        
        result = mock_stdout.getvalue()
        assert "-" * 20 in result
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_section(self, mock_stdout):
        """Test section() prints section title."""
        output = CLIOutput()
        output.section("Processing models...")
        
        result = mock_stdout.getvalue()
        assert "Processing models..." in result
        assert result.count('\n') >= 2  # Blank line + section
    
    def test_format_status_without_duration(self):
        """Test _format_status() without duration."""
        output = CLIOutput()
        result = output._format_status("OK")
        assert result == "[OK]"
    
    def test_format_status_with_duration(self):
        """Test _format_status() with duration."""
        output = CLIOutput()
        result = output._format_status("OK", duration=2.345)
        assert result == "[OK in 2.3s]"
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_colors_disabled_when_not_tty(self, mock_stdout):
        """Test colors are disabled when stdout is not a TTY."""
        with patch('sys.stdout.isatty', return_value=False):
            output = CLIOutput(use_colors=True)
            assert output.use_colors is False
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_multiple_status_types(self, mock_stdout):
        """Test all status types format correctly."""
        output = CLIOutput(use_colors=False)
        
        statuses = ["RUN", "OK", "ERROR", "WARN", "SKIP"]
        for i, status in enumerate(statuses, 1):
            output.progress(i, 5, f"item_{status.lower()}", status)
        
        result = mock_stdout.getvalue()
        for status in statuses:
            assert f"[{status}]" in result


class TestProgressBatcher:
    """Test suite for ProgressBatcher helper."""
    
    def test_init(self):
        """Test ProgressBatcher initialization."""
        batcher = ProgressBatcher(total=100, batch_size=10)
        assert batcher.total == 100
        assert batcher.batch_size == 10
        assert batcher.last_shown == 0
    
    def test_should_show_first_item(self):
        """Test first item is always shown."""
        batcher = ProgressBatcher(total=100, batch_size=10)
        assert batcher.should_show(1) is True
    
    def test_should_show_last_item(self):
        """Test last item is always shown."""
        batcher = ProgressBatcher(total=100, batch_size=10)
        assert batcher.should_show(100) is True
    
    def test_should_show_batch_intervals(self):
        """Test items are shown at batch intervals."""
        batcher = ProgressBatcher(total=100, batch_size=10)
        
        # First item
        assert batcher.should_show(1) is True
        
        # Should show every 10th item
        assert batcher.should_show(2) is False
        assert batcher.should_show(9) is False
        assert batcher.should_show(11) is True  # 11 - 1 >= 10
        assert batcher.should_show(12) is False
        assert batcher.should_show(21) is True  # 21 - 11 >= 10
    
    def test_should_batch_decision(self):
        """Test should_batch() returns correct decision."""
        batcher_small = ProgressBatcher(total=50, batch_size=10)
        assert batcher_small.should_batch() is False
        
        batcher_large = ProgressBatcher(total=200, batch_size=10)
        assert batcher_large.should_batch() is True
    
    def test_custom_batch_size(self):
        """Test custom batch_size works correctly."""
        batcher = ProgressBatcher(total=100, batch_size=25)
        
        assert batcher.should_show(1) is True
        assert batcher.should_show(25) is False  # 25 - 1 = 24 < 25
        assert batcher.should_show(26) is True  # 26 - 1 = 25 >= 25
        assert batcher.should_show(50) is False  # 50 - 26 = 24 < 25
        assert batcher.should_show(51) is True  # 51 - 26 = 25 >= 25
    
    def test_realistic_workflow(self):
        """Test realistic workflow with progress tracking."""
        total = 628
        batcher = ProgressBatcher(total=total, batch_size=50)
        
        shown_items = []
        for i in range(1, total + 1):
            if batcher.should_show(i):
                shown_items.append(i)
        
        # Verify first and last are shown
        assert 1 in shown_items
        assert total in shown_items
        
        # Verify reasonable batching (not showing all 628)
        assert len(shown_items) < 20  # Should show ~13 items (628/50 + 2)
        assert len(shown_items) > 5

