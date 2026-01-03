"""
Test logging utilities.

Tests the simplified logging system.
"""

import logging

import pytest

from snowflake_semantic_tools.shared.utils.logger import get_logger


class TestLogger:
    """Test essential logging functionality."""

    def test_get_logger_basic(self):
        """Test basic logger creation."""
        logger = get_logger("test_module")

        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"

    def test_get_logger_with_package_name(self):
        """Test logger creation with package name."""
        logger = get_logger("snowflake_semantic_tools.core.parsing")

        # Should extract just the module name
        assert logger.name == "parsing"

    def test_get_logger_consistency(self):
        """Test that same name returns same logger."""
        logger1 = get_logger("same_module")
        logger2 = get_logger("same_module")

        assert logger1 is logger2

    def test_logger_functionality(self):
        """Test that logger actually works."""
        logger = get_logger("test_functional")

        # Should not raise exceptions
        logger.info("Test info message")
        logger.warning("Test warning message")
        logger.error("Test error message")

        # Basic functionality test passed if no exceptions
        assert True

    def test_logger_different_names(self):
        """Test that different names create different loggers."""
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")

        assert logger1 is not logger2
        assert logger1.name != logger2.name
