"""
Test CLI Utilities

Tests for shared CLI helper functions in interfaces/cli/utils.py
"""

import pytest
import os
from unittest.mock import patch, Mock, MagicMock
from pathlib import Path
import tempfile

from snowflake_semantic_tools.interfaces.cli.utils import (
    load_environment,
    setup_command,
    build_snowflake_config
)
from snowflake_semantic_tools.infrastructure.snowflake import SnowflakeConfig


class TestLoadEnvironment:
    """Test environment variable loading."""
    
    def test_loads_env_from_current_directory(self, tmp_path):
        """Test that .env is loaded from current directory."""
        # Create .env file
        env_file = tmp_path / '.env'
        env_file.write_text('TEST_VAR=test_value\n')
        
        # Change to temp directory
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            
            load_environment()
            
            # Verify variable was loaded
            assert os.getenv('TEST_VAR') == 'test_value'
            
        finally:
            os.chdir(original_cwd)
            # Clean up
            if 'TEST_VAR' in os.environ:
                del os.environ['TEST_VAR']
    
    def test_handles_missing_env_file_gracefully(self, tmp_path):
        """Test that missing .env doesn't cause errors."""
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            
            # Should not raise exception
            load_environment()
            
        finally:
            os.chdir(original_cwd)
    
    def test_verbose_logs_env_file_path(self, tmp_path, capsys):
        """Test that verbose mode logs which .env was loaded."""
        env_file = tmp_path / '.env'
        env_file.write_text('TEST_VAR2=value\n')
        
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            
            load_environment(verbose=True)
            
            captured = capsys.readouterr()
            assert 'Loaded environment from:' in captured.out
            assert str(env_file) in captured.out
            
        finally:
            os.chdir(original_cwd)
            if 'TEST_VAR2' in os.environ:
                del os.environ['TEST_VAR2']


class TestSetupCommand:
    """Test common command setup."""
    
    @patch('snowflake_semantic_tools.interfaces.cli.utils.load_environment')
    @patch('snowflake_semantic_tools.interfaces.cli.utils.setup_events')
    @patch('snowflake_semantic_tools.interfaces.cli.utils.validate_cli_config')
    def test_calls_all_setup_functions(self, mock_validate, mock_events, mock_load_env):
        """Test that setup_command calls all initialization functions."""
        setup_command(verbose=False, quiet=False, validate_config=True)
        
        # Verify all setup functions were called
        mock_load_env.assert_called_once_with(verbose=False)
        mock_events.assert_called_once()
        mock_validate.assert_called_once()
    
    @patch('snowflake_semantic_tools.interfaces.cli.utils.load_environment')
    @patch('snowflake_semantic_tools.interfaces.cli.utils.setup_events')
    @patch('snowflake_semantic_tools.interfaces.cli.utils.validate_cli_config')
    def test_skips_config_validation_when_requested(self, mock_validate, mock_events, mock_load_env):
        """Test that config validation can be skipped."""
        setup_command(verbose=False, validate_config=False)
        
        # Verify validation was NOT called
        mock_validate.assert_not_called()
    
    @patch('snowflake_semantic_tools.interfaces.cli.utils.load_environment')
    @patch('snowflake_semantic_tools.interfaces.cli.utils.setup_events')
    @patch('snowflake_semantic_tools.interfaces.cli.utils.validate_cli_config')
    @patch('logging.getLogger')
    def test_sets_debug_logging_when_verbose(self, mock_get_logger, mock_validate, mock_events, mock_load_env):
        """Test that verbose mode sets DEBUG logging."""
        setup_command(verbose=True, validate_config=False)
        
        mock_logger = mock_get_logger.return_value
        mock_logger.setLevel.assert_called()
        # Check if DEBUG level was set (level 10)
        call_args = mock_logger.setLevel.call_args[0][0]
        import logging
        assert call_args == logging.DEBUG


class TestBuildSnowflakeConfig:
    """Test Snowflake configuration builder."""
    
    @patch.dict(os.environ, {
        'SNOWFLAKE_ACCOUNT': 'test_account',
        'SNOWFLAKE_USER': 'test_user',
        'SNOWFLAKE_ROLE': 'test_role',
        'SNOWFLAKE_WAREHOUSE': 'test_wh'
    })
    @patch('snowflake_semantic_tools.infrastructure.snowflake.SnowflakeConfig.detect_auth_method')
    def test_builds_config_from_env_vars(self, mock_detect_auth):
        """Test building config from environment variables."""
        mock_detect_auth.return_value = (None, None, 'externalbrowser')
        
        config = build_snowflake_config(
            database='TEST_DB',
            schema='TEST_SCHEMA'
        )
        
        assert isinstance(config, SnowflakeConfig)
        assert config.account == 'test_account'
        assert config.user == 'test_user'
        assert config.role == 'test_role'
        assert config.warehouse == 'test_wh'
        assert config.database == 'TEST_DB'
        assert config.schema == 'TEST_SCHEMA'
    
    @patch.dict(os.environ, {})
    @patch('snowflake_semantic_tools.infrastructure.snowflake.SnowflakeConfig.detect_auth_method')
    def test_explicit_params_override_env(self, mock_detect_auth):
        """Test that explicit parameters override environment variables."""
        mock_detect_auth.return_value = (None, None, 'externalbrowser')
        
        with patch('click.prompt', return_value='fallback_value'):
            config = build_snowflake_config(
                account='explicit_account',
                user='explicit_user',
                role='explicit_role',
                warehouse='explicit_wh',
                database='DB',
                schema='SCHEMA'
            )
            
            assert config.account == 'explicit_account'
            assert config.user == 'explicit_user'
            assert config.role == 'explicit_role'
            assert config.warehouse == 'explicit_wh'
    
    @patch.dict(os.environ, {'SNOWFLAKE_USERNAME': 'username_var'}, clear=False)
    @patch('snowflake_semantic_tools.infrastructure.snowflake.SnowflakeConfig.detect_auth_method')
    def test_supports_both_user_and_username_env_vars(self, mock_detect_auth):
        """Test support for both SNOWFLAKE_USER and SNOWFLAKE_USERNAME."""
        mock_detect_auth.return_value = (None, None, 'externalbrowser')
        
        # Clear SNOWFLAKE_USER to test SNOWFLAKE_USERNAME fallback
        with patch.dict(os.environ, {'SNOWFLAKE_USER': ''}, clear=False):
            with patch('click.prompt', return_value='prompt_value'):
                config = build_snowflake_config(
                    account='test',
                    database='DB',
                    schema='SCHEMA'
                )
                
                # Should use SNOWFLAKE_USERNAME when SNOWFLAKE_USER not set
                assert config.user == 'username_var'
    
    @patch('snowflake_semantic_tools.infrastructure.snowflake.SnowflakeConfig.detect_auth_method')
    def test_calls_detect_auth_method(self, mock_detect_auth):
        """Test that auth detection is called."""
        mock_detect_auth.return_value = ('password123', None, None)
        
        with patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'acc', 'SNOWFLAKE_USER': 'user'}):
            with patch('click.prompt', return_value='value'):
                config = build_snowflake_config(database='DB', schema='SCHEMA')
                
                mock_detect_auth.assert_called_once()
                assert config.password == 'password123'


class TestIntegration:
    """Integration tests for CLI utils."""
    
    def test_setup_and_snowflake_config_work_together(self):
        """Test that setup_command and build_snowflake_config work together."""
        with patch('snowflake_semantic_tools.interfaces.cli.utils.load_environment'):
            with patch('snowflake_semantic_tools.interfaces.cli.utils.setup_events'):
                with patch('snowflake_semantic_tools.interfaces.cli.utils.validate_cli_config'):
                    with patch('snowflake_semantic_tools.infrastructure.snowflake.SnowflakeConfig.detect_auth_method') as mock_auth:
                        with patch.dict(os.environ, {
                            'SNOWFLAKE_ACCOUNT': 'test',
                            'SNOWFLAKE_USER': 'user',
                            'SNOWFLAKE_ROLE': 'role',
                            'SNOWFLAKE_WAREHOUSE': 'wh'
                        }):
                            mock_auth.return_value = (None, None, 'externalbrowser')
                            
                            # Typical command pattern
                            setup_command(verbose=True)
                            config = build_snowflake_config(database='DB', schema='SCHEMA')
                            
                            assert config is not None
                            assert isinstance(config, SnowflakeConfig)

