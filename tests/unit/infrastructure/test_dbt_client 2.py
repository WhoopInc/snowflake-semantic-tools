"""
Test DbtClient

Tests for the dbt CLI abstraction layer, including auto-detection
and command execution.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import subprocess

from snowflake_semantic_tools.infrastructure.dbt import (
    DbtClient,
    DbtResult,
    DbtNotFoundError,
    DbtCompileError
)
from snowflake_semantic_tools.infrastructure.dbt.client import DbtType


class TestDbtTypeDetection:
    """Test dbt type detection logic."""
    
    def test_detects_pip_cloud_cli(self):
        """Test auto-detection of pip-installed Cloud CLI."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(
                stdout="dbt Cloud CLI - 0.40.7 (3aa8c1ef 2025-10-06)",
                stderr='',
                returncode=0
            )
            
            client = DbtClient()
            
            assert client.dbt_type == DbtType.CLOUD_CLI
            assert "dbt Cloud CLI" in client.version
    
    def test_detects_dbt_core(self):
        """Test auto-detection of dbt Core."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(
                stdout="Core:\n  - installed: 1.7.15",
                stderr='',
                returncode=0
            )
            
            client = DbtClient()
            
            assert client.dbt_type == DbtType.CORE
            assert "Core:" in client.version
    
    def test_defaults_to_core_on_unrecognized_output(self):
        """Test fallback to Core when version output is unrecognized."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(
                stdout="some unknown dbt version 3.0",
                stderr='',
                returncode=0
            )
            
            client = DbtClient()
            
            # Should default to Core (safest)
            assert client.dbt_type == DbtType.CORE
    
    def test_handles_dbt_not_found(self):
        """Test graceful handling when dbt command not found."""
        with patch('subprocess.run', side_effect=FileNotFoundError):
            client = DbtClient()
            
            assert client.dbt_type == DbtType.UNKNOWN
            assert client.version is None
    
    def test_handles_version_timeout(self):
        """Test graceful handling when dbt --version times out."""
        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired('dbt', 5)):
            client = DbtClient()
            
            # Defaults to Core (safest fallback)
            assert client.dbt_type == DbtType.CORE
            assert client.version is None


class TestDbtCompileCommand:
    """Test dbt compile command execution."""
    
    def test_compile_with_target_for_core(self):
        """Test that --target is used for dbt Core."""
        with patch('subprocess.run') as mock_run:
            # Setup: Detect as Core
            mock_run.return_value = Mock(
                stdout="Core:\n  - dbt-core: 1.7.0",
                stderr='',
                returncode=0
            )
            client = DbtClient()
            
            # Execute: Compile with target
            mock_run.return_value = Mock(stdout='', stderr='', returncode=0)
            result = client.compile(target='prod')
            
            # Verify: --target was added
            call_args = mock_run.call_args_list[-1]
            cmd = call_args[0][0]
            assert 'dbt' in cmd
            assert 'compile' in cmd
            assert '--target' in cmd
            assert 'prod' in cmd
    
    def test_compile_without_target_for_cloud_cli(self):
        """Test that --target is NOT used for dbt Cloud CLI."""
        with patch('subprocess.run') as mock_run:
            # Setup: Auto-detect as Cloud CLI
            mock_run.return_value = Mock(
                stdout="dbt Cloud CLI - 0.40.7",
                stderr='',
                returncode=0
            )
            client = DbtClient()
            
            # Execute: Compile with target
            mock_run.return_value = Mock(stdout='', stderr='', returncode=0)
            result = client.compile(target='prod')
            
            # Verify: --target was NOT added
            call_args = mock_run.call_args_list[-1]
            cmd = call_args[0][0]
            assert 'dbt' in cmd
            assert 'compile' in cmd
            assert '--target' not in cmd
    
    def test_compile_defaults_to_core_uses_target(self):
        """Test that default (Core) uses --target flag."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout="Core:", stderr='', returncode=0)
            client = DbtClient()  # Defaults to Core
            
            # Execute: Compile with target
            mock_run.return_value = Mock(stdout='', stderr='', returncode=0)
            result = client.compile(target='prod')
            
            # Verify: --target WAS added (Core uses it)
            call_args = mock_run.call_args_list[-1]
            cmd = call_args[0][0]
            assert 'dbt' in cmd
            assert 'compile' in cmd
            assert '--target' in cmd
            assert 'prod' in cmd
    
    def test_compile_with_select(self):
        """Test compile with --select flag."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout="Core:", stderr='', returncode=0)
            client = DbtClient()
            
            mock_run.return_value = Mock(stdout='', stderr='', returncode=0)
            result = client.compile(select='tag:semantic')
            
            call_args = mock_run.call_args_list[-1]
            cmd = call_args[0][0]
            assert '--select' in cmd
            assert 'tag:semantic' in cmd
    
    def test_compile_with_exclude(self):
        """Test compile with --exclude flag."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout="Core:", stderr='', returncode=0)
            client = DbtClient()
            
            mock_run.return_value = Mock(stdout='', stderr='', returncode=0)
            result = client.compile(exclude='tag:deprecated')
            
            call_args = mock_run.call_args_list[-1]
            cmd = call_args[0][0]
            assert '--exclude' in cmd
            assert 'tag:deprecated' in cmd
    
    def test_compile_success_returns_result(self):
        """Test that successful compile returns DbtResult."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout="Core:", stderr='', returncode=0)
            client = DbtClient()
            
            mock_run.return_value = Mock(
                stdout='Compiled 10 models',
                stderr='',
                returncode=0
            )
            result = client.compile()
            
            assert isinstance(result, DbtResult)
            assert result.success is True
            assert result.returncode == 0
            assert 'Compiled 10 models' in result.stdout
    
    def test_compile_failure_returns_result_with_error(self):
        """Test that failed compile returns DbtResult with error."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout="Core:", stderr='', returncode=0)
            client = DbtClient()
            
            mock_run.return_value = Mock(
                stdout='',
                stderr='Compilation error: model not found',
                returncode=1
            )
            result = client.compile()
            
            assert isinstance(result, DbtResult)
            assert result.success is False
            assert result.returncode == 1
            assert 'Compilation error' in result.stderr
    
    def test_compile_raises_on_dbt_not_found(self):
        """Test that FileNotFoundError raises DbtNotFoundError."""
        with patch('subprocess.run') as mock_run:
            # Version check succeeds
            mock_run.return_value = Mock(stdout="Core:", stderr='', returncode=0)
            client = DbtClient()
            
            # Compile fails with FileNotFoundError
            mock_run.side_effect = FileNotFoundError()
            
            with pytest.raises(DbtNotFoundError):
                client.compile()


class TestDbtResult:
    """Test DbtResult dataclass."""
    
    def test_result_has_all_fields(self):
        """Test that DbtResult contains expected fields."""
        result = DbtResult(
            success=True,
            command='dbt compile',
            stdout='output',
            stderr='',
            returncode=0,
            dbt_type=DbtType.CORE
        )
        
        assert result.success is True
        assert result.command == 'dbt compile'
        assert result.stdout == 'output'
        assert result.stderr == ''
        assert result.returncode == 0
        assert result.dbt_type == DbtType.CORE
    
    def test_output_property_combines_stdout_stderr(self):
        """Test that output property combines stdout and stderr."""
        result = DbtResult(
            success=False,
            command='dbt compile',
            stdout='some output',
            stderr='some error',
            returncode=1,
            dbt_type=DbtType.CORE
        )
        
        output = result.output
        assert 'some output' in output
        assert 'some error' in output


class TestDbtClientHelpers:
    """Test helper methods."""
    
    def test_get_manifest_path_returns_expected_path(self):
        """Test that get_manifest_path returns target/manifest.json."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout="Core:", stderr='', returncode=0)
            
            client = DbtClient(project_dir=Path('/test/project'))
            manifest_path = client.get_manifest_path()
            
            assert manifest_path == Path('/test/project/target/manifest.json')
    
    def test_project_dir_defaults_to_cwd(self):
        """Test that project_dir defaults to current directory."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout="Core:", stderr='', returncode=0)
            
            client = DbtClient()
            
            assert client.project_dir == Path.cwd()


class TestDbtClientVerboseMode:
    """Test verbose logging."""
    
    def test_verbose_mode_logs_detection(self):
        """Test that verbose mode logs detection results."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(
                stdout="dbt Cloud CLI - 0.40.7",
                stderr='',
                returncode=0
            )
            
            # Should log detection when verbose=True
            client = DbtClient(verbose=True)
            
            assert client.dbt_type == DbtType.CLOUD_CLI
    
    def test_verbose_mode_logs_command_execution(self):
        """Test that verbose mode logs command details."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout="Core:", stderr='', returncode=0)
            client = DbtClient(verbose=True)
            
            mock_run.return_value = Mock(stdout='output', stderr='', returncode=0)
            result = client.compile()
            
            # Verbose mode should log (tested by not raising exceptions)
            assert result.success is True


class TestDbtExceptions:
    """Test custom exceptions."""
    
    def test_dbt_not_found_error_has_helpful_message(self):
        """Test that DbtNotFoundError includes install instructions."""
        error = DbtNotFoundError()
        message = str(error)
        
        assert 'dbt command not found' in message
        assert 'pip install dbt-snowflake' in message
        assert 'pip install dbt' in message
        assert 'docs.getdbt.com' in message
    
    def test_dbt_compile_error_includes_stderr(self):
        """Test that DbtCompileError includes dbt's error output."""
        error = DbtCompileError(stderr="Model not found: customers", target="prod")
        message = str(error)
        
        assert 'Model not found' in message
        assert 'target: prod' in message
        assert 'Common causes' in message


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_handles_empty_version_output(self):
        """Test handling of empty dbt --version output."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout='', stderr='', returncode=0)
            
            client = DbtClient()
            
            # Defaults to Core (can't determine type, but Core is safest default)
            assert client.dbt_type == DbtType.CORE
    
    def test_handles_version_with_only_stderr(self):
        """Test handling when version output is in stderr."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(
                stdout='',
                stderr='Core:\n  - dbt-core: 1.7.0',
                returncode=0
            )
            
            client = DbtClient()
            
            assert client.dbt_type == DbtType.CORE
    
    def test_compile_with_no_flags(self):
        """Test basic compile with no optional flags."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout="Core:", stderr='', returncode=0)
            client = DbtClient()
            
            mock_run.return_value = Mock(stdout='', stderr='', returncode=0)
            result = client.compile()
            
            call_args = mock_run.call_args_list[-1]
            cmd = call_args[0][0]
            
            assert cmd == ['dbt', 'compile']
    
    def test_subprocess_uses_project_dir(self):
        """Test that subprocess runs in specified project directory."""
        test_dir = Path('/test/project')
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(stdout="Core:", stderr='', returncode=0)
            client = DbtClient(project_dir=test_dir)
            
            mock_run.return_value = Mock(stdout='', stderr='', returncode=0)
            client.compile()
            
            call_args = mock_run.call_args_list[-1]
            assert call_args[1]['cwd'] == test_dir

