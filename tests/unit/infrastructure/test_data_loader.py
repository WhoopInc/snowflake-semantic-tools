"""
Unit tests for data_loader error handling.

Tests the error handling improvements:
- Issue #30: Permission errors are formatted with helpful context
"""

import pytest
from snowflake_semantic_tools.infrastructure.snowflake.data_loader import format_permission_error


class TestFormatPermissionError:
    """Test permission error formatting (Issue #30)."""
    
    def test_permission_error_not_authorized(self):
        """Test formatting of 'not authorized' error."""
        error = Exception("Object 'ANALYTICS' does not exist or not authorized")
        result = format_permission_error(error, "ANALYTICS", "PUBLIC", "SYSADMIN")
        
        assert "Cannot access database 'ANALYTICS'" in result
        assert "Current role: SYSADMIN" in result
        assert "GRANT USAGE ON DATABASE ANALYTICS TO ROLE SYSADMIN" in result
        assert "Solutions:" in result
    
    def test_permission_error_insufficient_privileges(self):
        """Test formatting of 'insufficient privileges' error."""
        error = Exception("Insufficient privileges to operate on table")
        result = format_permission_error(error, "PROD_DB", "SCHEMA1", "ANALYST_ROLE")
        
        assert "Cannot access database 'PROD_DB'" in result
        assert "Current role: ANALYST_ROLE" in result
        assert "GRANT USAGE ON DATABASE PROD_DB TO ROLE ANALYST_ROLE" in result
    
    def test_permission_error_access_denied(self):
        """Test formatting of 'access denied' error."""
        error = Exception("Access denied to database")
        result = format_permission_error(error, "SECURE_DB", "PRIVATE", None)
        
        assert "Cannot access database 'SECURE_DB'" in result
        assert "Current role: Unknown" in result
        assert "YOUR_ROLE" in result  # Placeholder when role is None
    
    def test_non_permission_error_unchanged(self):
        """Test that non-permission errors are returned unchanged."""
        error = Exception("Connection timeout")
        result = format_permission_error(error, "DB", "SCHEMA", "ROLE")
        
        # Should just return the original error string
        assert result == "Connection timeout"
    
    def test_syntax_error_unchanged(self):
        """Test that syntax errors are not treated as permission errors."""
        error = Exception("SQL compilation error: invalid identifier")
        result = format_permission_error(error, "DB", "SCHEMA", "ROLE")
        
        assert result == "SQL compilation error: invalid identifier"
    
    def test_permission_error_includes_schema(self):
        """Test that schema is included in remediation steps."""
        error = Exception("not authorized")
        result = format_permission_error(error, "MY_DB", "MY_SCHEMA", "MY_ROLE")
        
        assert "MY_DB.MY_SCHEMA" in result
        assert "GRANT USAGE ON SCHEMA MY_DB.MY_SCHEMA TO ROLE MY_ROLE" in result
    
    def test_original_error_preserved(self):
        """Test that original error message is included."""
        error = Exception("Object 'TEST' does not exist or not authorized for operation")
        result = format_permission_error(error, "DB", "SCHEMA", "ROLE")
        
        assert "Original error:" in result
        assert "does not exist or not authorized" in result

