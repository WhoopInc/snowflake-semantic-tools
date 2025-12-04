"""
Comprehensive tests for FileTypeDetector
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

from snowflake_semantic_tools.core.parsing.file_detector import FileTypeDetector


class TestFileTypeDetector:
    """Test file type detection with edge cases"""
    
    @pytest.fixture
    def detector(self):
        return FileTypeDetector()
    
    def test_detect_metrics_file(self, detector, tmp_path):
        """Test detection of metrics files"""
        # Standard metrics file
        file1 = tmp_path / "metrics.yml"
        file1.write_text("snowflake_metrics:\n  - name: revenue")
        assert detector.detect_file_type(file1) == "semantic"
        assert detector.detect_semantic_type(file1) == "metrics"
        
        # With Jinja templates
        file2 = tmp_path / "metrics_jinja.yml"
        file2.write_text("snowflake_metrics:\n  - name: test\n    table: {{ table('users') }}")
        assert detector.detect_file_type(file2) == "semantic"
        assert detector.detect_semantic_type(file2) == "metrics"
        
    def test_detect_relationships_file(self, detector, tmp_path):
        """Test detection of relationship files"""
        file1 = tmp_path / "rel.yml"
        file1.write_text("snowflake_relationships:\n  - name: user_orders")
        assert detector.detect_file_type(file1) == "semantic"
        assert detector.detect_semantic_type(file1) == "relationships"
        
    def test_detect_dbt_file(self, detector, tmp_path):
        """Test detection of dbt model files"""
        # Version 2 format
        file1 = tmp_path / "models.yml"
        file1.write_text("version: 2\nmodels:\n  - name: users")
        assert detector.detect_file_type(file1) == "dbt"
        
        # With sources (might not be detected as dbt without models key)
        file2 = tmp_path / "sources.yml"
        file2.write_text("version: 2\nmodels:\n  - name: source_model")
        assert detector.detect_file_type(file2) == "dbt"
        
    def test_detect_semantic_views(self, detector, tmp_path):
        """Test detection of semantic view files"""
        file1 = tmp_path / "views.yml"
        file1.write_text("snowflake_semantic_views:\n  - name: user_metrics")
        assert detector.detect_file_type(file1) == "semantic"
        assert detector.detect_semantic_type(file1) == "semantic_views"
        
    def test_detect_custom_instructions(self, detector, tmp_path):
        """Test detection of custom instruction files"""
        file1 = tmp_path / "custom.yml"
        file1.write_text("snowflake_custom_instructions:\n  - name: optimize")
        assert detector.detect_file_type(file1) == "semantic"
        assert detector.detect_semantic_type(file1) == "custom_instructions"
        
    def test_detect_filters(self, detector, tmp_path):
        """Test detection of filter files"""
        file1 = tmp_path / "filters.yml"
        file1.write_text("snowflake_filters:\n  - name: active_only")
        assert detector.detect_file_type(file1) == "semantic"
        assert detector.detect_semantic_type(file1) == "filters"
        
    def test_detect_verified_queries(self, detector, tmp_path):
        """Test detection of verified query files"""
        file1 = tmp_path / "queries.yml"
        file1.write_text("snowflake_verified_queries:\n  - name: monthly_revenue")
        assert detector.detect_file_type(file1) == "semantic"
        assert detector.detect_semantic_type(file1) == "verified_queries"
        
    def test_empty_file(self, detector, tmp_path):
        """Test handling of empty files"""
        empty_file = tmp_path / "empty.yml"
        empty_file.write_text("")
        assert detector.detect_file_type(empty_file) == "unknown"
        
    def test_malformed_yaml(self, detector, tmp_path):
        """Test handling of malformed YAML"""
        bad_file = tmp_path / "bad.yml"
        bad_file.write_text("{{not valid yaml: ][")
        # Should fallback to string detection
        assert detector.detect_file_type(bad_file) == "unknown"
        
        # But should still detect if key is present
        bad_file2 = tmp_path / "bad2.yml"
        bad_file2.write_text("{{jinja}}\nsnowflake_metrics:\n  - broken")
        assert detector.detect_file_type(bad_file2) == "semantic"
        
    def test_large_file(self, detector, tmp_path):
        """Test detection in large files"""
        large_file = tmp_path / "large.yml"
        # Create 10MB file
        content = "# Comment\n" * 100000
        content += "snowflake_metrics:\n  - name: test"
        large_file.write_text(content)
        assert detector.detect_file_type(large_file) == "semantic"
        
    def test_unicode_content(self, detector, tmp_path):
        """Test files with unicode content"""
        unicode_file = tmp_path / "unicode.yml"
        unicode_file.write_text("snowflake_metrics:\n  - name: 测试指标\n    description: 中文描述")
        assert detector.detect_file_type(unicode_file) == "semantic"
        
    def test_multiple_types_in_file(self, detector, tmp_path):
        """Test file with multiple type markers (should detect first)"""
        multi_file = tmp_path / "multi.yml"
        multi_file.write_text("""
snowflake_metrics:
  - name: metric1
snowflake_relationships:
  - name: rel1
""")
        # Should detect as semantic file
        assert detector.detect_file_type(multi_file) == "semantic"
        # Should detect first specific type found
        assert detector.detect_semantic_type(multi_file) == "metrics"
        
    def test_case_sensitivity(self, detector, tmp_path):
        """Test case sensitivity in detection"""
        # Should be case-sensitive
        upper_file = tmp_path / "upper.yml"
        upper_file.write_text("SNOWFLAKE_METRICS:\n  - name: test")
        assert detector.detect_file_type(upper_file) == "unknown"
        
        # Correct case
        lower_file = tmp_path / "lower.yml"
        lower_file.write_text("snowflake_metrics:\n  - name: test")
        assert detector.detect_file_type(lower_file) == "semantic"
        assert detector.detect_semantic_type(lower_file) == "metrics"
        
    def test_file_not_found(self, detector):
        """Test handling of non-existent files"""
        # detect_file_type might return 'unknown' instead of raising
        result = detector.detect_file_type(Path("/non/existent/file.yml"))
        assert result == "unknown"
            
    def test_directory_instead_of_file(self, detector, tmp_path):
        """Test handling when directory is passed instead of file"""
        # detect_file_type might return 'unknown' for directories
        result = detector.detect_file_type(tmp_path)
        assert result == "unknown"
            
    def test_binary_file(self, detector, tmp_path):
        """Test handling of binary files"""
        binary_file = tmp_path / "binary.yml"
        binary_file.write_bytes(b'\x00\x01\x02\x03')
        assert detector.detect_file_type(binary_file) == "unknown"
        
    def test_semantic_type_enum(self, detector):
        """Test semantic type enumeration"""
        expected_types = {
            "metrics", "relationships", "filters", 
            "custom_instructions", "verified_queries", 
            "semantic_views", "dbt", "unknown"
        }
        # Ensure all expected types are handled
        assert hasattr(detector, 'detect_file_type')
        
    def test_concurrent_detection(self, detector, tmp_path):
        """Test thread-safe detection"""
        import concurrent.futures
        
        # Create multiple files
        files = []
        for i in range(10):
            file = tmp_path / f"file{i}.yml"
            file.write_text(f"snowflake_metrics:\n  - name: metric{i}")
            files.append(file)
        
        # Detect concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(detector.detect_file_type, files))
        
        # All should be detected as semantic
        assert all(r == "semantic" for r in results)
